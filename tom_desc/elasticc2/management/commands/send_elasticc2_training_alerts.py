import sys
import io
import socket
import math
import pathlib
import logging
import json
import time
import datetime
import tarfile
import confluent_kafka
import fastavro

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from elasticc2.models import TrainingAlert, TrainingDiaObject, TrainingDiaSource, TrainingDiaForcedSource
from elasticc2.models import ClassIdOfGentype, TrainingDiaObjectTruth

_rundir = pathlib.Path(__file__).parent

class Command(BaseCommand):
    help = 'Send ELAsTiCC2 training set alerts'

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )
        self.logger = logging.getLogger( "send_elasticc_alerts" )
        self.logger.propagate = False
        if not self.logger.hasHandlers():
            logout = logging.StreamHandler( sys.stderr )
            self.logger.addHandler( logout )
            formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                           datefmt='%Y-%m-%d %H:%M:%S' )
            logout.setFormatter( formatter )
        self.logger.setLevel( logging.INFO )

    def add_arguments( self, parser ):
        parser.add_argument( '-k', '--kafka-server', default='brahms.lbl.gov:9092', help="Kafka server to stream to" )
        parser.add_argument( '-t', '--kafka-topic-base', default='elasticc2-training-1-',
                             help="Kafka topic base; -<classid> will be appended" )
        parser.add_argument( '-s', '--alert-schema', default=f'{_rundir}/elasticc.v0_9_1.alert.avsc',
                             help='File with AVRO schema' )
        parser.add_argument( '-f', '--flush-every', default=1000, type=int,
                             help="Flush the kafka producer every this man alerts" )
        parser.add_argument( '-l', '--log-every', default=2000, type=int,
                             help="Log alerts sent at this interval; 0=don't log" )
        parser.add_argument( '-d', '--tardir', default=None,
                             help="Directory to write tar files; if not given, not tar files are written" )
        parser.add_argument( '-x', '--delete-tar', default=False, action='store_true',
                             help="Delete tar files before starting; by default, appends to them." )
        parser.add_argument( '-u', '--unsent-only', default=False, action='store_true',
                             help="Only send alerts with a null AlertSentTimestamp (default: send all)" )
        parser.add_argument( '--do', action='store_true', default=False,
                             help="Actually do it (otherwise, it's a dry run)" )
        parser.add_argument( '--stop-after', default=None, type=int,
                             help="Stop after this many alerts (for testing purposes)" )

    @transaction.atomic
    def update_alertsent( self, ids ):
        sentalerts = TrainingAlert.objects.filter( pk__in=ids )
        for sa in sentalerts:
            sa.alertsenttimestamp = datetime.datetime.now( datetime.timezone.utc )
            sa.save()
        
    def handle( self, *args, **options ):

        getclass_time = 0
        getsources_time = 0
        getforced_time = 0
        getalert_time = 0
        reconstruct_time = 0
        avrowrite_time = 0
        produce_time = 0
        taradd_time = 0
        flush_time = 0
        update_alertsent_time = 0

        t0 = time.perf_counter()
        
        # Get classid ← gentype match
        matches = ClassIdOfGentype.objects.filter( exactmatch=True ).all()
        gentypes = {}
        classids = set()
        for row in matches:
            if row.gentype in gentypes:
                raise RuntimeError( "Found gentype {row.gentype} exact match more than once in mapping table!" )
            gentypes[row.gentype] = row.classid
            classids.add( row.classid )

        self.logger.info( "**** streaming starting ****" )
        self.logger.info( f"Streaming training alerts to {options['kafka_server']} "
                          f"with topic base {options['kafka_topic_base']}" )

        diaobjects = TrainingDiaObject.objects.order_by("diaobject_id")
        self.logger.info( f"{diaobjects.count()} objects to stream" )

        # import pdb; pdb.set_trace()
        schema = fastavro.schema.load_schema( options['alert_schema'] )

        if options['do']:
            producer = confluent_kafka.Producer( { 'bootstrap.servers': options[ 'kafka_server' ],
                                                   'batch.size': 131072,
                                                   'linger.ms': 50 } )
            # Open tarfiles if necessary
            if options['tardir'] is not None:
                tardir = pathlib.Path( options['tardir'] )
                if not tardir.is_dir():
                    raise RuntimeError( f"{tardir} isn't an existing directory" )

                tarfiles = {}
                for classid in classids:
                    path = tardir / f'{classid}.tar'
                    if path.exists():
                        if not path.is_file():
                            raise RuntimeError( f"File {path} exists but is not a file!" )
                        if options['delete_tar']:
                            self.logger.warning( f"Deleting existing tar file {path}" )
                            path.unlink()
                        elif not tarfile.is_tarfile( path ):
                            raise RuntimeError( f"{path} is not a tarfile!" )
                    tarfiles[classid] = tarfile.open( path, 'a' )

        self.logger.info( f"Startup time: {time.perf_counter()-t0}" )

        ids_produced = []
        totflushed = 0
        nextlog = 0
        tot = 0
        i = 0
        # I don't use enumerate here because I think it was subverting the
        #   lazy loading of the django diaobjects iterator.  Dunno.
        #   Django is a mystery.
        for diaobject in diaobjects:
            if ( options['stop_after'] is not None ) and ( tot >= options['stop_after'] ):
                break

            if ( options['log_every'] > 0 ) and ( i >= nextlog ):
                self.logger.info( f"Have processed {i} of {diaobjects.count()} objects; "
                                  f"{tot} alerts, {totflushed} flushed." )
                self.logger.info( f"Timings:\n"
                                  f"          getclass_time = {getclass_time:9.2f}\n"
                                  f"        getsources_time = {getsources_time:9.2f}\n"
                                  f"         getforced_time = {getforced_time:9.2f}\n"
                                  f"          getalert_time = {getalert_time:9.2f}\n"
                                  f"       reconstruct_time = {reconstruct_time:9.2f}\n"
                                  f"                             source = {TrainingAlert._sourcetime:9.2f}\n"
                                  f"                             object = {TrainingAlert._objecttime:9.2f}\n"
                                  f"                               ovrh = {TrainingAlert._objectoverheadtime:9.2f}\n"
                                  f"                             prvsrc = {TrainingAlert._prvsourcetime:9.2f}\n"
                                  f"                             prvfrc = {TrainingAlert._prvforcedsourcetime:9.2f}\n"
                                  f"         avrowrite_time = {avrowrite_time:9.2f}\n"
                                  f"           produce_time = {produce_time:9.2f}\n"
                                  f"            taradd_time = {taradd_time:9.2f}\n"
                                  f"             flush_time = {flush_time:9.2f}\n"
                                  f"  update_alertsent_time = {update_alertsent_time:9.2f}" )
                nextlog += options['log_every']

            t0 = time.perf_counter()
            objtruth = TrainingDiaObjectTruth.objects.filter( diaobject_id=diaobject.diaobject_id ).first()
            classid = gentypes[ objtruth.gentype ]
            getclass_time += time.perf_counter() - t0

            t0 = time.perf_counter()
            objsources = list( TrainingDiaSource.objects
                               .filter( diaobject_id=diaobject.diaobject_id )
                               .order_by( 'midpointtai' )
                               .values() )
            getsources_time += time.perf_counter() - t0

            t0 = time.perf_counter()
            objforced = list( TrainingDiaForcedSource.objects
                              .filter( diaobject_id=diaobject.diaobject_id )
                              .order_by( 'midpointtai' )
                              .values() )
            getforced_time += time.perf_counter() - t0

            t0 = time.perf_counter()
            objalerts = list( TrainingAlert.objects
                              .filter( diaobject_id=diaobject.diaobject_id ) )
            getalert_time += time.perf_counter() - t0

            for alert in objalerts:
                t0 = time.perf_counter()
                reconstructed = alert.reconstruct( objsources=objsources, objforced=objforced )
                reconstruct_time += time.perf_counter() - t0

                t0 = time.perf_counter()
                msgio = io.BytesIO()
                fastavro.write.schemaless_writer( msgio, schema, reconstructed )
                avrowrite_time += time.perf_counter() - t0
                if options['do']:
                    t0 = time.perf_counter()
                    producer.produce( f"{options['kafka_topic_base']}{classid}", msgio.getvalue() )
                    ids_produced.append( alert.alert_id )
                    avrowrite_time += time.perf_counter() - t0

                    t0 = time.perf_counter()
                    if options['tardir']:
                        msgio.seek(0)
                        tarinfo = tarfile.TarInfo( f'{alert.alert_id}.avro' )
                        tarinfo.size = msgio.getbuffer().nbytes
                        tarinfo.mtime = datetime.datetime.now().timestamp()
                        tarfiles[classid].addfile( tarinfo, msgio )
                        taradd_time += time.perf_counter() - t0

                if len(ids_produced) >= options['flush_every']:
                    if options['do']:
                        t0 = time.perf_counter()
                        producer.flush()
                        totflushed += len( ids_produced )
                        flush_time += time.perf_counter() - t0
                        t0 = time.perf_counter()
                        self.update_alertsent( ids_produced )
                        update_alertsent_time += time.perf_counter() - t0
                    ids_produced = []

                tot += 1

            i += 1

        if len(ids_produced) > 0:
            if options['do']:
                t0 = time.perf_counter()
                producer.flush()
                totflushed += len( ids_produced )
                flush_time += time.perf_counter() - t0
                t0 = time.perf_counter()
                self.update_alertsent( ids_produced )
                update_alertsent_time += time.perf_counter() - t0
            ids_produced = []

        #CLose tar files
        if options['do'] and options['tardir']:
            for classid, tar in tarfiles.items():
                tar.close()

        self.logger.info( f"**** Done sending {tot} alerts; {totflushed} flushed ****" )
