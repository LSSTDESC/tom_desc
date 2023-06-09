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
from elasticc2.models import TrainingAlert, ClassIdOfGentype, TrainingDiaObjectTruth

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
        parser.add_argument( '-l', '--log-every', default=10000, type=int,
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

        alerts = TrainingAlert.objects
        if options[ 'unsent_only' ]:
            alerts = alerts.filter( alertsenttimestamp__isnull=True )
        alerts = alerts.order_by( 'diasource__midpointtai' )
        self.logger.info( f"{len(alerts)} alerts to stream" )

        if len(alerts) == 0:
            return

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


        objectclassids = {}
        ids_produced = []
        totflushed = 0
        nextlog = 0
        tot = 0
        for i, alert in enumerate( alerts ):
            if ( options['stop_after'] is not None ) and ( tot >= options['stop_after'] ):
                break
            
            if alert.diaobject_id not in objectclassids:
                objtruth = TrainingDiaObjectTruth.objects.filter( diaobject_id=alert.diaobject_id ).first()
                classid = gentypes[ objtruth.gentype ]
                objectclassids[ alert.diaobject_id ] = classid
            else:
                classid = objectclassids[ alert.diaobject_id ]

            if ( options['log_every'] > 0 ) and ( i >= nextlog ):
                self.logger.info( f"Have processed {i} of {len(alerts)} alerts, {totflushed} flushed." )
                nextlog += options['log_every']

            msgio = io.BytesIO()
            fastavro.write.schemaless_writer( msgio, schema, alert.reconstruct() )
            if options['do']:
                producer.produce( f"{options['kafka_topic_base']}{classid}", msgio.getvalue() )
                ids_produced.append( alert.alert_id )

                if options['tardir']:
                    msgio.seek(0)
                    tarfiles[classid].addfile( tarfile.TarInfo( f'{alert.alert_id}.avro' ), msgio )

            if len(ids_produced) >= options['flush_every']:
                if options['do']:
                    producer.flush()
                    totflushed += len( ids_produced )
                    self.update_alertsent( ids_produced )
                ids_produced = []

            tot += 1 

        if len(ids_produced) > 0:
            if options['do']:
                producer.flush()
                totflushed += len( ids_produced )
                self.update_alertsent( ids_produced )
            ids_produced = []

        #CLose tar files
        if options['do'] and options['tardir']:
            for classid, tar in tarfiles.items():
                tar.close()

        self.logger.info( f"**** Done sending {tot} alerts; {totflushed} flushed ****" )
