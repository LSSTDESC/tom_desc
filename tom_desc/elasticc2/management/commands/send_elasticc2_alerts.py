import sys
import io
import socket
import math
import pathlib
import logging
import json
import time
import datetime
import confluent_kafka
import fastavro

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from elasticc2.models import PPDBAlert

_rundir = pathlib.Path(__file__).parent

class Command(BaseCommand):
    help = 'Send ELAsTiCC2 Alerts'

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
        parser.add_argument( '-d', '--through-day', type=float, default=None,
                             help=( "Sets simulation date: stream alerts with source through this midPointTai. "
                                    "Must give one of this or -a." ) )
        parser.add_argument( '-a', '--added-days', type=float, default=None,
                             help=( "Will look at greatest midpoitntai on alerts sent, and will then go to that day "
                                    "plus this many days, rounded down to the last 0.5.  (0.5 because 12:00 UTC "
                                    "is 8:00 Cero Pachon time, which should be after a night's worth of "
                                    "observations." ) )
        parser.add_argument( '-k', '--kafka-server', default='brahms.lbl.gov:9092', help="Kafka server to stream to" )
        parser.add_argument( '-t', '--kafka-topic', default='elasticc-test-20230418', help="Kafka topic" )
        parser.add_argument( '-s', '--alert-schema', default=f'{_rundir}/elasticc.v0_9_1.alert.avsc',
                             help='File with AVRO schema' )
        parser.add_argument( '--daily-delay', type=float, default=0.,
                             help="Delay this many seconds between simulated MJDs" )
        parser.add_argument( '-f', '--flush-every', default=1000, type=int,
                             help="Flush the kafka producer every this man alerts" )
        parser.add_argument( '-l', '--log-every', default=10000, type=int,
                             help="Log alerts sent at this interval; 0=don't log" )
        parser.add_argument( '-r', '--runningfile', default=f'{_rundir}/isrunning.log',
                             help=( "Will write to this file when run starts, delete when done.  Will not start "
                                    "if this file exists." ) )
        parser.add_argument( '--do', action='store_true', default=False,
                             help="Actually do it (otherwise, it's a dry run)" )

    @transaction.atomic
    def update_alertsent( self, ids ):
        sentalerts = PPDBAlert.objects.filter( pk__in=ids )
        for sa in sentalerts:
            sa.alertsenttimestamp = datetime.datetime.now( datetime.timezone.utc )
            sa.save()
        
    def handle( self, *args, **options ):

        # There is a race condition built-in here -- if the file is created by
        #   another process between when I check if it exists and when I create
        #   it here, then both processes will merrily run.  Since my use case
        #   is a nightly cron job, and I want to make sure that the previous
        #   night has finished before I start the next one, this shouldn't
        #   be a practical problem.
        runningfile = pathlib.Path( options['runningfile'] )
        if runningfile.exists():
            self.logger.warn( f"{runningfile} exists, not starting." )
            return

        try:
            with open( runningfile, "w" ) as ofp:
                ofp.write( f"{datetime.datetime.now().isoformat()} on host {socket.gethostname()}\n" )

            self.logger.info( "Figuring out starting day" )
                
            if ( ( ( options['through_day'] is None ) == ( options['added_days'] is None ) )
                 or
                 ( ( options['through_day'] is None ) and ( options['added_days'] is None ) ) ):
                raise RuntimeError( f"Must give exactly one of -d or -a: "
                                    f"-d was {options['through_day']} (type {type(options['through_day'])}) "
                                    f"and -a was {options['added_days']} (type {type(options['added_days'])}) " )

            # import pdb; pdb.set_trace()
            if options['through_day'] is not None:
                through_day = options['through_day']
            else:
                lastalertquery = ( PPDBAlert.objects
                                   .filter( alertsenttimestamp__isnull=False )
                                   .order_by( '-diasource__midpointtai' ) )
                try:
                    lastalert = lastalertquery[0]
                    t = lastalert.diasource.midpointtai
                    self.logger.info( f"Last alert sent had midpointtai {t}" )
                except IndexError as ex:
                    # No alerts have been sent yet, so find the first one
                    self.logger.info( "No alerts have been sent yet, figuring out the time of the first one." )
                    firstalertquery = PPDBAlert.objects.order_by( 'diasource__midpointtai' )
                    t = firstalertquery[0].diasource.midpointtai - 1
                    self.logger.info( "First alert is at MJD {t+1}" )
                through_day = math.floor( t + 0.5 ) + options['added_days'] + 0.5
            
            self.logger.info( "**** streaming starting ****" )
            self.logger.info( f"Streaming to {options['kafka_server']} topic {options['kafka_topic']}" )
            self.logger.info( f"Streaming alerts through midPointTai {through_day}" )

            alerts = ( PPDBAlert.objects
                       .filter( alertsenttimestamp__isnull=True,
                                diasource__midpointtai__lte=through_day )
                       .order_by( 'diasource__midpointtai' ) )
            self.logger.info( f"{alerts.count()} alerts to stream" )

            if alerts.count() == 0:
                return

            # import pdb; pdb.set_trace()
            schema = fastavro.schema.load_schema( options['alert_schema'] )

            if options['do']:
                producer = confluent_kafka.Producer( { 'bootstrap.servers': options[ 'kafka_server' ],
                                                       'batch.size': 131072,
                                                       'linger.ms': 50 } )

            ids_produced = []
            totflushed = 0
            lastmjd = -99999
            nextlog = 0
            for i, alert in enumerate( alerts ):

                if ( options['log_every'] > 0 ) and ( i >= nextlog ):
                    self.logger.info( f"Have processed {i} of {len(alerts)} alerts, {totflushed} flushed." )
                    nextlog += options['log_every']

                if ( lastmjd > 0 ) and ( alert.midPointTai - lastmjd > 0.5 ):
                    if len(ids_produced) > 0 :
                        if options['do']:
                            producer.flush()
                            totflushed += len( ids_produced )
                            self.update_alertsent( ids_produced )
                    if options['daily_delay'] > 0:
                        self.logger.info( f"Sleeping {options['daily_delay']} at end of mjd {lastmjd}" )
                        time.sleep( options['daily_delay'] )
                    self.logger.info( f'Jumping from day {lastmjd} to {alert.midPointTai}; '
                                      f'have flushed {totflushed} alerts total.' )
                    lastmjd = alert.midPointTai
                    ids_produced = []

                msgio = io.BytesIO()
                fastavro.write.schemaless_writer( msgio, schema, alert.reconstruct() )
                if options['do']:
                    producer.produce( options['kafka_topic'], msgio.getvalue() )
                    ids_produced.append( alert.alert_id )

                if len(ids_produced) >= options['flush_every']:
                    if options['do']:
                        producer.flush()
                        totflushed += len( ids_produced )
                        self.update_alertsent( ids_produced )
                    ids_produced = []
                    
            if len(ids_produced) > 0:
                if options['do']:
                    producer.flush()
                    totflushed += len( ids_produced )
                    self.update_alertsent( ids_produced )
                ids_produced = []

            self.logger.info( f"**** Done sending {len(alerts)} alerts; {totflushed} flushed ****" )
        finally:
            runningfile.unlink()
