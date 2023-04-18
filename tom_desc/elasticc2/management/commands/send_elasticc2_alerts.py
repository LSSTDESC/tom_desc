import sys
import io
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
raise RuntimeError( "This is currently broken -- rob, fix for database table name refactor" )
from elasticc2.models import DiaAlert, DiaObject, DiaSource, DiaForcedSource

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
        parser.add_argument( '-d', '--through-day', type=float, default=None, required=True,
                             help="Sets simulation date: stream alerts with source through this midPointTai" )
        parser.add_argument( '-k', '--kafka-server', default='brahms.lbl.gov:9092', help="Kafka server to stream to" )
        parser.add_argument( '-t', '--kafka-topic', default='elasticc-test-only', help="Kafka topic" )
        parser.add_argument( '-s', '--alert-schema', default=f'{_rundir}/elasticc.v0_9.alert.avsc',
                             help='File with AVRO schema' )
        parser.add_argument( '--daily-delay', type=float, default=0.,
                             help="Delay this many seconds between simulated MJDs" )
        parser.add_argument( '-l', '--log-every', default=10000, type=int,
                             help="Log alerts sent at this interval; 0=don't log" )
        parser.add_argument( '--do', action='store_true', default=False,
                             help="Actually do it (otherwise, it's a dry run)" )

    @transaction.atomic
    def update_alertsent( self, ids ):
        sentalerts = DiaAlert.objects.filter( pk__in=ids )
        for sa in sentalerts:
            sa.alertSentTimestamp = datetime.datetime.now( datetime.timezone.utc )
            sa.save()
        
    def handle( self, *args, **options ):
        self.logger.info( "**** streaming starting ****" )
        self.logger.info( f"Streaming to {options['kafka_server']} topic {options['kafka_topic']}" )
        self.logger.info( f"Streaming alerts through midPointTai {options['through_day']}" )

        alerts = ( DiaAlert.objects
                   .filter( alertSentTimestamp__isnull=True,
                            diaSource__midPointTai__lte=options['through_day'] )
                   .order_by( 'diaSource__midPointTai' ) )
        self.logger.info( f"{len(alerts)} alerts to stream" )

        if len(alerts) == 0:
            return

        # import pdb; pdb.set_trace()
        schema = fastavro.schema.load_schema( options['alert_schema'] )

        if options['do']:
            producer = confluent_kafka.Producer( { 'bootstrap.servers': options[ 'kafka_server' ],
                                                   'batch.size': 131072,
                                                   'linger.ms': 50 } )
            
        ids_produced = []
        lastmjd = -99999
        nextlog = 0
        for i, alert in enumerate( alerts ):

            if ( options['log_every'] > 0 ) and ( i >= nextlog ):
                self.logger.info( f"Have sent {i} of {len(alerts)} alerts" )
                nextlog += options['log_every']

            if ( lastmjd > 0 ) and ( alert.midPointTai - lastmjd > 0.5 ):
                if len(ids_produced) > 0 :
                    if options['do']:
                        producer.flush()
                        self.update_alertsent( ids_produced )
                if options['daily_delay'] > 0:
                    self.logger.info( f"Sleeping {options['daily_delay']} at end of mjd {lastmjd}" )
                    time.sleep( options['daily_delay'] )
                lastmjd = alerts.midPointTai
                ids_produced = []
            
            msgio = io.BytesIO()
            fastavro.write.schemaless_writer( msgio, schema, alert.reconstruct() )
            if options['do']:
                producer.produce( options['kafka_topic'], msgio.getvalue() )
                ids_produced.append( alert.alertId )
                
        if len(ids_produced) > 0:
            if options['do']:
                producer.flush()
                self.update_alertsent( ids_produced )

        self.logger.info( f"**** Done sending {len(alerts)} alerts ****" )
