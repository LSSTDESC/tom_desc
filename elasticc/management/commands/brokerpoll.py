import sys
import io
import pathlib
import datetime
import logging
import json
import multiprocessing
import fastavro
import confluent_kafka
from django.core.management.base import BaseCommand, CommandError
from elasticc.models import BrokerMessage

_rundir = pathlib.Path(__file__).parent
sys.path.insert(0, str(_rundir) )
from _consumekafkamsgs import MsgConsumer

# class DateTimeEncoder( json.JSONEncoder ):
#     def default( self, obj ):
#         if isinstance( obj, datetime.datetime ):
#             return obj.isoformat()
#         else:
#             return super().default( obj )

# ======================================================================
        
class BrokerConsumer:
    def __init__( self, server, groupid, topics=None, schemaless=True, reset=False, extraconfig={},
                  schemafile=None, loggername="BROKER" ):

        self.logger = logging.getLogger( loggername )
        self.logger.propagate = False
        logout = logging.StreamHandler( sys.stderr )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - {loggername} - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        self.logger.setLevel( logging.INFO )
        # self.logger.setLevel( logging.DEBUG )

        if schemafile is None:
            schemafile = _rundir / "elasticc.v0_9.brokerClassification.avsc"
        
        self.schemaless = schemaless
        if not self.schemaless:
            raise RuntimeError( "I only know how to handle schemaless streams" )
        self.schemafile = schemafile
        self.schema = fastavro.schema.load_schema( self.schemafile )
        self.consumer = MsgConsumer( server, groupid, self.schemafile, topics,
                                     extraconsumerconfig=extraconfig,
                                     consume_nmsgs=1000, consume_timeout=1, nomsg_sleeptime=5,
                                     logger=self.logger )
        self.topics = topics
        if reset and ( self.topics is not None ):
            self.reset_to_start()

    @property
    def topics( self ):
        return self.consumer.topics

    @topics.setter
    def topics( self, topics ):
        self.consumer.subscribe( topics )
        
    def reset_to_start( self ):
        self.logger.info( "Resetting all topics to start" )
        for topic in self.topics:
            self.consumer.reset_to_start( topic )

    def handle_message_batch( self, msgs ):
        messagebatch = []
        for msg in msgs:
            timestamptype, timestamp = msg.timestamp()
            if timestamptype == confluent_kafka.TIMESTAMP_NOT_AVAILABLE:
                timestamp = None
            else:
                timestamp = datetime.datetime.fromtimestamp( timestamp / 1000,
                                                             tz=datetime.timezone.utc )
            payload = msg.value()
            if not self.schemaless:
                raise RuntimeError( "I only know how to handle schemaless streams" )
            alert = fastavro.schemaless_reader( io.BytesIO( payload ), self.schema )
            messagebatch.append( { 'topic': msg.topic(),
                                   'msgoffset': msg.offset(),
                                   'timestamp': timestamp,
                                   'msg': alert } )
        BrokerMessage.load_batch( messagebatch, logger=self.logger )
        
    def poll( self ):
        self.consumer.poll_loop( handler=self.handle_message_batch, max_consumed=None, max_runtime=None )

# ======================================================================
        
class AntaresConsumer(BrokerConsumer):
    def __init__( self, grouptag=None,
                  usernamefile='/secrets/antares_username', passwdfile='/secrets/antares_passwd',
                  loggername="ANTARES", **kwargs ):
        server = "kafka.antares.noirlab.edu:9092"
        groupid = "elasticc-lbnl-test" + ( "" if grouptag is None else "-" + grouptag )
        topics = [ 'elasticc-test-mid-august-classifications' ]
        with open( usernamefile ) as ifp:
            username = ifp.readline().strip()
        with open( passwdfile ) as ifp:
            passwd = ifp.readline().strip()
        extraconfig = {
            "api.version.request": True,
            "broker.version.fallback": "0.10.0.0",
            "api.version.fallback.ms": "0",
            "enable.auto.commit": False,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": username,
            "sasl.password": passwd,
            "ssl.ca.location": str( _rundir / "antares-ca.pem" ),
            "auto.offset.reset": "earliest",
        }
        super().__init__( server, groupid, topics=topics, extraconfig=extraconfig,
                          loggername=loggername, **kwargs )
        self.logger.info( f"Antares group id is {groupid}" )
        

# ======================================================================
        
class FinkConsumer(BrokerConsumer):
    def __init__( self, grouptag=None, loggername="FINK", **kwargs ):
        server = "134.158.74.95:24499"
        groupid = "elasticc-lbnl-test" + ( "" if grouptag is None else "-" + grouptag )
        topics = [ 'fink_elasticc-test-late-july' ]
        super().__init__( server, groupid, topics=topics, loggername=loggername, **kwargs )
        self.logger.info( f"Fink group id is {groupid}" )
        

# ======================================================================
        
class AlerceConsumer(BrokerConsumer):
    def __init__( self, grouptag=None,
                  usernamefile='/secrets/alerce_username', passwdfile='/secrets/alerce_passwd',
                  loggername="ALERCE", **kwargs ):
        server = "kafka.alerce.science:9093"
        groupid = "elasticc-lbnl-test" + ( "" if grouptag is None else "-" + grouptag )
        topics = [ 'alerce_balto_lc_classifier_20220727' ]
        with open( usernamefile ) as ifp:
            username = ifp.readline().strip()
        with open( passwdfile ) as ifp:
            passwd = ifp.readline().strip()
        extraconfig = {  "security.protocol": "SASL_PLAINTEXT",
                         "sasl.mechanism": "SCRAM-SHA-256",
                         "sasl.username": username,
                         "sasl.password": passwd }
        super().__init__( server, groupid, topics=topics, extraconfig=extraconfig,
                          loggername=loggername, **kwargs )
        self.logger.info( f"Alerce group id is {groupid}" )

# =====================================================================

class Command(BaseCommand):
    help = 'Poll ELAsTiCC Brokers'
    schemafile = _rundir / "elasticc.v0_9.brokerClassification.avsc"

    def add_arguments( self, parser ):
        parser.add_argument( '-g', '--grouptag', default=None, help="Tag to add to end of kafka group ids" )
        parser.add_argument( '-r', '--reset', default=False, action='store_true',
                             help='Reset all stream pointers' )

    def handle( self, *args, **options ):
        do_antares = True
        do_fink = False
        do_alerce = True
        join = None
        
        if do_antares:
            def launch_antares():
                consumer = AntaresConsumer( grouptag=options['grouptag'], reset=options['reset'] )
                consumer.poll()
            antares_process = multiprocessing.Process( target=launch_antares )
            antares_process.start()
            if join is None: join = antares_process

        if do_fink:
            def launch_fink():
                consumer = FinkConsumer( grouptag=options['grouptag'], reset=options['reset'] )
                consumer.poll()
            fink_process = multiprocessing.Process( target=launch_fink )
            fink_process.start()
            if join is None: join = fink_process

        if do_alerce:
            def launch_alerce():
                consumer = AlerceConsumer( grouptag=options['grouptag'], reset=options['reset'] )
                consumer.poll()
            alerce_process = multiprocessing.Process( target=launch_alerce )
            alerce_process.start()
            if join is None: join = alerce_process
            
        # Rob, put in a heartbeat thing or something
        join = join.join()
