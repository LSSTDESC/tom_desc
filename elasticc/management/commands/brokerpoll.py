import sys
import io
import re
import pathlib
import time
import datetime
import logging
import traceback
import signal
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
    def __init__( self, server, groupid, topics=None, updatetopics=False,
                  schemaless=True, reset=False, extraconfig={},
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

        self.server = server
        self.groupid = groupid
        self.topics = topics
        self._updatetopics = updatetopics
        self._reset = reset
        self.extraconfig = extraconfig
        
        self.schemaless = schemaless
        if not self.schemaless:
            raise RuntimeError( "I only know how to handle schemaless streams" )
        self.schemafile = schemafile
        self.schema = fastavro.schema.load_schema( self.schemafile )

        self.create_connection()
        
    @property
    def reset( self ):
        return self._reset

    @reset.setter
    def reset( self, val ):
        self._reset = val
        
    def create_connection( self ):
        countdown = 5
        while countdown >= 0:
            try:
                self.consumer = MsgConsumer( self.server, self.groupid, self.schemafile, self.topics,
                                             extraconsumerconfig=self.extraconfig,
                                             consume_nmsgs=1000, consume_timeout=1, nomsg_sleeptime=5,
                                             logger=self.logger )
                countdown = -1
            except Exception as e:
                countdown -= 1
                strio = io.StringIO("")
                strio.write( f"Exception connecting to broker: {str(e)}" )
                traceback.print_exc( file=strio )
                self.logger.warning( strio.getvalue() )
                if countdown >= 0:
                    self.logger.warning( "Sleeping 5s and trying again." )
                    time.sleep(5)
                else:
                    self.logger.error( "Repeated exceptions connecting to broker, punting." )
                    raise RuntimeError( "Failed to connect to broker" )
            
        if self._reset and ( self.topics is not None ):
            self.reset_to_start()

    def close_connection( self ):
        self.consumer.close()
        self.consumer = None
            
    def update_topics( self, *args, **kwargs ):
        raise NotImplementedError( "Subclass must implement this if you use it." )
        
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
        
    def poll( self, restart_time=datetime.timedelta(minutes=30) ):
        while True:
            if self._updatetopics:
                self.update_topics()
            self.logger.info( f"Subscribed to topics: {self.consumer.topics}; starting poll loop." )
            strio = io.StringIO("")
            try:
                self.consumer.poll_loop( handler=self.handle_message_batch,
                                         max_consumed=None, max_runtime=restart_time )
                strio.write( f"Reached poll timeout; handled {self.consumer.tot_handled} messages. " )
            except Exception as e:
                otherstrio = io.StringIO("")
                traceback.print_exc( file=otherstrio )
                self.logger.warning( otherstrio.getvalue() )
                strio.write( f"Exception polling: {str(e)}. " )
            strio.write( "Reconnecting.\n" )
            self.logger.info( strio.getvalue() )
            self.close_connection()
            if self._updatetopics:
                self.topics = None
            self.create_connection()

# ======================================================================
        
class AntaresConsumer(BrokerConsumer):
    def __init__( self, grouptag=None,
                  usernamefile='/secrets/antares_username', passwdfile='/secrets/antares_passwd',
                  loggername="ANTARES", **kwargs ):
        server = "kafka.antares.noirlab.edu:9092"
        groupid = "elasticc-lbnl-test" + ( "" if grouptag is None else "-" + grouptag )
        topics = [ 'elasticc-test-early-september-classifications' ]
        updatetopics = False
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
        super().__init__( server, groupid, topics=topics, updatetopics=updatetopics,
                          extraconfig=extraconfig, loggername=loggername, **kwargs )
        self.logger.info( f"Antares group id is {groupid}" )
        

# ======================================================================
        
class FinkConsumer(BrokerConsumer):
    def __init__( self, grouptag=None, loggername="FINK", **kwargs ):
        server = "134.158.74.95:24499"
        groupid = "elasticc-lbnl-test" + ( "" if grouptag is None else "-" + grouptag )
        topics = [ 'fink_elasticc-test-early-september' ]
        updatetopics = False
        super().__init__( server, groupid, topics=topics, updatetopics=updatetopics,
                          loggername=loggername, **kwargs )
        self.logger.info( f"Fink group id is {groupid}" )
        

# ======================================================================
        
class AlerceConsumer(BrokerConsumer):
    def __init__( self, grouptag=None,
                  usernamefile='/secrets/alerce_username', passwdfile='/secrets/alerce_passwd',
                  loggername="ALERCE", **kwargs ):
        server = "kafka.alerce.science:9093"
        groupid = "elasticc-lbnl-test" + ( "" if grouptag is None else "-" + grouptag )
        topics = None
        updatetopics = True
        with open( usernamefile ) as ifp:
            username = ifp.readline().strip()
        with open( passwdfile ) as ifp:
            passwd = ifp.readline().strip()
        extraconfig = {  "security.protocol": "SASL_PLAINTEXT",
                         "sasl.mechanism": "SCRAM-SHA-256",
                         "sasl.username": username,
                         "sasl.password": passwd }
        super().__init__( server, groupid, topics=topics, updatetopics=updatetopics, extraconfig=extraconfig,
                          loggername=loggername, **kwargs )
        self.logger.info( f"Alerce group id is {groupid}" )

    def update_topics( self, *args, **kwargs ):
        now = datetime.datetime.now()
        datestrs = []
        for ddays in range(-4, 3):
            then = now + datetime.timedelta( days=ddays )
            datestrs.append( f"{then.year:04d}{then.month:02d}{then.day:02d}" )
        tosub = []
        topics = self.consumer.get_topics()
        for topic in topics:
            match = re.search( '^alerce_elasticc_.*_(\d{4}\d{2}\d{2})$', topic )
            if match and ( match.group(1) in datestrs ):
                tosub.append( topic )
        self.topics = tosub
        self.consumer.subscribe( self.topics )
            
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
        do_fink = True
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

        # Make sure that atexit stuff gets run when we get a TERM singal
        # signal.signal( signal.SIGTERM, lambda *args : sys.exit(1) )
        #...doesn't seem to work.  I bet django/tom subvert this.  Sigh.
        
        # Rob, put in a heartbeat thing or something
        join = join.join()
