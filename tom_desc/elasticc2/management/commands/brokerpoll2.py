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
from elasticc2.models import BrokerMessage

_rundir = pathlib.Path(__file__).parent
sys.path.insert(0, str(_rundir) )
# Add the db/management/commands directory as we include stuff from there
sys.path.append( str(_rundir.parent.parent.parent / "db/management/commands" ) )
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
                  schemafile=None, pipe=None, loggername="BROKER", **kwargs ):

        self.logger = logging.getLogger( loggername )
        self.logger.propagate = False
        logout = logging.StreamHandler( sys.stderr )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - {loggername} - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        self.logger.setLevel( logging.INFO )
        # self.logger.setLevel( logging.DEBUG )

        self.countlogger = logging.getLogger( f"countlogger_{loggername}" )
        self.countlogger.propagate = False
        _countlogout = logging.FileHandler( _rundir.parent.parent.parent / f"logs/brokerpoll_counts_{loggername}.log" )
        _countformatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                             datefmt='%Y-%m-%d %H:%M:%S' )
        _countlogout.setFormatter( _countformatter )
        self.countlogger.addHandler( _countlogout )
        self.countlogger.setLevel( logging.INFO )

        if schemafile is None:
            schemafile = _rundir / "elasticc.v0_9_1.brokerClassification.avsc"

        self.countlogger.info( f"************ Starting Brokerconsumer for {loggername} ****************" )

        self.pipe = pipe
        self.server = server
        self.groupid = groupid
        self.topics = topics
        self._updatetopics = updatetopics
        self._reset = reset
        self.extraconfig = extraconfig

        self.schemaless = schemaless
        if not self.schemaless:
            self.countlogger.error( "CRASHING.  I only know how to handle schemaless streams." )
            raise RuntimeError( "I only know how to handle schemaless streams" )
        self.schemafile = schemafile
        self.schema = fastavro.schema.load_schema( self.schemafile )

        self.nmessagesconsumed = 0


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
                    self.countlogger.error( "Repeated exceptions connecting to broker, punting." )
                    raise RuntimeError( "Failed to connect to broker" )

        if self._reset and ( self.topics is not None ):
            self.countlogger.info( f"*************** Resetting to start of broker kafka stream ***************" )
            self.reset_to_start()

        self.countlogger.info( f"**************** Consumer connection opened *****************" )

    def close_connection( self ):
        self.countlogger.info( f"**************** Closing consumer connection ******************" )
        self.consumer.close()
        self.consumer = None

    def update_topics( self, *args, **kwargs ):
        self.countlogger.info( "Subclass must implement this if you use it." )
        raise NotImplementedError( "Subclass must implement this if you use it." )

    def reset_to_start( self ):
        self.logger.info( "Resetting all topics to start" )
        for topic in self.topics:
            self.consumer.reset_to_start( topic )

    def handle_message_batch( self, msgs ):
        messagebatch = []
        self.countlogger.info( f"Handling {len(msgs)} messages; consumer has received "
                               f"{self.consumer.tot_handled} messages." )
        for msg in msgs:
            timestamptype, timestamp = msg.timestamp()
            if timestamptype == confluent_kafka.TIMESTAMP_NOT_AVAILABLE:
                timestamp = None
            else:
                timestamp = datetime.datetime.fromtimestamp( timestamp / 1000,
                                                             tz=datetime.timezone.utc )
            payload = msg.value()
            if not self.schemaless:
                self.countlogger.error( "I only know how to handle schemaless streams" )
                raise RuntimeError( "I only know how to handle schemaless streams" )
            alert = fastavro.schemaless_reader( io.BytesIO( payload ), self.schema )
            messagebatch.append( { 'topic': msg.topic(),
                                   'msgoffset': msg.offset(),
                                   'timestamp': timestamp,
                                   'msg': alert } )
        added = BrokerMessage.load_batch( messagebatch, logger=self.logger )
        self.countlogger.info( f"...added {added['addedmsgs']} messages, "
                               f"{added['addedclassifiers']} classifiers, "
                               f"{added['addedclassifications']} classifications. " )

    def poll( self, restart_time=datetime.timedelta(minutes=30) ):
        self.create_connection()
        while True:
            if self._updatetopics:
                self.update_topics()
            strio = io.StringIO("")
            if len(self.consumer.topics) == 0:
                self.logger.info( "No topics, will wait 10s and reconnect." )
                time.sleep(10)
            else:
                self.logger.info( f"Subscribed to topics: {self.consumer.topics}; starting poll loop." )
                self.countlogger.info( f"Subscribed to topics: {self.consumer.topics}; starting poll loop." )
                try:
                    happy = self.consumer.poll_loop( handler=self.handle_message_batch,
                                                     max_consumed=None, max_runtime=restart_time,
                                                     pipe=self.pipe )
                    if happy:
                        strio.write( f"Reached poll timeout for {self.server}; "
                                     f"handled {self.consumer.tot_handled} messages. " )
                    else:
                        strio.write( f"Poll loop received die command after handling "
                                     f"{self.consumer.tot_handled} messages.  Exiting." )
                        self.logger.info( strio.getvalue() )
                        self.countlogger.info( strio.getvalue() )
                        self.close_connection()
                        return
                except Exception as e:
                    otherstrio = io.StringIO("")
                    traceback.print_exc( file=otherstrio )
                    self.logger.warning( otherstrio.getvalue() )
                    strio.write( f"Exception polling: {str(e)}. " )

            if self.pipe.poll():
                msg = self.pipe.recv()
                if ( 'command' in msg ) and ( msg['command'] == 'die' ):
                    self.logger.info( "No topics, but also exiting broker poll due to die command." )
                    self.countlogger.info( "No topics, but also existing broker poll due to die command." )
                    self.close_connection()
                    return
            strio.write( "Reconnecting.\n" )
            self.logger.info( strio.getvalue() )
            self.countlogger.info( strio.getvalue() )
            self.close_connection()
            if self._updatetopics:
                self.topics = None
            self.create_connection()

# ======================================================================

class BrahmsConsumer(BrokerConsumer):
    def __init__( self, grouptag=None, brahms_topic=None, loggername="BRAHMS", **kwargs ):
        if brahms_topic is None:
            raise RuntimeError( "Must specify brahms topic" )
        server = "brahms.lbl.gov:9092"
        groupid = "elasticc-lbnl" + ("" if grouptag is None else "-" + grouptag )
        topics = [ brahms_topic ]
        super().__init__( server, groupid, topics=topics, loggername=loggername, **kwargs )
        self.logger.info( f"Brahms group id is {groupid}, topic is {brahms_topic}" )

# ======================================================================

class AntaresConsumer(BrokerConsumer):
    def __init__( self, grouptag=None,
                  usernamefile='/secrets/antares_username', passwdfile='/secrets/antares_passwd',
                  loggername="ANTARES", **kwargs ):
        server = "kafka.antares.noirlab.edu:9092"
        groupid = "elasticc-lbnl" + ( "" if grouptag is None else "-" + grouptag )
        topics = [ 'elasticc-2022fall-classifications' ]
        updatetopics = False
        with open( usernamefile ) as ifp:
            username = ifp.readline().strip()
        with open( passwdfile ) as ifp:
            passwd = ifp.readline().strip()
        extraconfig = {
            "api.version.request": True,
            "broker.version.fallback": "0.10.0.0",
            "api.version.fallback.ms": "0",
            "enable.auto.commit": True,
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
        groupid = "elasticc-lbnl" + ( "" if grouptag is None else "-" + grouptag )
        topics = [ 'fink_elasticc-2022fall' ]
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
        groupid = "elasticc-lbnl" + ( "" if grouptag is None else "-" + grouptag )
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
# To make this die cleanly, send the USR1 signal to it
# (SIGTERM doesn't work because django captures that, sadly.)

class Command(BaseCommand):
    help = 'Poll ELAsTiCC Brokers'
    schemafile = _rundir / "elasticc.v0_9.brokerClassification.avsc"

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )
        self.logger = logging.getLogger( "brokerpoll_baselogger" )
        self.logger.propagate = False
        logout = logging.FileHandler( _rundir.parent.parent.parent / f"logs/brokerpoll.log" )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - brokerpoll - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        self.logger.setLevel( logging.INFO )

    def add_arguments( self, parser ):
        parser.add_argument( '--do-alerce', action='store_true', default=False, help="Poll from ALERCE" )
        parser.add_argument( '--do-antares', action='store_true', default=False, help="Poll from ANTARES" )
        parser.add_argument( '--do-fink', action='store_true', default=False, help="Poll from FINK" )
        parser.add_argument( '--do-brahms', action='store_true', default=False,
                             help="Poll from Rob's test kafka server" )
        parser.add_argument( '--brahms-topic', default=None,
                             help="Topic to poll on brahms (required if --do-brahms is True)" )
        
        parser.add_argument( '-g', '--grouptag', default=None, help="Tag to add to end of kafka group ids" )
        parser.add_argument( '-r', '--reset', default=False, action='store_true',
                             help='Reset all stream pointers' )

    def sigterm( self, sig="TERM" ):
        self.logger.warning( f"Got a {sig} signal, trying to die." )
        self.mustdie = True

    def launch_broker( self, brokerclass, pipe, **options ):
        signal.signal( signal.SIGINT,
                       lambda sig, stack: self.logger.warning( f"{brokerclass.__name__} ignoring SIGINT" ) )
        signal.signal( signal.SIGTERM,
                       lambda sig, stack: self.logger.warning( f"{brokerclass.__name__} ignoring SIGTERM" ) )
        signal.signal( signal.SIGUSR1,
                       lambda sig, stack: self.logger.warning( f"{brokerclass.__name__} ignoring SIGUSR1" ) )
        consumer = brokerclass( pipe=pipe, **options )
        consumer.poll()

    def handle( self, *args, **options ):
        self.logger.info( "******** brokerpoll starting ***********" )

        self.mustdie = False
        signal.signal( signal.SIGTERM, lambda sig, stack: self.sigterm( "TERM" ) )
        signal.signal( signal.SIGINT, lambda sig, stack: self.sigterm( "INT" ) )
        signal.signal( signal.SIGUSR1, lambda sig, stack: self.sigterm( "USR1" ) )

        brokerstodo = {}
        if options['do_alerce']:
            brokerstodo['alerce'] = AlerceConsumer
        if options['do_antares']:
            brokerstodo['antares'] = AntaresConsumer
        if options['do_fink']:
            brokerstodo['fink'] = FinkConsumer
        if options['do_brahms']:
            brokerstodo['brahms'] = BrahmsConsumer
        if len( brokerstodo ) == 0:
            self.logger.error( "Must give at least one broker to listen to." )
            raise RuntimeError( "No brokers given to listen to." )
            
        # Launch a process for each broker that will poll that broker indefinitely

        brokers = {}
        for name,brokerclass in brokerstodo.items():
            self.logger.info( f"Launching thread for {name}" )
            parentconn, childconn = multiprocessing.Pipe()
            proc = multiprocessing.Process( target=lambda: self.launch_broker(brokerclass, childconn, **options) )
            proc.start()
            brokers[name] = { "process": proc,
                              "pipe": parentconn,
                              "lastheartbeat": time.monotonic() }

        # Listen for a heartbeat from all processes.
        # If we don't get a heartbeat for 5min,
        # kill that process and restart it.

        heartbeatwait = 2
        toolongsilent = 300
        while not self.mustdie:
            try:
                pipelist = [ b['pipe'] for i,b in brokers.items() ]
                whichpipe = multiprocessing.connection.wait( pipelist, timeout=heartbeatwait )

                brokerstorestart = set()
                for name, broker in brokers.items():
                    try:
                        while broker['pipe'].poll():
                            msg = broker['pipe'].recv()
                            if ( 'message' not in msg ) or ( msg['message'] != "ok" ):
                                self.logger.error( f"Got unexpected message from thread for {name}; "
                                                   f"will restart: {msg}" )
                                brokerstorestart.add( name )
                            else:
                                self.logger.debug( f"Got heartbeat from {name}" )
                                broker['lastheartbeat'] = time.monotonic()
                    except Exception as ex:
                        self.logger.error( f"Got exception listening for heartbeat from {name}; will restart." )
                        brokerstorestart.add( name )

                for name, broker in brokers.items():
                    dt = time.monotonic() - broker['lastheartbeat']
                    if dt > toolongsilent:
                        self.logger.error( f"It's been {dt:.0f} seconds since last heartbeat from {name}; "
                                           f"will restart." )
                        brokerstorestart.add( name )

                for torestart in brokerstorestart:
                    self.logger.warning( f"Killing and restarting process for {torestart}" )
                    brokers[torestart]['process'].kill()
                    brokers[torestart]['pipe'].close()
                    del brokers[torestart]
                    parentconn, childconn = multiprocessing.Pipe()
                    proc = multiprocessing.Process( target=lambda: self.launch_broker( brokerstodo[torestart],
                                                                                      childconn, **options ) )
                    proc.start()
                    brokers[torestart] = { "process": proc,
                                           "pipe": parentconn,
                                           "lastheartbeat": time.monotonic() }
            except Exception as ex:
                self.logger.exception( "brokerpoll got an exception, going to shut down." )
                self.mustdie = True

        # I chose 20s since kubernetes sends a TERM and then waits 30s before shutting things down
        self.logger.warning( "Shutting down.  Sending die to all processes and waiting 20s" )
        for name, broker in brokers.items():
            broker['pipe'].send( { "command": "die" } )
        time.sleep( 20 )
        self.logger.warning( "Exiting." )
        return
