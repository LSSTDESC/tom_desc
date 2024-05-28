from pymongo import MongoClient
import sys
import pathlib
import logging
import fastavro
import json
import multiprocessing
import signal
import time
import confluent_kafka
import io
import os
import re
import traceback
import datetime
import collections
import atexit
from optparse import OptionParser

_rundir = pathlib.Path(__file__).parent
print(_rundir)

_logger = logging.getLogger(__name__)
if not _logger.hasHandlers():
    _logout = logging.FileHandler( _rundir / f"logs/broker.log" )
    _logger.addHandler( _logout )
    _formatter = logging.Formatter( f'[msgconsumer - %(asctime)s - %(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S' )
    _logout.setFormatter( _formatter )
_logger.setLevel( logging.DEBUG )

def _donothing( *args, **kwargs ):
    pass

def close_msg_consumer( obj ):
    obj.close()


class MsgConsumer(object):
    def __init__( self, server, groupid, schema, topics=None, extraconsumerconfig=None,consume_nmsgs=10, consume_timeout=5, nomsg_sleeptime=1, logger=_logger, username=None, password=None ):
        """Wraps a confluent_kafka.Consumer.

        server : the bootstrap.servers value
        groupid : the group.id value
        schema : filename where the schema of messages to be consumed can be found
        topics : topic name, or list of topic names, to subscribe to
        extraconsumerconfig : (optional) additional consumer config (dict)
        consume_nmsgs : number of messages to pull from the server at once (default 10)
        consume_timeout : timeout after waiting on the server for this many seconds
        nomsg_sleeptime : sleep for this many seconds after a consume_timeout before trying again
        logger : a logging object

        """

        self.consumer = None
        self.logger = logger
        self.tot_handled = 0

        self.schema = fastavro.schema.load_schema( schema )
        self.consume_nmsgs = consume_nmsgs
        self.consume_timeout = consume_timeout
        self.nomsg_sleeptime = nomsg_sleeptime
        self.username = username
        self.password = password

        consumerconfig = { "bootstrap.servers": server,
                           "auto.offset.reset": "earliest",
                           "group.id": groupid,
                           "sasl.username": self.username,
                           "sasl.password": self.password,
                           "sasl.mechanism": "SCRAM-SHA-512",
                           "security.protocol":"SASL_SSL"
        }

        if extraconsumerconfig is not None:
            consumerconfig.update( extraconsumerconfig )

        self.logger.debug( f'Initializing Kafka consumer with\n{json.dumps(consumerconfig, indent=4)}' )
        self.consumer = confluent_kafka.Consumer( consumerconfig )
        atexit.register( close_msg_consumer, self )

        self.subscribed = False
        self.subscribe( topics )

    def close( self ):
        if self.consumer is not None:
            self.logger.info( "Closing MsgConsumer" )
            self.consumer.close()
            self.consumer = None

    def subscribe( self, topics ):
        if topics is None:
            self.topics = []
        elif isinstance( topics, str ):
            self.topics = [ topics ]
        elif isinstance( topics, collections.abc.Sequence ):
            self.topics = list( topics )
        else:
            raise ValueError( f'topics must be either a string or a list' )

        servertopics = self.get_topics()
        subtopics = []
        for topic in self.topics:
            if topic not in servertopics:
                self.logger.warning( f'Topic {topic} not on server, not subscribing' )
            else:
                subtopics.append( topic )
        self.topics = subtopics

        #for topic in self.topics:
        #    st = [i for i in servertopics if topic in i]
        #    if len(st) !=0:
        #        for t in st:
        #            subtopics.append(t)
        #    else:
        #        self.logger.warning( f'Topic {topic} not on server, not subscribing' )
        #self.topics = subtopics
                
        if self.topics is not None and len(self.topics) > 0:
            self.logger.info( f'Subscribing to topics: {", ".join( self.topics )}' )
            self.consumer.subscribe( self.topics, on_assign=self._sub_callback )
        else:
            self.logger.warning( f'No existing topics given, not subscribing.' )

    def get_topics( self ):
        cluster_meta = self.consumer.list_topics()
        return [ n for n in cluster_meta.topics ]

    def print_topics( self, newlines=False ):
        topics = self.get_topics()
        if not newlines:
            self.logger.info( f"\nTopics: {', '.join(topics)}" )
        else:
            topicstr = '\n  '.join( topics )
            self.logger.info( f"\nTopics:\n  {topicstr}" )

    def _get_positions( self, partitions ):
        return self.consumer.position( partitions )
        
    def _dump_assignments( self, ofp, partitions ):
        ofp.write( f'{"Topic":<32s} {"partition":>9s} {"offset":>12s}\n' )
        for par in partitions:
            ofp.write( f"{par.topic:32s} {par.partition:9d} {par.offset:12d}\n" )
        ofp.write( "\n" )
        
    def print_assignments( self ):
        asmgt = self._get_positions( self.consumer.assignment() )
        ofp = io.StringIO()
        ofp.write( "Current partition assignments\n" )
        self._dump_assignments( ofp, asmgt )
        self.logger.info( ofp.getvalue() )
        ofp.close()

    def _sub_callback( self, consumer, partitions ):
        self.subscribed = True
        ofp = io.StringIO()
        ofp.write( "Consumer subscribed.  Assigned partitions:\n" )
        self._dump_assignments( ofp, self._get_positions( partitions ) )
        self.logger.info( ofp.getvalue() )
        ofp.close()

    def reset_to_start( self, topic ):
        partitions = self.consumer.list_topics( topic ).topics[topic].partitions
        self.logger.info( f'Resetting partitions for topic {topic}' )
        # partitions is a map
        partlist = []
        # Must consume one message to really hook up to the topic
        self.consume_one_message( handler=_donothing, timeout=10 )
        for i in range(len(partitions)):
            self.logger.info( f'...resetting partition {i}' )
            curpart = confluent_kafka.TopicPartition( topic, i )
            lowmark, highmark = self.consumer.get_watermark_offsets( curpart )
            self.logger.debug( f'Partition {curpart.topic} has id {curpart.partition} '
                               f'and current offset {curpart.offset}; lowmark={lowmark} '
                               f'and highmark={highmark}' )
            curpart.offset = lowmark
            if lowmark < highmark:
                self.consumer.seek( curpart )
            partlist.append( curpart )
        self.logger.info( f'Committing partition offsets.' )
        self.consumer.commit( offsets=partlist )
        self.tot_handled = 0

    def consume_one_message( self, timeout=None, handler=None ):
        """Both calls handler and returns a batch of 1 message."""
        timeout = self.consume_timeout if timeout is None else timeout
        self.logger.info( f"Trying to consume one message with timeout {timeout}...\n" )
        msgs = self.consumer.consume( 1, timeout=timeout )
        if len(msgs) == 0:
            return None
        else:
            self.tot_handled += len(msgs)
            if handler is not None:
                handler( msgs )
            else:
                self.default_handle_message_batch( msgs )

    def default_handle_message_batch( self, msgs ):
        self.logger.info( f'Got {len(msgs)}; have received {self._tot_handled} so far.' )
                
    def echoing_handle_message_batch( self, msgs ):
        self.logger.info( f'Handling {len(msgs)} messages' )
        for msg in msgs:
            ofp = io.StringIO( f"Topic: {msg.topic()} ; Partition: {msg.partition()} ; "
                               f"Offset: {msg.offset()} ; Key: {msg.key()}\n" )
            alert = fastavro.schemaless_reader( io.BytesIO(msg.value()), self.schema )
            ofp.write( json.dumps( alert, indent=4, sort_keys=True ) )
            ofp.write( "\n" )
            self.logger.info( ofp.getvalue() )
            ofp.close()
        self.logger.info( f'Have handled {self.tot_handled} messages so far' )

    def poll_loop( self, handler=None, max_consumed=None, pipe=None, max_runtime=datetime.timedelta(hours=1) ):
        """Calls handler with batches of messages.

        handler : a callback that's called with batches of messages (the list
                  returned by confluent_kafka.Consumer.consume().
        max_consumed : Quit polling after this many messages have been
                       consumed (default: no limit)
        pipe : A pipe to send regular heartbeats to, and to listen for "die" messages from.
        max_runtime : Quit polling after this much time has elapsed;
                      must be a datetime.timedelta object.  (Default: 1h.)

        returns True if consumed ?max_consumed or timed out, False if died due to die command
        """
        nconsumed = 0
        starttime = datetime.datetime.now()
        keepgoing = True
        retval = True
        while keepgoing:
            self.logger.debug( f"Trying to consume {self.consume_nmsgs} messages "
                               f"with timeout {self.consume_timeout}..." )
            msgs = self.consumer.consume( self.consume_nmsgs, timeout=self.consume_timeout )
            if len(msgs) == 0:
                self.logger.debug( f"No messages, sleeping {self.nomsg_sleeptime} sec" )
                time.sleep( self.nomsg_sleeptime )
            else:
                self.logger.debug( f"...got {len(msgs)} messages" )
                self.tot_handled += len(msgs)
                if handler is not None:
                    handler( msgs )
                else:
                    self.default_handle_message_batch( msgs )
            nconsumed += len( msgs )
            runtime = datetime.datetime.now() - starttime
            if ( ( ( max_consumed is not None ) and ( nconsumed >= max_consumed ) )
                 or
                 ( ( max_runtime is not None ) and ( runtime > max_runtime ) ) ):
                keepgoing = False
            if pipe is not None:
                pipe.send( { "message": "ok", "nconsumed": nconsumed, "runtime": runtime } )
                if pipe.poll():
                    msg = pipe.recv()
                    if ( 'command' in msg ) and ( msg['command'] == 'die' ):
                        self.logger.info( "Exiting poll loop due to die command." )
                        retval = False
                        keepgoing = False
                    else:
                        self.logger.error( f"Received unknown message from pipe, ignoring: {msg}" )

        self.logger.info( f"Stopping poll loop after consuming {nconsumed} messages during {runtime}" )
        return retval


class BrokerConsumer:
    def __init__( self, server, groupid, topics=None, updatetopics=False,
                  schemaless=True, reset=False, extraconfig={}, collection=None, 
                  schemafile=None, pipe=None, loggername="BROKER", username=None, password=None, **kwargs ):

        self.logger = logging.getLogger( loggername )
        self.logger.propagate = False
        logout = logging.FileHandler( _rundir / f"logs/broker.log"  )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - {loggername} - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        self.logger.setLevel( logging.DEBUG )

        if schemafile is None:
            schemafile = _rundir / "elasticc.v0_9_1.brokerClassification.avsc"
 
        self.countlogger = logging.getLogger( f"countlogger_{loggername}" )
        self.countlogger.propagate = False
        _countlogout = logging.FileHandler( _rundir / f"logs/brokerpoll_counts_{loggername}.log" )
        _countformatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                             datefmt='%Y-%m-%d %H:%M:%S' )
        _countlogout.setFormatter( _countformatter )
        self.countlogger.addHandler( _countlogout )
        self.countlogger.setLevel( logging.DEBUG )

        self.countlogger.info( f"************ Starting Brokerconsumer for {loggername} ****************" )

        self.pipe = pipe
        self.server = server
        self.groupid = groupid
        self.topics = topics
        self._updatetopics = updatetopics
        self._reset = reset
        self.extraconfig = extraconfig
        self.username = username
        self.password = password

        self.schemaless = schemaless
        if not self.schemaless:
            self.countlogger.error( "CRASHING.  I only know how to handle schemaless streams." )
            raise RuntimeError( "I only know how to handle schemaless streams" )
        self.schemafile = schemafile
        self.schema = fastavro.schema.load_schema( self.schemafile )

        self.nmessagesconsumed = 0
        
        self.countlogger.info( f"************ Connecting to MongoDB {loggername} ****************" )
        client = MongoClient("mongodb://fastdbdev-mongodb:27017/")    
        self.db = client.alerts
        self.collection = self.db[collection]
        self.countlogger.info(self.db)
        self.countlogger.info(self.collection)
    
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
                self.consumer = MsgConsumer( self.server, self.groupid, self.schemafile, self.topics, extraconsumerconfig=self.extraconfig, consume_nmsgs=1000, consume_timeout=1, nomsg_sleeptime=5, logger=self.logger, username=self.username, password=self.password )
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
            # Only want to reset the first time the connection is opened!
            self._reset = False

        self.countlogger.info( f"**************** Consumer connection opened *****************" )

    def close_connection( self ):
        self.countlogger.info( f"**************** Closing consumer connection ******************" )
        self.consumer.close()
        self.consumer = None


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
                timestamp = datetime.datetime.fromtimestamp( timestamp / 1000, tz=datetime.timezone.utc )

            payload = msg.value()
            if not self.schemaless:
                self.countlogger.error( "I only know how to handle schemaless streams" )
                raise RuntimeError( "I only know how to handle schemaless streams" )
            alert = fastavro.schemaless_reader( io.BytesIO( payload ), self.schema )
            messagebatch.append( { 'topic': msg.topic(),
                                   'msgoffset': msg.offset(),
                                   'timestamp': timestamp,
                                   'msg': alert } )
        added = self.store( messages = messagebatch )
        #self.countlogger.info( f"...added {added['addedmsgs']} messages, "
#                               f"{added['addedclassifiers']} classifiers, "
#                               f"{added['addedclassifications']} classifications#. " )

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


    def store(self, messages = None):
        
        messagebatch = messages
        results = self.collection.insert_many(messagebatch)
        count = len(results.inserted_ids)
        self.logger.info(f"Inserted {count} messages")
        
        return


class AlerceConsumer(BrokerConsumer):

    def __init__( self, loggername="ALERCE", early_offset=os.getenv( "ALERCE_TOPIC_RELDATEOFFSET", -4 ), **kwargs ):


        username = os.environ['ALERCE_USERNAME']
        password = os.environ['ALERCE_PASSWORD']


        server = "b-2-public.publicproduction.o8ncxm.c18.kafka.us-east-1.amazonaws.com:9196,b-3-public.publicproduction.o8ncxm.c18.kafka.us-east-1.amazonaws.com:9196,b-1-public.publicproduction.o8ncxm.c18.kafka.us-east-1.amazonaws.com:9196"
        #server = 'b-2-public.publicproduction.o8ncxm.c18.kafka.us-east-1.amazonaws.com:9196'
        groupid = "external_elasticc-4"
        self.early_offset = int( early_offset )
        topics = None
        updatetopics = True
        self.username = username
        self.password = password
        extraconfig = {}
        collection = 'alerce'

        super().__init__( server, groupid, topics=topics, updatetopics=updatetopics, extraconfig=extraconfig, colection=collection, loggername=loggername, username=username, password=password, **kwargs )
        self.logger.info( f"ALERCE group id is {groupid}" )

        self.badtopics = [ 'lc_classifier_balto_20230807' ]

    def update_topics( self, *args, **kwargs ):
        now = datetime.datetime.now()
        datestrs = []
        for ddays in range(self.early_offset, 3):
            then = now + datetime.timedelta( days=ddays )
            datestrs.append( f"{then.year:04d}{then.month:02d}{then.day:02d}" )
        tosub = []
        topics = self.consumer.get_topics()
        for topic in topics:
            match = re.search( '^lc_classifier_.*_(\d{4}\d{2}\d{2})$', topic )
            if match and ( match.group(1) in datestrs ) and ( topic not in self.badtopics ):
                tosub.append( topic )
        self.topics = tosub
        self.consumer.subscribe( self.topics )
        
class Broker(object):

    def __init__( self, reset=False, broker_list=None, username=None, password=None, *args, **kwargs ):

        self.logger = logging.getLogger( "brokerpoll_baselogger" )
        self.logger.propagate = False
        logout = logging.FileHandler( _rundir / f"logs/brokerpoll.log" )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - brokerpoll - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        self.logger.setLevel( logging.DEBUG )
        self.reset = reset
        self.broker_list = broker_list
        self.username = username
        self.password = password


    def sigterm( self, sig="TERM" ):
        self.logger.warning( f"Got a {sig} signal, trying to die." )
        self.mustdie = True

    def launch_broker( self, brokerclass, pipe, **options ):
        signal.signal( signal.SIGINT,
                       lambda sig, stack: self.logger.warning( f"{brokerclass.__name__} ignoring SIGINT" ) )
        signal.signal( signal.SIGTERM,
                       lambda sig, stack: self.logger.warning( f"{brokerclass.__name__} ignoring SIGTERM" ) )
        consumer = brokerclass( pipe=pipe )
        consumer.poll()

    def broker_poll( self, *args, **options ):
        self.logger.info( "******** brokerpoll starting ***********" )

        self.mustdie = False
        signal.signal( signal.SIGTERM, lambda sig, stack: self.sigterm( "TERM" ) )
        signal.signal( signal.SIGINT, lambda sig, stack: self.sigterm( "INT" ) )

        #brokerstodo = { 'antares': AntaresConsumer,
        #                 'fink': FinkConsumer,
        #                 'alerce': AlerceConsumer,
        #                'ztf': PublicZTFConsumer }
        brokerstodo = { 'alerce': AlerceConsumer }

        brokers = {}

        # Launch a process for each broker that will poll that broker indefinitely

        for name,brokerclass in brokerstodo.items():
            if name in self.broker_list:
                self.logger.info( f"Launching thread for {name}" )
                parentconn, childconn = multiprocessing.Pipe()
                proc = multiprocessing.Process( target=self.launch_broker(brokerclass, childconn, **options) )
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
                        self.logger.error( f"It's been {dt:.0f} seconds since last heartbeat from {name}; "f"will restart." )
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
        

if __name__ == '__main__':
    
    logger = logging.getLogger( "brokerpoll_baselogger" )
    logger.propagate = False
    logout = logging.FileHandler( _rundir / f"logs/brokerpoll.log" )
    logger.addHandler( logout )
    formatter = logging.Formatter( f'[%(asctime)s - brokerpoll - %(levelname)s] - %(message)s',datefmt='%Y-%m-%d %H:%M:%S' )
    logout.setFormatter( formatter )
    logger.setLevel( logging.DEBUG )


    parser = OptionParser()
    parser.add_option('-r', '--reset', action='store_true', default=False, help='Reset all stream pointers')
    parser.add_option('-b', '--broker_list', help='Set list of kafka brokers')

    (options, args) = parser.parse_args()

    broker = Broker(reset=options.reset, broker_list=options.broker_list)
    
    poll = broker.broker_poll(reset=options.reset)
