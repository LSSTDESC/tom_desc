import sys
import os
import io
import time
import datetime
import traceback
import pathlib
import urllib
import logging

import confluent_kafka
import fastavro
from pymongo import MongoClient

# TODO : uncomment this next line
#   and the whole PittGoogleBroker class
#   when pittgoogle works again
# from concurrent.futures import ThreadPoolExecutor  # for pittgoogle
# import pittgoogle

_rundir = pathlib.Path(__file__).parent
_djangodir = _rundir.parent.parent.parent
_logdir = pathlib.Path( os.getenv( 'LOGDIR', '/logs' ) )

# sys.path.insert( 0, str(_rundir) )
# Add the db/management/commands directory as we include stuff from there
sys.path.append( str( _djangodir / "db/management/commands" ) )
from _consumekafkamsgs import MsgConsumer


class BrokerConsumer:
    """A class for consuming broker messages from brokers.

    Currently supports only kafka brokers, though there is some
    (currently broken and commented out) code for pulling from the
    pubsub Pitt-Google broker.

    Currently may assume that broker messages are coming in the elasticc2 v0.91 schema.

    """
    _brokername = 'unknown_broker'

    def __init__( self, server, groupid, topics=None, updatetopics=False,
                  schemaless=True, reset=False, extraconfig={},
                  schemafile=None, pipe=None, loggername="BROKER", loggername_prefix='',
                  postgres_brokermessage_model=None, mongodb_dbname=None, mongodb_collection=None,
                  **kwargs ):

        self.logger = logging.getLogger( loggername )
        self.logger.propagate = False
        logout = logging.StreamHandler( sys.stderr )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - {loggername_prefix}{loggername} - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        # self.logger.setLevel( logging.INFO )
        self.logger.setLevel( logging.DEBUG )

        self.countlogger = logging.getLogger( f"countlogger_{loggername_prefix}{loggername}" )
        self.countlogger.propagate = False
        _countlogout = logging.FileHandler( _logdir / f"brokerpoll_counts_{loggername_prefix}{loggername}.log" )
        _countformatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                             datefmt='%Y-%m-%d %H:%M:%S' )
        _countlogout.setFormatter( _countformatter )
        self.countlogger.addHandler( _countlogout )
        self.countlogger.setLevel( logging.INFO )
        # self.countlogger.setLevel( logging.DEBUG )

        if schemafile is None:
            schemafile = _djangodir / "elasticc2/management/commands/elasticc.v0_9_1.brokerClassification.avsc"

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

        # Figure out where we're saving stuff.  postgres_brokermessage_model right
        #   now only works with elasticc2.models.BrokerMessage.  Before trying it
        #   with anything else, make sure the interface works right and that it
        #   understands the alert schema.
        # mongodb_dbname is the name of the Mongo database on the mongodb running on $MONGOHOST
        #
        # This class supports saving to *both*, but usually you will probably only do
        #   one or the other.

        self.postgres_brokermessage_model = postgres_brokermessage_model
        self.mongodb_dbname = mongodb_dbname
        self.mongodb_collection = mongodb_collection
        if ( self.mongodb_dbname is None ) != ( self.mongodb_collection is None ):
            raise ValueError( "Must give either both or neither of mongodb_name and mongodb_collection" )

        if ( ( self.postgres_brokermessage_model is None ) and
             ( self.mongodb_dbname is None ) ):
            raise ValueError( "Both postgres_brokermessage_model and mongodb_dbname are None; "
                              "nowhere to save consumed messages!" )

        if self.postgres_brokermessage_model is not None:
            self.logger.info( f"Writing broker messages to postgres model "
                              f"{self.postgres_brokermessage_model.__name__}" )
        if self.mongodb_dbname is not None:
            # mongodb running on port 27017 on host $MONGOHOST; default
            #   $MONGOHOST to fastdbdev-mongodb for backwards compatibility
            #   with previous installs
            self.mongohost = os.getenv( 'MONGOHOST', 'fastdbdev-mongodb' )
            self.mongousername = urllib.parse.quote_plus(os.environ['MONGODB_ALERT_WRITER'])
            self.mongopassword = urllib.parse.quote_plus(os.environ['MONGODB_ALERT_WRITER_PASSWORD'])
            self.logger.info( f"Writing broker messages to monogdb {self.mongodb_dbname} "
                              f"collection {self.mongodb_collection}" )
            # Thought required: it would be less overhead to connect to the mongo hosdt
            #   once here and just reuse that connection.  However, if the mongodb restarts
            #   or the connection becomes invalid for any reason, we might regret holding
            #   the connection open for hours.  On the assumption that the mongo connection
            #   overhead is going to be small compared to the time it takes to receive
            #   a batch of messages, just connect to the mongodb every time we handle
            #   a message batch.  TODO: test this to see if that assumption is correct.


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
            # Only want to reset the first time the connection is opened!
            self._reset = False

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
        if self.postgres_brokermessage_model is not None:
            added = self.postgres_brokermessage_model.load_batch( messagebatch, logger=self.logger )
            self.countlogger.info( f"...added {added['addedmsgs']} messages, "
                                   f"{added['addedclassifiers']} classifiers, "
                                   f"{added['addedclassifications']} classifications. " )
        if self.mongodb_dbname is not None:
            nadded = self.mongodb_store( messagebatch )
            self.countlogger.info( f"...added {nadded} messages to mongodb {self.mongodb_dbname} "
                                   f"collection {self.mongodb_collection}" )


    def mongodb_store(self, messagebatch=None):
        if messagebatch is None:
            return 0
        connstr = ( f"mongodb://{self.mongousername}:{self.mongopassword}@{self.mongohost}:27017/"
                    f"?authSource={self.mongodb_dbname}" )
        self.logger.debug( f"mongodb connection string {connstr}" )
        client = MongoClient( connstr )
        db = getattr( client, self.mongodb_dbname )
        collection = db[ self.mongodb_collection ]
        results = collection.insert_many(messagebatch)
        return len(results.inserted_ids)


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
# I should replace this and the next one with a generic noauth consumer

class BrahmsConsumer(BrokerConsumer):
    _brokername = 'brahms'

    def __init__( self, grouptag=None, brahms_topic=None, loggername="BRAHMS", **kwargs ):
        if brahms_topic is None:
            raise RuntimeError( "Must specify brahms topic" )
        server = "brahms.lbl.gov:9092"
        groupid = "elasticc-lbnl" + ("" if grouptag is None else "-" + grouptag )
        topics = [ brahms_topic ]
        super().__init__( server, groupid, topics=topics, loggername=loggername, **kwargs )
        self.logger.info( f"Brahms group id is {groupid}, topic is {brahms_topic}" )

# ======================================================================
# This consumer is used in the tests

class TestConsumer(BrokerConsumer):
    _brokername = 'fakebroker'

    def __init__( self, grouptag=None, test_topic=None, loggername="TEST", **kwargs ):
        if test_topic is None:
            raise RuntimeError( "Must specify test topic" )
        server = "kafka-server:9092"
        groupid = "testing" + ("" if grouptag is None else "-" + grouptag )
        topics = [ test_topic ]
        super().__init__( server, groupid, topics=topics, loggername=loggername, **kwargs )
        self.logger.info( f"Test group id is {groupid}, topic is {test_topic}" )


# ======================================================================

class AntaresConsumer(BrokerConsumer):
    _brokername = 'antares'

    def __init__( self, grouptag=None,
                  usernamefile='/secrets/antares_username', passwdfile='/secrets/antares_passwd',
                  loggername="ANTARES", antares_topic='elasticc2-st1-ddf-full', **kwargs ):
        server = "kafka.antares.noirlab.edu:9092"
        groupid = "elasticc-lbnl" + ( "" if grouptag is None else "-" + grouptag )
        topics = [ antares_topic ]
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
    _brokername = 'fink'

    def __init__( self, grouptag=None, loggername="FINK", fink_topic='fink_elasticc-2022fall', **kwargs ):
        server = "134.158.74.95:24499"
        groupid = "elasticc-lbnl" + ( "" if grouptag is None else "-" + grouptag )
        topics = [ fink_topic ]
        updatetopics = False
        super().__init__( server, groupid, topics=topics, updatetopics=updatetopics,
                          loggername=loggername, **kwargs )
        self.logger.info( f"Fink group id is {groupid}" )


# ======================================================================

class AlerceConsumer(BrokerConsumer):
    _brokername = 'alerce'

    def __init__( self,
                  grouptag=None,
                  usernamefile='/secrets/alerce_username',
                  passwdfile='/secrets/alerce_passwd',
                  loggername="ALERCE",
                  early_offset=os.getenv( "ALERCE_TOPIC_RELDATEOFFSET", -4 ),
                  alerce_topic_pattern='^lc_classifier_.*_(\d{4}\d{2}\d{2})$',
                  **kwargs ):
        server = os.getenv( "ALERCE_KAFKA_SERVER", "kafka.alerce.science:9093" )
        groupid = "elasticc-lbnl" + ( "" if grouptag is None else "-" + grouptag )
        self.early_offset = int( early_offset )
        self.alerce_topic_pattern = alerce_topic_pattern
        topics = None
        updatetopics = True
        with open( usernamefile ) as ifp:
            username = ifp.readline().strip()
        with open( passwdfile ) as ifp:
            passwd = ifp.readline().strip()
        extraconfig = {  "security.protocol": "SASL_SSL",
                         "sasl.mechanism": "SCRAM-SHA-512",
                         "sasl.username": username,
                         "sasl.password": passwd }
        super().__init__( server, groupid, topics=topics, updatetopics=updatetopics, extraconfig=extraconfig,
                          loggername=loggername, **kwargs )
        self.logger.info( f"Alerce group id is {groupid}" )

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
            match = re.search( self.alerce_topic_pattern, topic )
            if match and ( match.group(1) in datestrs ) and ( topic not in self.badtopics ):
                tosub.append( topic )
        self.topics = tosub
        self.consumer.subscribe( self.topics )

# =====================================================================

# class PittGoogleBroker(BrokerConsumer):
#     _brokername = 'pitt-google'
#
#     def __init__(
#         self,
#         pitt_topic: str,
#         pitt_project: str,
#         max_workers: int = 8,  # max number of ThreadPoolExecutor workers
#         batch_maxn: int = 1000,  # max number of messages in a batch
#         batch_maxwait: int = 5,  # max seconds to wait between messages before processing a batch
#         loggername: str = "PITTGOOGLE",
#         **kwargs
#     ):
#         super().__init__(server=None, groupid=None, loggername=loggername, **kwargs)

#         topic = pittgoogle.pubsub.Topic(pitt_topic, pitt_project)
#         subscription = pittgoogle.pubsub.Subscription(name=f"{pitt_topic}-desc", topic=topic)
#         # if the subscription doesn't already exist, this will create one in the
#         # project given by the env var GOOGLE_CLOUD_PROJECT
#         subscription.touch()

#         self.consumer = pittgoogle.pubsub.Consumer(
#             subscription=subscription,
#             msg_callback=self.handle_message,
#             batch_callback=self.handle_message_batch,
#             batch_maxn=batch_maxn,
#             batch_maxwait=batch_maxwait,
#             executor=ThreadPoolExecutor(
#                 max_workers=max_workers,
#                 initializer=self.worker_init,
#                 initargs=(
#                     self.schema,
#                     subscription.topic.name,
#                     self.logger,
#                     self.countlogger
#                 ),
#             ),
#         )

#     @staticmethod
#     def worker_init(classification_schema: dict, pubsub_topic: str,
#                     broker_logger: logging.Logger, broker_countlogger: logging.Logger ):
#

    """Initializer for the ThreadPoolExecutor."""
#         global countlogger
#         global logger
#         global schema
#         global topic

#         countlogger = broker_countlogger
#         logger = broker_logger
#         schema = classification_schema
#         topic = pubsub_topic

#         logger.info( "In worker_init" )

#     @staticmethod
#     def handle_message(alert: pittgoogle.pubsub.Alert) -> pittgoogle.pubsub.Response:
#         """Callback that will process a single message. This will run in a background thread."""
#         global logger
#         global schema
#         global topic

#         logger.info( "In handle_message" )

#         message = {
#             "msg": fastavro.schemaless_reader(io.BytesIO(alert.bytes), schema),
#             "topic": topic,
#             # this is a DatetimeWithNanoseconds, a subclass of datetime.datetime
#             # https://googleapis.dev/python/google-api-core/latest/helpers.html
#             "timestamp": alert.metadata["publish_time"].astimezone(datetime.timezone.utc),
#             # there is no offset in pubsub
#             # if this cannot be null, perhaps the message id would work?
#             "msgoffset": alert.metadata["message_id"],
#         }

#         return pittgoogle.pubsub.Response(result=message, ack=True)

#     @staticmethod
#     def handle_message_batch(messagebatch: list) -> None:
#         """Callback that will process a batch of messages. This will run in the main thread."""
#         global logger
#         global countlogger

#         logger.info( "In handle_message_batch" )
#         # import pdb; pdb.set_trace()

#         added = BrokerMessage.load_batch(messagebatch, logger=logger)
#         countlogger.info(
#             f"...added {added['addedmsgs']} messages, "
#             f"{added['addedclassifiers']} classifiers, "
#             f"{added['addedclassifications']} classifications. "
#         )

#     def poll(self):
#         # this blocks indefinitely or until a fatal error
#         # use Control-C to exit
#         self.consumer.stream( pipe=self.pipe, heartbeat=60 )




