import sys
import io
import time
import datetime
import atexit
import collections
import logging
import json
import fastavro
import confluent_kafka

_logger = logging.getLogger(__name__)
if not _logger.hasHandlers():
    _logout = logging.StreamHandler( sys.stderr )
    _logger.addHandler( _logout )
    _formatter = logging.Formatter( f'[msgconsumer - %(asctime)s - %(levelname)s] - %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S' )
    _logout.setFormatter( _formatter )
_logger.setLevel( logging.INFO )
# _logger.setLevel( logging.DEBUG )

def _donothing( *args, **kwargs ):
    pass

def close_msg_consumer( obj ):
    obj.close()

class MsgConsumer(object):
    def __init__( self, server, groupid, schema, topics=None,
                  extraconsumerconfig=None, consume_nmsgs=10, consume_timeout=5, nomsg_sleeptime=1,
                  logger=_logger ):
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
        
        consumerconfig = { "bootstrap.servers": server,
                           "auto.offset.reset": "earliest",
                           "group.id": groupid }
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
            sys.stderr.write( "Closing MsgConsumer\n" )
            self.consumer.close()
            self.consumer = None
        
    def __del__( self ):
        self.close()
        
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
                
        if self.topics is not None and len(self.topics) > 0:
            self.logger.info( f'Subscribing to topics: {", ".join( topics )}' )
            self.consumer.subscribe( topics, on_assign=self._sub_callback )
        else:
            self.logger.warning( f'No existing topics given, not subscribing.' )

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

    def poll_loop( self, handler=None, max_consumed=None, max_runtime=datetime.timedelta(hours=1) ):
        """Calls handler with batches of messages.

        handler : a callback that's called with batches of messages (the list
                  returned by confluent_kafka.Consumer.consume().
        max_consumed : Quit polling after this many messages have been
                       consumed (default: no limit)
        max_runtime : Quit polling after this much time has elapsed;
                      must be a datetime.timedelta object.  (Default: 1h.)

        """
        nconsumed = 0
        starttime = datetime.datetime.now()
        keepgoing = True
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
        self.logger.debug( f"Stopping poll loop after consuming {nconsumed} messages during {runtime}" )

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
        # self.print_assignments()

