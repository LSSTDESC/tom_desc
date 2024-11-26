import sys
import io
import math
import pathlib
import logging
import argparse
import time
import datetime
import random
import confluent_kafka
import fastavro

sys.stderr.write( "FAKEBROKER importing testmsgconsumer\n" )
from testmsgconsumer import MsgConsumer

_rundir = pathlib.Path( __file__ ).parent

_logger = logging.getLogger( "fakebroker" )
_logger.propagate = False
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - fakebroker - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.INFO )

# ======================================================================

class Classifier:
    def __init__( self, brokername, brokerversion, classifiername, classifierparams,
                  kafkaserver="brahms.lbl.gov:9092", topic="somebody-didnt-replace-a-default",
                  alertschema=None, brokermessageschema=None ):
        self.brokername = brokername
        self.brokerversion = brokerversion
        self.classifiername = classifiername
        self.classifierparams = classifierparams
        self.kafkaserver = kafkaserver
        self.topic = topic
        self.alertschema = alertschema
        self.brokermessageschema = brokermessageschema

        self.nclassified = 0
        self.logevery = 10
        self.nextlog = self.logevery

    def determine_types_and_probabilities( self, alert ):
        """Given an alert (a dict in the format of the elasticc alert schema), return a list of
        two-element tuples that is (classId, probability)."""
        raise RuntimeError( "Need to implement this function in a subclass!" )

    def classify_alerts( self, messages ):
        producer = confluent_kafka.Producer( { 'bootstrap.servers': self.kafkaserver,
                                               'batch.size': 131072,
                                               'linger.ms': 50 } )
        for msg in messages:
            alert = fastavro.schemaless_reader( io.BytesIO(msg.value()), self.alertschema )
            probs = self.determine_types_and_probabilities( alert )
            brokermsg = { "alertId": alert["alertId"],
                          "diaSourceId": alert["diaSource"]["diaSourceId"],
                          "elasticcPublishTimestamp": msg.timestamp()[1],
                          "brokerIngestTimestamp": datetime.datetime.now(),
                          "brokerName": self.brokername,
                          "brokerVersion": self.brokerversion,
                          "classifierName": self.classifiername,
                          "classifierParams": self.classifierparams,
                          "classifications": []
                         }
            for prob in probs:
                brokermsg['classifications'].append( { "classId": prob[0],
                                                       "probability": prob[1] } )
            outdata = io.BytesIO()
            fastavro.write.schemaless_writer( outdata, self.brokermessageschema, brokermsg )
            producer.produce( self.topic, outdata.getvalue() )
        producer.flush()

        self.nclassified += len(messages)
        if ( self.nclassified > self.nextlog ):
            _logger.info( f"{self.classifiername} has classified {self.nclassified} alerts" )
            self.nextlog = self.logevery * ( math.floor( self.nclassified / self.logevery ) + 1 )

# ======================================================================

class NugentClassifier(Classifier):
    def __init__( self, *args, **kwargs ):
        super().__init__( "FakeBroker", "v1.0", "NugentClassifier", "100%", **kwargs )

    def determine_types_and_probabilities( self, alert ):
        return [ ( 2222, 1.0 ) ]

# ======================================================================

class RandomSNType(Classifier):
    def __init__( self, *args, **kwargs ):
        super().__init__( "FakeBroker", "v1.0", "RandomSNType", "Perfect", **kwargs )
        random.seed()

    def determine_types_and_probabilities( self, alert ):
        totprob = 0.
        types = [ 2222, 2223, 2224, 2225, 2226,
                  2232, 2233, 2234, 2235,
                  2243, 2244, 2245, 2246,
                  2322, 2323, 2324, 2325, 2326,
                  2332 ]
        retval = []
        random.shuffle( types )
        for sntype in types:
            thisprob = random.random() * ( 1 - totprob )
            totprob += thisprob
            retval.append( ( sntype, thisprob ) )
        # SLSN seems to be the default type....
        retval.append( ( 2242, 1-totprob ) )
        return retval

# ======================================================================

class FakeBroker:
    def __init__( self,
                  source,
                  source_topics,
                  dest,
                  dest_topic,
                  group_id="rknop-test",
                  alert_schema=f"{_rundir}/schema/elasticc.v0_9_1.alert.avsc",
                  brokermessage_schema=f"{_rundir}/schema/elasticc.v0_9_1.brokerClassification.avsc",
                  reset=False ):
        self.source = source
        self.source_topics = source_topics
        self.dest = dest
        self.dest_topic = dest_topic
        self.group_id = group_id
        self.reset = reset

        self.alert_schema = alert_schema
        alertschemaobj = fastavro.schema.load_schema( alert_schema )
        brokermsgschema = fastavro.schema.load_schema( brokermessage_schema )
        self.classifiers = [ NugentClassifier( kafkaserver=self.dest, topic=self.dest_topic,
                                               alertschema=alertschemaobj, brokermessageschema=brokermsgschema ),
                             RandomSNType(  kafkaserver=self.dest, topic=self.dest_topic,
                                            alertschema=alertschemaobj, brokermessageschema=brokermsgschema )
                            ]

    def handle_message_batch( self, msgs ):
        for cfer in self.classifiers:
            cfer.classify_alerts( msgs )

    def __call__( self ):
        if self.reset:
            topicstoreset = set( self.source_topics )
        else:
            topicstoreset = set()
        consumer = None
        while True:
            subbed = []
            if consumer is not None:
                consumer.close()
            consumer = MsgConsumer( self.source, self.group_id, [], self.alert_schema, logger=_logger,
                                    consume_nmsgs=100 )
            # Wait for the topic to exist, and only then subscribe
            while len(subbed) == 0:
                topics = consumer.topic_list()
                _logger.debug( f"Topics seen on server: {topics}" )
                for topic in self.source_topics:
                    if topic in topics:
                        subbed.append( topic )
                if len(subbed) > 0:
                    _logger.debug( f"Subscribing to topics {subbed}" )
                    if len(subbed) < len( self.source_topics ):
                        missing = [ i for i in self.source_topics if i not in subbed ]
                        _logger.debug( f"(Didn't see topics: {missing})" )
                    consumer.subscribe( subbed )
                else:
                    _logger.warning( f"No topics in {self.source_topics} exists, sleeping 10s and trying again." )
                    time.sleep( 10 )

                if len(topicstoreset) > 0:
                    for topic in subbed:
                        if topic in topicstoreset:
                            consumer.reset_to_start( topic )
                            topicstoreset.remove( topic )

            consumer.poll_loop( handler=self.handle_message_batch,
                                stoponnomessages=(len(subbed)<len(self.source_topics)),
                                stopafter=datetime.timedelta(minutes=10) )


# ======================================================================

def main():
    parser = argparse.ArgumentParser( description="Pretend to be an elasticc broker",
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "--source", default="brahms.lbl.gov:9092",
                         help="Server to pull ELAsTiCC alerts from" )
    parser.add_argument( "-t", "--source-topics", nargs='+', required=True, help="Topics on source server" )
    parser.add_argument( "-g", "--group-id", default="rknop-test",
                         help="Group ID to use on source server" )
    parser.add_argument( "-r", "--reset", action='store_true', default=False,
                         help="Reset to beginning of source stream?" )
    parser.add_argument( "--dest", default="brahms.lbl.gov:9092",
                         help="Server to push broker message alerts to" )
    parser.add_argument( "-u", "--dest-topic", required=True, help="Topic on dest server" )
    parser.add_argument( "-s", "--alert-schema", default=f"{_rundir.parent}/alert_schema/elasticc.v0_9_1.alert.avsc",
                         help="File with elasticc alert schema" )
    parser.add_argument( "-b", "--brokermessage-schema",
                         default=f"{_rundir.parent}/alert_schema/elasticc.v0_9_1.brokerClassification.avsc",
                         help="File with broker message alert schema" )

    args = parser.parse_args()

    broker = FakeBroker( args.source, args.source_topics, args.dest, args.dest_topic,
                         group_id=args.group_id, alert_schema=args.alert_schema,
                         brokermessage_schema=args.brokermessage_schema, reset=args.reset )
    broker()


# ======================================================================

if __name__ == "__main__":
    main()
