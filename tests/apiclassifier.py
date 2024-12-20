import sys
import io
import time
import pathlib
import argparse
import logging
import fastavro
import datetime

from testmsgconsumer import MsgConsumer
from tom_client import TomClient

_rundir = pathlib.Path( __file__ ).parent

_logger = logging.getLogger( "tests/apiclassifier" )
_logger.propagate = False
if not _logger.hasHandlers():
    _logout = logging.StreamHandler( sys.stderr )
    _logger.addHandler( _logout )
    _formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S' )
    _logout.setFormatter( _formatter )
_logger.setLevel( logging.INFO )


class APIClassifier:
    def __init__( self, brokername, brokerversion, classifiername, classifierparams,
                  tomuser, tompassword, alertschema=None ):
        self.brokername = brokername
        self.brokerversion = brokerversion
        self.classifiername = classifiername
        self.classifierparams = classifierparams
        self.alertschema = alertschema

        self.nclassified = 0
        self.logevery = 100
        self.nextlog = self.logevery

        self.tomclient = TomClient( url="http://tom:8080", username=tomuser, password=tompassword )

    def classify_alerts( self, messages ):
        ingesttime = datetime.datetime.now()
        brokermsgs = []
        for msg in messages:
            alert = fastavro.schemaless_reader( io.BytesIO(msg.value()), self.alertschema )
            brokermsg = { "alertId": alert["alertId"],
                          "diaSourceId": alert["diaSource"]["diaSourceId"],
                          "elasticcPublishTimestamp": msg.timestamp()[1],
                          "brokerIngestTimestamp": ingesttime.timestamp()*1000,
                          "brokerPublishTimestamp": datetime.datetime.now().timestamp()*1000,
                          "brokerName": self.brokername,
                          "brokerVersion": self.brokerversion,
                          "classifierName": self.classifiername,
                          "classifierParams": self.classifierparams,
                          "classifications": [ { "classId": 111, "probability": 0.25 },
                                               { "classId": 112, "probability": 0.75 } ]
                         }
            brokermsgs.append( brokermsg )

        res = self.tomclient.request( "PUT", "elasticc2/brokermessage/", json=brokermsgs )
        if res.status_code != 201:
            sys.stderr.write( f"Error, got status code {res.status_code} from PUT." )


# ======================================================================

def main():
    parser = argparse.ArgumentParser( description="Pretend to be an elasticc API-putting broker",
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "--source", default="brahms.lbl.gov:9092",
                         help="Server to pull ELAsTiCC alerts from" )
    parser.add_argument( "-t", "--source-topics", nargs='+', required=True, help="Topics on source server" )
    parser.add_argument( "-g", "--group-id", default="rknop-test",
                         help="Group ID to use on source server" )
    parser.add_argument( "-r", "--reset", action='store_true', default=False,
                         help="Reset to beginning of source stream?" )
    parser.add_argument( "-a", "--alert-schema", default=f"{_rundir.parent}/alert_schema/elasticc.v0_9_1.alert.avsc",
                         help="File with elasticc alert schema" )
    parser.add_argument( "-b", "--brokermessage-schema",
                         default=f"{_rundir.parent}/alert_schema/elasticc.v0_9_1.brokerClassification.avsc",
                         help="File with broker message alert schema" )
    parser.add_argument( "-u", "--tom-user", default=None )
    parser.add_argument( "-p", "--tom-password", default=None )
    parser.add_argument( "-s", "--stop-after-sleeps", type=int, default=None,
                         help="Stop after this many sleeps polling the alert server (default: keep going 10 years)" )
    args = parser.parse_args()

    if ( args.tom_user is None ) or ( args.tom_password is None ):
        raise RuntimeError( "--tom-user and --tom-password are required" )

    alertschema = fastavro.schema.load_schema( args.alert_schema )
    brokermsgschema = fastavro.schema.load_schema( args.brokermessage_schema )

    cfer = APIClassifier( "apiclassifier", "1.0", "AlwaysTheSame", "0.5 111, 0.75 112",
                          args.tom_user, args.tom_password, alertschema )

    def handle_message_batch( msgs ):
        cfer.classify_alerts( msgs )

    consumer = None
    if args.reset:
        topicstoreset = set( args.source_topics )
    else:
        topicstoreset = set()

    subbed = []
    missing = []

    # TODO: make this a loop so it can suck in new topics, but also make
    # it so that there is a way to tell it to just run once, or a few
    # times.  (Right now that's handled by the stopafternsleeps in
    # cosumer.poll_loop.)

    while len(subbed) == 0:
        if consumer is not None:
            consumer.close()
        consumer = MsgConsumer( args.source, args.group_id, [], args.alert_schema, logger=_logger,
                                consume_nmsgs=100 )
        topics = consumer.topic_list()
        for topic in args.source_topics:
            if topic in topics:
                subbed.append( topic )
            else:
                missing.append( topic )
        if len( subbed ) > 0:
            consumer.subscribe( subbed )
            _logger.info( f"Subscribed to topics {subbed}" )
            if len( missing ) > 0:
                _logger.warning( f"(Didn't see topics {missing}, proceeding anyway." )
        else:
            _logger.warning( f"No topics in {args.source_topics} exist, sleeping 10s and trying again." )
            time.sleep(10)

    if len( topicstoreset ) > 0:
        for topic in subbed:
            if topic in topicstoreset:
                consumer.reset_to_start( topic )
                topicstoreset.remove( topic )

    consumer.poll_loop( handler = handle_message_batch,
                        stopafter=datetime.timedelta(days=3650),
                        stopafternsleeps=args.stop_after_sleeps )

# ======================================================================

if __name__ == "__main__":
    main()

