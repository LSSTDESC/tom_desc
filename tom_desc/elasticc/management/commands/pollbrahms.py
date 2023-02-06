raise RuntimeError( "Broken" )

import sys
import io
import pathlib
import datetime
import json
import fastavro
from django.core.management.base import BaseCommand, CommandError
from elasticc.models import BrokerMessage

_rundir = pathlib.Path(__file__).parent
sys.path.insert(0, str(_rundir) )
from _consumekafkamsgs import MsgConsumer

class DTJason(json.JSONEncoder):
    def default( self, obj ):
        if isinstance( obj, datetime.datetime ):
            return obj.isoformat()
        else:
            return json.JSONDecoder.default( self, obj )

class Command(BaseCommand):
    help = 'Poll brahms.lbl.gov for BrokerMessages'
    schemafile = _rundir / "elasticc.v0_9.brokerClassification.avsc"

    def add_arguments( self, parser ):
        parser.add_argument( '-t', '--topic', required=True, help="Topic to poll" )
        parser.add_argument( '-g', '--groupid', default='rob_elasticc-test-7', help="Group ID to use" )
        parser.add_argument( '-s', '--server', default='brahms.lbl.gov:9092', help="Kafka server" )

    def handle( self, *args, **options ):
        self.schema = fastavro.schema.load_schema( self.schemafile )
        consumer = MsgConsumer( options['server'], options['groupid'], options['topic'], self.schemafile )
        consumer.poll_loop( handler=self.handle_msgs, max_consumed=10, max_runtime=datetime.timedelta(seconds=15) )
        
    def handle_msgs( self, msgs ):
        batch = []
        for msg in msgs:
            alert = fastavro.schemaless_reader( io.BytesIO(msg.value()), self.schema )
            batch.append( { 'topic': msg.topic(),
                            'msgoffset': msg.offset(),
                            'msg': alert } )
        BrokerMessage.load_batch( batch )

    def echo_msgs( self, msgs ):
        sys.stderr.write( f'Handling {len(msgs)} messages' )
        for msg in msgs:
            ofp = io.StringIO( f"Topic: {msg.topic()} ; Partition: {msg.partition()} ; "
                               f"Offset: {msg.offset()} ; Key: {msg.key()}\n" )
            ofp.write( json.dumps( alert, indent=4, sort_keys=True, cls=DTJason ) )
            ofp.write( "\n" )
            sys.stderr.write( ofp.getvalue() )
            ofp.close()
        
