import sys
import io
import pathlib
import fastavro

_rundrir = pathlib.Path( __file__ ).parent
sys.path.insert( 0, str( _rundir.parent / "db/management/commands" ) )
from _consumekafkamsgs import MsgConsumer

def main():

    
    consumer = MsgConsumer( 'kafka-server:9092', 'probe-alert-stream',
                            '../elasticc2/management/commands/elasticc.v0_9_1.alert.avsc',
                            topics=[ 'alerts-ddf-full' ] )

    for i in range 10:
        msgs = consumer.consume_one_message( timeout=10 )
        alert = fastavro.schemaless_reader( io.BytesIO( msg.value() ), consumer.schema )
        import pdb; pdb.set_trace()
        pass

# ----------------------------------------------------------------------

if __name__ == "__main__":
    main()
    
