import sys
import io
import pathlib
import fastavro

_rundir = pathlib.Path( __file__ ).parent
sys.path.insert( 0, str( _rundir.parent / "db/management/commands" ) )
from _consumekafkamsgs import MsgConsumer

def main():

    
    consumer = MsgConsumer( 'kafka-server:9092', 'probe-alert-stream-4',
                            '../elasticc2/management/commands/elasticc.v0_9_1.alert.avsc',
                            topics=[ 'alerts-ddf-limited' ] )

    def msghandler( msgs ):
        alert = fastavro.schemaless_reader( io.BytesIO( msgs[0].value() ), consumer.schema )
        print( f'{len(alert["prvDiaSources"])} previous sources, '
               f'{len(alert["prvDiaForcedSources"])} previous forced sources' )
        if len( alert['prvDiaForcedSources'] ) > 0 :
            dt = alert['diaSource']['midPointTai'] - alert['prvDiaForcedSources'][0]['midPointTai']
            print( f'   First diaForcedSource is {dt} days back' )
        
    
    for i in range(10):
        consumer.consume_one_message( timeout=10, handler=msghandler )

# ----------------------------------------------------------------------

if __name__ == "__main__":
    main()
    
