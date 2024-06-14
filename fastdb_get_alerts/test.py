import argparse


class Broker(object):

    def __init__( self, username=None, password=None, *args, **options ):
        print(*options)
        if options['reset']:
            self.reset = options['reset']
        self.username = username
        self.password = password

    def broker_poll(self, *args, **options):


        print( "******** brokerpoll starting ***********" )


        brokerstodo = {}
        if options['do_alerce']:
            brokerstodo['alerce'] = AlerceConsumer
        if options['do_antares']:
            brokerstodo['antares'] = AntaresConsumer
        if options['do_fink']:
            brokerstodo['fink'] = FinkConsumer
        if options['do_pitt']:
            brokerstodo['pitt'] = PittGoogleBroker
        if options['do_brahms']:
            brokerstodo['brahms'] = BrahmsConsumer
        if options['do_test']:
            brokerstodo['test'] = 'TestConsumer'
        if len( brokerstodo ) == 0:
            print( "Must give at least one broker to listen to." )

        print(options['test_topic'])

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()



    parser.add_argument( '--do-alerce', action='store_true', default=False, help="Poll from ALeRCE" )
    parser.add_argument( '--alerce-topic-pattern', default='^lc_classifier_.*_(\d{4}\d{2}\d{2})$',
                         help='Regex for matching ALeRCE topics (warning: custom code, see AlerceBroker)' )
    parser.add_argument( '--do-antares', action='store_true', default=False, help="Poll from ANTARES" )
    parser.add_argument( '--antares-topic', default=None, help='Topic name for Antares' )
    parser.add_argument( '--do-fink', action='store_true', default=False, help="Poll from Fink" )
    parser.add_argument( '--fink-topic', default=None, help='Topic name for Fink' )
    parser.add_argument( '--do-brahms', action='store_true', default=False,
                         help="Poll from Rob's test kafka server" )
    parser.add_argument( '--brahms-topic', default=None,
                         help="Topic to poll on brahms (required if --do-brahms is True)" )
    parser.add_argument( '--do-pitt', action='store_true', default=False, help="Poll from PITT-Google" )
    parser.add_argument( '--pitt-topic', default=None, help="Topic name for PITT-Google" )
    parser.add_argument( '--pitt-project', default=None, help="Project name for PITT-Google" )
    parser.add_argument( '--do-test', action='store_true', default=False,
                         help="Poll from kafka-server:9092 (for testing purposes)" )
    parser.add_argument( '--test-topic', default='classifications',
                         help="Topic to poll from on kafka-server:9092" )
    parser.add_argument( '-g', '--grouptag', default=None, help="Tag to add to end of kafka group ids" )
    parser.add_argument('-r', '--reset', action='store_true', default=False, help='Reset all stream pointers')

    options =vars( parser.parse_args())
    print(options)

    broker = Broker(**options)
    
    poll = broker.broker_poll(**options)
