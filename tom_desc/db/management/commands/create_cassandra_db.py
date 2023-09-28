import sys
import os
import django.db
import django_cassandra_engine

class Command(BaseCommand):
    help = 'Create Cassandra database and keyspace'

    def handle( self, *args, **options ):
        cursor = django.db.connections['cassandra'].cursor()
        cursor.execute( 

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )

        # Make sure the log directory exists

        logdir = _rundir.parent.parent.parent / "logs"
        if logdir.exists():
            if not logdir.is_dir():
                raise RuntimeError( "{logdir} exists but is not a directory!" )
        else:
            logdir.mkdir( parents=True )

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
        parser.add_argument( '--do-pitt', action='store_true', default=False, help="Poll from PITT-Google" )
        parser.add_argument( '--pitt-topic', default=None, help="Topic name for PITT-Google" )
        parser.add_argument( '--pitt-project', default=None, help="Project name for PITT-Google" )
        parser.add_argument( '--do-test', action='store_true', default=False,
                             help="Poll from kafka-server:9092 (for testing purposes)" )
        parser.add_argument( '---test-topic', default='classifications',
                             help="Topic to poll from on kafka-server:9092" )
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
        if options['do_pitt']:
            brokerstodo['pitt'] = PittGoogleBroker
        if options['do_brahms']:
            brokerstodo['brahms'] = BrahmsConsumer
        if options['do_test']:
            brokerstodo['test'] = TestConsumer
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
