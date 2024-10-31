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
import argparse
import urllib



class Broker(object):

    def __init__( self, username=None, password=None, *args, **options ):

        self.logger = logging.getLogger( "brokerpoll_baselogger" )
        self.logger.propagate = False
        logout = logging.FileHandler( _logdir / f"logs/brokerpoll.log" )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - brokerpoll - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        self.logger.setLevel( logging.DEBUG )
        if options['reset']:
            self.reset = options['reset']

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
        consumer = brokerclass( pipe=pipe, **options )
        consumer.poll()

    def broker_poll( self, *args, **options):
        self.logger.info( "******** brokerpoll starting ***********" )

        self.mustdie = False
        signal.signal( signal.SIGTERM, lambda sig, stack: self.sigterm( "TERM" ) )
        signal.signal( signal.SIGINT, lambda sig, stack: self.sigterm( "INT" ) )


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
            print( "Must give at least one broker to listen to." )

        brokers = {}

        # Launch a process for each broker that will poll that broker indefinitely

        for name,brokerclass in brokerstodo.items():
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
    logout = logging.FileHandler( _logdir / f"logs/brokerpoll.log" )
    logger.addHandler( logout )
    formatter = logging.Formatter( f'[%(asctime)s - brokerpoll - %(levelname)s] - %(message)s',datefmt='%Y-%m-%d %H:%M:%S' )
    logout.setFormatter( formatter )
    logger.setLevel( logging.DEBUG )


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

    options = vars(parser.parse_args())

    broker = Broker(**options)
    
    poll = broker.broker_poll(**options)
