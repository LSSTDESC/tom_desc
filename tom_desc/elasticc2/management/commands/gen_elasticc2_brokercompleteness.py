import sys
import io
import re
import math
import copy
import pathlib
import traceback
import time
import datetime
import dateutil.parser
import dateutil.tz
import pytz
import logging
import threading
import numpy
import pandas
import psycopg2
import psycopg2.extras
import django.db
from matplotlib import pyplot
from django.core.management.base import BaseCommand, CommandError
import cassandra.query

_rundir = pathlib.Path(__file__).parent

_logger = logging.getLogger( __name__ )
_logger.propagate = False
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.DEBUG )

class Command(BaseCommand):
    help = 'Generate broker time delay graphs'
    outdir = _rundir / "../../static/elasticc2/brokertiminggraphs"

    def add_arguments( self, parser ):
        parser.add_argument( '--start-day', default='2023-10-16',
                             help="YYYY-MM-DD of first day to consider (default 2023-10-16)" )
        parser.add_argument( '--end-day', default='2023-10-18',
                             help="YYYY-MM-DD of last day to consider (default 2023-10-18)" )
        parser.add_argument( '--dt', default=1, type=float,
                             help="Bargraph step in days (default 1)" )
    
    def handle( self, *args, **options ):
        _logger.info( "Starting gen_elasticc2_brokercompleteness" )

        conn = None
        # Jump through hoops to get access to the psycopg2 connection from django
        conn = django.db.connection.cursor().connection
        orig_autocommit = conn.autocommit
        cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )

        try:
            just_read_pickle = False
            updatetime = None
            
            if not just_read_pickle:

                updatetime = datetime.datetime.utcnow().date().isoformat()

                # Figure out classifiers

                cursor.execute( "SELECT * FROM elasticc2_brokerclassifier "
                                "ORDER BY brokername, brokerverison, classifiername, classifierparams" )
                cfers = { r['classifier_id']: dict(r) for r in cursor.fetchall() }
                
                casssession = django.db.connections['cassandra'].connection.session
                casssession.default_fetch_size = 10000

                t0 = datetime.datetime.fromisoformat( options['start_day'] )
                t1 = datetime.datetime.fromisoformat( options['start_day'] )
                dt = datetime.timedelta( days=options['dt'] )
                
                for cferid, cfer in cfers.items():
                    t = t0
                    while t + dt < t1:

                        cassq = ( "SELECT * FROM tom_desc.cass_broker_message_by_time "
                                  "WHERE classifier_id IN %(id)s "
                                  "  AND descingesttimestamp >= %(t0)s "
                                  "  AND descingesttimestamp < %(t1)s "
                                  "ALLOW FILTERING" )
                        future = casssession.execute_async( cassq, { 'id': tuple( brokergroups[broker] ),
                                                                         't0': week,
                                                                         't1': week+dt } )
                            handler = CassBrokerMessagePageHandler( future, cursor,
                                                                    self.bucketleft, self.bucketright, self.dbucket,
                                                                    lowcutoff=self.lowcutoff,
                                                                    highcutoff=self.highcutoff )
                            _logger.info( "...waiting for finished..." )
                            handler.finished_event.wait()
                            _logger.info( "...done." )
                            handler.finalize()
                            if handler.error:
                                _logger.error( handler.error )
                                raise handler.error

                            if len( handler.df ) > 0:
                                df = handler.df.reset_index()
                                df[ 'broker' ] = broker
                                df[ 'week' ] = weeklab
                                df.set_index( [ 'broker', 'week', 'which', 'buck' ], inplace=True )
                                self.df = self.df.add( df, fill_value=0 )


                # It seems like there should be a more elegant way to do this
                summeddf = ( self.df.query( 'week!="cumlative"' )
                             .groupby( [ 'broker', 'which', 'buck' ] )
                             .sum().reset_index() )
                summeddf['week'] = 'cumulative'
                summeddf.set_index( [ 'broker', 'week', 'which', 'buck' ], inplace=True )
                self.df.loc[ ( slice(None), 'cumulative', slice(None), slice(None) ), : ] = summeddf

                # self.df.set_index( [ 'broker', 'week', 'which', 'buck' ], inplace=True )
                _logger.info( "Writing gen_elasticc2_brokerdelaygraphs.pkl" )
                self.df.to_pickle( "gen_elasticc2_brokerdelaygraphs.pkl" )
            else:
                _logger.info( "Reading gen_elasticc2_brokerdelaygraphs.pkl" )
                self.df = pandas.read_pickle( "gen_elasticc2_brokerdelaygraphs.pkl" )

            _logger.info( "Saving plots." )
            self.makeplots()
            if updatetime is not None:
                with open( self.outdir / "updatetime.txt", 'w' ) as ofp:
                    ofp.write( updatetime )
            _logger.info( "All done." )
        except Exception as e:
            _logger.exception( e )
            _logger.exception( traceback.format_exc() )
            # import pdb; pdb.set_trace()
            raise e
        finally:
            if conn is not None:
                conn.autocommit = orig_autocommit
                conn.close()
                conn = None

