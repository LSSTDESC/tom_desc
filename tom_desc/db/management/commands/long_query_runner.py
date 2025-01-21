import sys
import logging
import datetime
import pathlib
import time
import json
import multiprocessing

import pandas
import psycopg2
import psycopg2.extras

import django.db
import django.conf
from django.db import transaction
from django.core.management.base import BaseCommand, CommandError
from db.models import QueryQueue

class Command(BaseCommand):
    help = 'Run long database queries'

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )

        self.outdir = pathlib.Path( "/query_results" )
        self.sleeptime = 10

        self.logger = logging.getLogger( "long_query_runner" )
        _logout = logging.StreamHandler( sys.stderr )
        self.logger.addHandler( _logout )
        _formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S' )
        _logout.setFormatter( _formatter )
        self.logger.propagate = False
        self.logger.setLevel( logging.INFO )
        # self.logger.setLevel( logging.DEBUG )


    def prune_old_query_results( self, days=7 ):
        "Delete query results that are more than days old."

        since = datetime.datetime.now( tz=datetime.timezone.utc ) - datetime.timedelta( days=days )
        qs = QueryQueue.objects.filter( finished__lt=since )
        for q in qs:
            self.logger.info( f"Pruning query {q.queryid}" )
            outf = self.outdir / str(q.queryid)
            if outf.is_file():
                outf.unlink()
            q.delete()


    def get_queued_query( self ):
        origautocommit = None
        conn = None
        try:
            # Want to get the actual psycopg2 connection.
            # so we can turn off autocommit and lock tables
            gratuitous = django.db.connection.cursor()
            conn = gratuitous.connection
            origautocommit = conn.autocommit
            conn.autocommit = False

            cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )
            cursor.execute( "LOCK TABLE db_queryqueue" )
            cursor.execute( "SELECT * FROM db_queryqueue WHERE started IS NULL ORDER BY submitted" )
            rows = cursor.fetchall()
            if len(rows) == 0:
                return None

            self.logger.info( f"Claiming query request {rows[0]['queryid']}" )
            cursor.execute( "UPDATE db_queryqueue SET started=%(t)s WHERE queryid=%(id)s",
                            { 't': datetime.datetime.now( tz=datetime.timezone.utc ),
                              'id': rows[0]['queryid'] } )
            conn.commit()

            cursor.execute( "SELECT * FROM db_queryqueue WHERE queryid=%(id)s", { 'id': rows[0]['queryid'] } )
            rows = cursor.fetchall()
            return dict( rows[0] )

        finally:
            if conn is not None:
                conn.rollback()
                if origautocommit is not None:
                    conn.autocommit = origautocommit


    def run_query( self, queryinfo ):
        conn = None
        qentry = None
        try:
            qentry = QueryQueue.objects.filter( queryid=queryinfo['queryid'] )
            if len(qentry) == 0:
                raise RuntimeError( f"Error, no queue entry with id {queryinfo['queryid']}" )
            if len(qentry) > 1:
                raise RuntimeError( f"Error, {len(qentry)} queue entries with id {queryinfo['queryid']}" )
            qentry = qentry[0]

            # Make a separate connection to the database to run
            # these queries.  We need a psycopg2 connection anyway,
            # which django yields only grudgingly, but more
            # importantly, we want to connect with the postgres_ro
            # user, which is not what django uses.

            with open( "/secrets/postgres_ro_password" ) as ifp:
                pgpasswd = ifp.readline().strip()
            if django.conf.settings.DATABASES['default']['ENGINE'] != 'psqlextra.backend':
                raise RuntimeError( f"Unexpected database backend "
                                    f"{django.conf.settings.DATABASES['default']['ENGINE']}, "
                                    f"expected psqlextra.backend" )
            conn = psycopg2.connect( host=django.conf.settings.DATABASES['default']['HOST'],
                                     port=django.conf.settings.DATABASES['default']['PORT'],
                                     dbname=django.conf.settings.DATABASES['default']['NAME'],
                                     user='postgres_ro', password=pgpasswd )
            conn.autocommit = False
            cursor = conn.cursor()

            self.logger.info( f"Starting query request {queryinfo['queryid']}" )

            for query, subdict_text in zip( queryinfo['queries'], queryinfo['subdicts'] ):
                try:
                    # Have to convert lists to tuples in the substitution dictionaries
                    subdict = json.loads( subdict_text )
                    if not isinstance( subdict, dict ):
                        raise TypeError( f"For query {queryinfo['queryid']}, query {query}, "
                                         f"subdict is a {type(subdict)}, not a dict." )
                    for key in subdict.keys():
                        if isinstance( subdict[key], list ):
                            subdict[key] = tuple( subdict[key] )
                    self.logger.debug( f"For query request {queryinfo['queryid']}, running query: "
                                      f"{cursor.mogrify(query,subdict)}" )
                    cursor.execute( query, subdict )
                except Exception as e:
                    self.logger.exception( f"Exception running query: {str(e)}" )
                    conn.rollback()
                    conn = None
                    qentry.finished = datetime.datetime.now( tz=datetime.timezone.utc )
                    qentry.error = True
                    qentry.errortext = str(e)
                    qentry.save()
                    return False

            self.logger.info( "Done running queries, fetching" )
            columns = [ d.name for d in cursor.description ]
            rows = cursor.fetchall()

            self.logger.info( "Done fetching, saving" )
            if ( queryinfo['format'] == 'csv' ) or ( queryinfo['format'] == 'pandas' ):
                df = pandas.DataFrame( rows, columns=columns )
                if queryinfo['format'] == 'pandas':
                    df.to_pickle( self.outdir / str(queryinfo['queryid']) )
                else:
                    df.to_csv( self.outdir / str(queryinfo['queryid']) )

            elif ( queryinfo['format'] == 'numpy' ):
                raise NotImplementedError( "numpy return format isn't implemented yet" )

            self.logger.info( "Done saving, marking finished" )
            conn = None
            qentry.finished = datetime.datetime.now( tz=datetime.timezone.utc )
            qentry.save()

            self.logger.info( "All done." )
            return True

        except Exception as ex:
            self.logger.exception( ex )
            if conn is not None:
                conn.rollback()
                cursor = conn.cursor()
                conn = None
                if qentry is not None:
                    qentry.finished = datetime.datetime.now( tz=datetime.timezone.utc )
                    qentry.error = True
                    qentry.errortext = str(ex)
                    qentry.save()
            return False

        finally:
            if conn is not None:
                conn.rollback()
                conn.close()

    def add_arguments( self, parser ):
        parser.add_argument( '-o', '--once', default=False, action='store_true',
                             help="Just run at most one query" )
        parser.add_argument( '-l', '--loop', default=False, action='store_true',
                             help="Run the check/run query loop" )
        parser.add_argument( '-s', '--sleep-time', default=10, type=int,
                             help=( "How many seconds to sleep between looking to see if there are queries to do "
                                    "(default 10)" ) )
        parser.add_argument( '-n', '--num-runners', default=10, type=int,
                             help=( "How many queries to run simutalenously (default 10)" ) )
        parser.add_argument( '-p', '--prune', default=None, type=float,
                             help=( "Prune queries older than this many days.  It probably doesn't "
                                    "make sense to use this with --loop" ) )


    def query_loop( self, sleeptime ):
        while True:
            while True:
                queryinfo = self.get_queued_query()
                if queryinfo is None:
                    time.sleep( self.sleeptime )
                    continue

                self.run_query( queryinfo )


    def handle( self, *args, **options ):
        if options['prune'] is not None:
            self.prune_old_query_results( options['prune'] )

        if options['once']:
            if options['loop']:
                self.logger.warning( "Both --once and --loop given, only running one query (ignoring --loop)." )
            queryinfo = self.get_queued_query()
            if queryinfo is None:
                self.logger.info( "No queries to run." )
            else:
                self.run_query( queryinfo )

            return

        if options['loop']:
            if ( options['num_runners'] < 1 ) or ( options['num_runners'] > 20 ):
                raise ValueError( "num-runners must be >=1 and <= 20." )
            if options['num_runners'] == 1:
                self.logger.info( f"Starting infinite loop to look for and run queries." )
                self.query_loop( options['sleep-time'] )
            else:
                self.logger.info( f"Starting {options['num_runners']} processes to go into infinite loops "
                                  f"looking for and running queries." )
                pool = multiprocessing.Pool( options['num_runners'] )
                for i in range( options['num_runners'] ):
                    pool.apply_async( query_loop, [ options['sleep-time'] ] )
                pool.close()
                pool.join()




