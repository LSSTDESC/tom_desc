import sys
import logging
import datetime
import pathlib
import time

import pandas
import psycopg2
import psycopg2.extras

import django.db
from django.db import transaction
from django.core.management.base import BaseCommand, CommandError
from db.models import QueryQueue

class Command(BaseCommand):
    help = 'Run long database queries'

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )

        self.outdir = pathlib.Path( "/query_results" )
        self.sleeptime = 60
        
        self.logger = logging.getLogger( "long_query_runner" )
        _logout = logging.StreamHandler( sys.stderr )
        self.logger.addHandler( _logout )
        _formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S' )
        _logout.setFormatter( _formatter )
        self.logger.setLevel( logging.INFO )

    
    def prune_old_query_results( self ):
        "Delete query results that are more than a week old."
        # TODO
        pass
    
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
            cursor.execute( "SELECT * FROM db_queryqueue WHERE started IS NULL" )
            rows = cursor.fetchall()
            if len(rows) == 0:
                return None

            self.logger.info( "Claming query request {rows[0]['queryid']}" )
            cursor.execute( "UPDATE db_queryqueue SET started=%(t)s WHERE queryid=%(id)s",
                            { 't': datetime.datetime.now(), 'id': rows[0]['queryid'] } )
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
        origautocommit = None
        conn = None
        try:
            # Want to get the actual psycopg2 connection.
            # so we can turn off autocommit and lock tables
            gratuitous = django.db.connection.cursor()
            conn = gratuitous.connection
            origautocommit = conn.autocommit
            conn.autocommit = False
            cursor = conn.cursor()

            self.logger.info( f"Starting query request {queryinfo['queryid']}" )
            
            for query, subdict in zip( queryinfo['queries'], queryinfo['subdicts'] ):
                try:
                    self.logger.info( "For query request {queryinfo['queryid']}, running query: "
                                      f"{cursor.mogrify(query,subdict)}" )
                    cursor.execute( query, subdict )
                except Exception as e:
                    conn.rollback()
                    cursor.execute( "UPDATE db_queryqueue SET error=TRUE, finished=%(t)s, errortext=%(err)s "
                                    "WHERE queryid=%(id)s",
                                    { 't': datetime.datetime.now(), 'err': str(e), 'id': queryinfo['queryid'] } )
                    conn.commit()
                    return False

            columns = [ d.name for d in cursor.description ]
            rows = cursor.fetchall()

            if ( queryinfo['format'] == 'csv' ) or ( queryinfo['format'] == 'pandas' ):
                df = pandas.DataFrame( rows, columns=columns )
                if queryinfo['format'] == 'pandas':
                    df.to_pickle( self.outdir / str(queryinfo['queryid']) )
                else:
                    df.to_csv( self.outdir / str(queryinfo['queryid']) )

            elif ( queryinfo['format'] == 'numpy' ):
                raise NotImplementedError( "numpy return format isn't implemented yet" )

            cursor.execute( "UPDATE db_queryqueue SET finished=%(t)s WHERE queryid=%(id)s",
                            { 't': datetime.datetime.now(), 'id': queryinfo['queryid'] } )
            conn.commit()
            return True

        except Exception as ex:
            conn.rollback()
            cursor = conn.cursor()
            cursor.execute( "UPDATE db_queryqueue SET error=TRUE, finished=%(t)s, errortext=%(err)s "
                            "WHERE queryid=%(id)s",
                            { 't': datetime.datetime.now(), 'err': str(ex), 'id': queryinfo['queryid'] } )
            conn.commit()
            return False

        finally:
            if conn is not None:
                conn.rollback()
                if origautocommit is not None:
                    conn.autocommit = origautocommit
                    
                    
    def add_arguments( self, parser ):
        parser.add_argument( '-o', '--once', default=False, action='store_true',
                             help="Just run at most one query" )
        parser.add_argument( '-l', '--loop', default=False, action='store_true',
                             help="Run the check/run query loop" )


    def handle( self, *args, **options ):
        if options['once']:
            queryinfo = self.get_queued_query()
            if queryinfo is None:
                self.logger.info( "No queries to run." )
            else:
                self.logger.info( f"Running query {queryinfo['queryid']}" )
                self.run_query( queryinfo )

            return

        if not options['loop']:
            self.logger.error( "Must specify either -o/--once or -l/--loop" )
                
        while True:
            self.prune_old_query_results()

            queryinfo = self.get_queued_query()
            if queryinfo is None:
                time.sleep( self.sleeptime )
                continue

            self.run_query( queryinfo )
                
        
