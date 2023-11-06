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
    help = 'Generate broker completeness graphs'
    outdir = ( _rundir / "../../static/elasticc2/brokercompleteness" ).resolve()


    def add_arguments( self, parser ):
        parser.add_argument( "--t0", default="2023-10-16",
                             help="First UTC day to look at (YYYY-MM-DD) (default: 2023-10-16)" )
        parser.add_argument( "--t1", default="2023-10-19",
                             help="Last UTC day to look at (YYYY-MM-DD) (default: 2023-10-19)" )
        parser.add_argument( "--dt", default=7, type=int, help="Step in days between subdivided graphs (default: 7)" )
        parser.add_argument( "--bardt", default=24, type=int, help="Step in hours between bars( default: 24)" )
        parser.add_argument( "--wipe", default=False, action='store_true',
                             help="Wipe out output directory before starting." )
        parser.add_argument( "--explain", default=False, action='store_true', help="Show EXPLAIN ANALYZE on queries" )

    def handle( self, *args, **options ):
        _logger.info( "Starting gen_elasticc2_brokercompleteness" )

        if not self.outdir.is_dir():
            if self.outdir.exists():
                raise FileExistsError( f"{self.outdir} exists but is not a directory!" )
            self.outdir.mkdir( parents=True, exist_ok=True )

        if options['wipe']:
            _logger.info( f"Wiping out {self.outdir}" )
            for f in self.outdir.glob( '*.svg' ):
                f.unlink()
            f = self.outdir / "updatetime.txt"
            if f.exists():
                f.unlink()

        grapht0 = pytz.utc.localize( datetime.datetime.fromisoformat( options['t0'] ) )
        grapht1 = pytz.utc.localize( datetime.datetime.fromisoformat( options['t1'] ) )
        graphdt = datetime.timedelta( days=options['dt'] )

        conn = None
        # Jump through hoops to get access to the psycopg2 connection from django
        conn = django.db.connection.cursor().connection
        orig_autocommit = conn.autocommit
        cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )

        try:
            updatetime = datetime.datetime.utcnow().date().isoformat()
            with open( self.outdir / "updatetime.txt", "w" ) as ofp:
                ofp.write( updatetime )

            # Figure out classifiers

            cursor.execute( "SELECT * FROM elasticc2_brokerclassifier "
                            "ORDER BY brokername, brokerversion, classifiername, classifierparams" )
            cfers = { r['classifier_id']: dict(r) for r in cursor.fetchall() }

            for cferid, cfer in cfers.items():
                _logger.info( f"Starting classifier {cferid} : {cfer['brokername']} {cfer['brokerversion']} "
                              f"{cfer['classifiername']} {cfer['classifierparams']}" )
                grapht = grapht0
                while grapht < grapht1:
                    try:
                        timelab = f"[{grapht.date().isoformat()} , {(grapht+graphdt).date().isoformat()})"
                        _logger.info( f"Starting {cferid} {timelab}" )
                        q = ( f"SELECT EXTRACT(EPOCH FROM a.alertsenttimestamp)::float as alerttime, "
                              f"       COUNT(b.brokermessage_id) AS n "
                              f"FROM elasticc2_ppdbalert a "
                              f"LEFT JOIN elasticc2_brokermessage b ON a.alert_id=b.alert_id "
                              f"                                    AND b.classifier_id=%(cferid)s "
                              f"WHERE a.alertsenttimestamp IS NOT NULL "
                              f"  AND a.alertsenttimestamp >= %(t0)s AND a.alertsenttimestamp < %(t1)s "
                              f"GROUP BY a.alertsenttimestamp" )

                        if options['explain']:
                            cursor.execute( f'EXPLAIN ANALYZE {q}',
                                            { 'cferid': cferid, 't0': grapht, 't1': grapht+graphdt } )
                            rows = cursor.fetchall()
                            strio = io.StringIO()
                            dictargs = { 'cferid': cferid, 't0': grapht, 't1': grapht+graphdt }
                            strio.write( f"Query: {cursor.mogrify( q, dictargs )}\n" )
                            strio.write( "\n".join( [ row['QUERY PLAN'] for row in rows ] ) )
                            _logger.info( strio.getvalue() )

                        cursor.execute( q, { 'cferid': cferid, 't0': grapht, 't1': grapht+graphdt } )
                        _logger.info( "... query sent, building dataframe" )
                        df = pandas.DataFrame( [ r for r in cursor.fetchall() ] )
                        if len(df) == 0:
                            _logger.info( f"No alerts in time range." )
                            continue
                        _logger.info( "...dataframe built, processing" )

                        bins = numpy.arange( grapht.timestamp(), ( grapht+graphdt ).timestamp(),
                                             options['bardt'] * 3600 )
                        binnum = numpy.arange( len(bins)-1, dtype=int )
                        df['tbin'] = pandas.cut( df.alerttime, bins=bins, labels=binnum, right=False )
                        if df['tbin'].isnull().any():
                            _logger.warning( "Some tbin values were outside the range; this shouldn't happen." )
                            df = df[ ~df.tbin.isnull() ]
                        histdf = df.groupby( ['tbin', 'n' ] ).apply( 'count' ).rename( { 'alerttime': 'count' },
                                                                                       axis=1 )
                        nonzerodf = histdf.query( 'n!=0' ).groupby( 'tbin' ).sum()
                        if len( nonzerodf ) == 0:
                            nonzerodf = None
                        if 0 in histdf.index.get_level_values('n').unique():
                            zerodf = histdf.xs( 0, level='n' )
                        else:
                            if nonzerodf is None:
                                raise RuntimeError( "This should never happen." )
                            tbinvalues = nonzerodf.index.get_level_values( 'tbin' ).unique().values
                            zerodf = pandas.DataFrame( { 'tbin': tbinvalues, 'count': 0 } ).set_index( 'tbin' )
                        if nonzerodf is None:
                            tbinvalues = zerodf.index.get_level_values( 'tbin' ).unique().values
                            nonzerodf = pandas.DataFrame( {'tbin': tbinvalues, 'count': 0 } ).set_index( 'tbin' )
                        totdf = nonzerodf.add( zerodf, fill_value=0 )
                        fracdf = nonzerodf.div( totdf, fill_value=0 )

                        # There may well be NaNs, if no alerts were sent for some of the time bins,
                        # so zero them out
                        fracdf.loc[ fracdf['count'].isnull(), 'count'] = 0

                        _logger.info( "...pandas done, plotting." )

                        # I'm not going to plot everything I have,
                        # but I have it in case I want to plot it
                        # later.  Here's what I've got, if I did all
                        # my pandas right:
                        #
                        # histdf : a histogram of number of broker
                        #    responses per alert.  indexed by tbin
                        #    and n, where n (starting at 0) is the
                        #    number of responses for alerts sent in
                        #    that tbin.  In the ideal world, this
                        #    histogram will have a big number for
                        #    n=1 and 0 for all other n.
                        #
                        # nonzerodf : indexed by tbin, the number of
                        #    alerts sent in that tbin that have at
                        #    least one broker message.
                        #
                        # zerodf : indexed by tbin, the number of alerts
                        #    sent in that tbin that do not have any
                        #    broker messages
                        #
                        # totdf : indexed by tbin, the number of alerts
                        #    sent in that tbin
                        #
                        # completeness as a function of tbin is nonzerodf / totdf

                        fig = pyplot.figure( figsize=(8,4), tight_layout=True )
                        ax = fig.add_subplot( 1, 1, 1 )
                        ax.set_title( f"{cfer['brokername']} {cfer['brokerversion']} "
                                      f"{cfer['classifiername']} {cfer['classifierparams']}\n"
                                      f"Alerts sent {timelab}" )
                        ax.set_xlabel( f"Time bin (steps of {options['bardt']} hours)" )
                        ax.set_ylabel( f"Fraction of alerts with reponse" )
                        ax.bar( binnum, fracdf['count'], width=1, align='edge' )
                        outfile = self.outdir / f'{cferid}_{timelab}.svg'
                        _logger.info( f"Writing {outfile}" )
                        fig.savefig( outfile )
                        pyplot.close( fig )

                    finally:
                        grapht += graphdt


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

