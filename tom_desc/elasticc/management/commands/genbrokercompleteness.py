import sys
import re
import pathlib
import datetime
import dateutil.parser
import dateutil.tz
import pytz
import logging
import numpy
import pandas
import psycopg2
import psycopg2.extras
import django.db
from matplotlib import pyplot
from django.core.management.base import BaseCommand, CommandError

_rundir = pathlib.Path(__file__).parent

_logger = logging.getLogger( __name__ )
_logger.propagate = False
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.INFO )

class Command(BaseCommand):
    help = 'Generate broker completeness-over-time graphs'
    outdir = _rundir / "../../static/elasticc/brokercompletenessgraphs"

    def add_arguments( self, parser) :
        parser.add_argument( '--start', default='2022-09-28',
                             help='YYYY-MM-DD of first day to look at (default: 2022-09-28)' )
        parser.add_argument( '--end', default='2023-01-31',
                             help='YYYY-MM-DD of last day to look at (default: 2023-01-31)' )

    def handle( self, *args, **options ):
        _logger.info( "Starting genbrokercompleteness" )
        
        self.outdir.mkdir( parents=True, exist_ok=True )
        conn = None

        try:
            # Jump through hoops to get access to the psycopg2 connection from django
            conn = django.db.connection.cursor().connection

            with conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
                cursor.execute( 'SELECT * FROM elasticc_brokerclassifier '
                                'ORDER BY "brokerName","brokerVersion","classifierName","classifierParams"' )
                brokers = { row["classifierId"] : row for row in cursor.fetchall() }
                whichbrokers = list( brokers.keys() )

                datedbrokercounts = []
                for brokerid in whichbrokers:
                    broker = brokers[brokerid]
                    _logger.info( f"Querying for {broker['brokerName']} {broker['brokerVersion']} "
                                  f"{broker['classifierName']} {broker['classifierParams']}" )
                    q = """
                        SELECT weekdate, n, COUNT(n) FROM
                        ( SELECT a."alertId",DATE_TRUNC('week',a."alertSentTimestamp") AS weekdate,
                            COUNT(c."classificationId") AS n
                          FROM elasticc_diaalert a
                          INNER JOIN elasticc_brokermessage m ON a."alertId"=m."alertId"
                          INNER JOIN elasticc_brokerclassification c ON c."brokerMessageId"=m."brokerMessageId"
                          WHERE a."alertSentTimestamp" IS NOT NULL
                             AND c."classifierId"=%(brokerid)s
                          GROUP BY a."alertId",a."alertSentTimestamp" ) subq
                        GROUP BY weekdate, n
                        ORDER BY weekdate, n
                        """
                    cursor.execute( q, { "brokerid": brokerid } )
                    for row in cursor.fetchall():
                        d = { 'brokerid': brokerid }
                        d.update( row )
                        datedbrokercounts.append( d )
                _logger.info( "Done with queries." )
                datedbrokercounts = pandas.DataFrame( datedbrokercounts )

                _logger.info( "Getting total number of alerts sent by week" )
                datedtotalerts = []
                q = """
                    SELECT weekdate, COUNT("alertId") FROM
                    ( SELECT a."alertId", DATE_TRUNC('week',a."alertSentTimestamp") AS weekdate
                      FROM elasticc_diaalert a
                      WHERE a."alertSentTimestamp" IS NOT NULL
                    ) subq 
                    GROUP BY weekdate
                    ORDER BY weekdate
                    """
                cursor.execute( q )
                for row in cursor.fetchall():
                    datedtotalerts.append( row )
                datedtotalerts = pandas.DataFrame( datedtotalerts ).set_index( 'weekdate' )
            conn.close()
            conn = None

            # Subtract individual broker totals from overall totals to get the zeros
            _logger.info( "Calculating zeros and fractions" )
            tmptot = datedbrokercounts.groupby( [ 'brokerid', 'weekdate' ] )['count'].apply(sum)
            zeros = []
            fractions = []
            # I'm sure there's a Pandas way to do this without for loops,
            # but I haven't figured out the Pandas wildcard syntax necessary
            for brokerid in tmptot.index.levels[0]:
                for weekdate in tmptot.xs( brokerid, level=0 ).index.values:
                    zeros.append( { 'brokerid': brokerid, 'weekdate': weekdate, 'n': 0, 
                                    'count': datedtotalerts.loc[weekdate].values[0]
                                             - tmptot.loc[brokerid, weekdate] } )
                    fractions.append( { 'brokerid': brokerid, 'weekdate': weekdate,
                                        'fraction': tmptot.loc[brokerid, weekdate]
                                                    / datedtotalerts.loc[weekdate].values[0] } )

            zeros = pandas.DataFrame( zeros ).set_index( ['brokerid', 'weekdate', 'n'] )
            datedbrokercounts = ( pandas.concat( [ datedbrokercounts, zeros ] )
                                  .sort_values( ['brokerid', 'weekdate', 'n'] ) )
            fractions = pandas.DataFrame( fractions ).set_index( [ 'brokerid', 'weekdate' ] )

            _logger.info( "Plotting" )
            mindate = datetime.datetime( 2999, 12, 31, tzinfo=pytz.utc )
            maxdate = datetime.datetime( 1970, 1, 1, tzinfo=pytz.utc )

            epoch = numpy.datetime64( '1970-01-01T00:00:00Z' )
            onesec = numpy.timedelta64( 1, 's' )
            for brokerid in fractions.index.levels[0]:
                for weekdate in fractions.xs( brokerid, level=0 ).index.values:
                    # I am very irritated that pandas uses the lesser numpy.datetime64
                    # type instead of the standard python datetime.datetime type.
                    # It was a bad design decision to make numpy.datetime64 not know
                    # anything about timezones, because they're real.
                    # Meanwhile, python's datetime by default returns things in the
                    # local timezone, but returns non-timezone-aware things.  This
                    # was another bad design decision.
                    ts = ( weekdate - epoch ) / onesec
                    t = ( datetime.datetime.fromtimestamp( ts )
                          .astimezone( dateutil.tz.tzlocal() )
                          .astimezone( pytz.utc ) )
                    if t < mindate: mindate = t
                    if t > maxdate: maxdate = t

            mindate -= datetime.timedelta( days=1 )
            maxdate += datetime.timedelta( days=8 )

            xticks = []
            xticklabels = []
            for weekdate in datedtotalerts.index.values:
                ts = ( weekdate - epoch ) / onesec
                t = ( datetime.datetime.fromtimestamp( ts )
                      .astimezone( dateutil.tz.tzlocal() )
                      .astimezone( pytz.utc ) )
                xticks.append( t )
                xticklabels.append( t.strftime( "%m-%d" ) )

            for brokerid in whichbrokers:
                broker = brokers[brokerid]
                subdf = fractions.xs( brokerid, level=0 )
                # Why, yes, it seems we need to import *two* packages
                # in addition to datetime and numpy in order to convert
                # a numpy.datetime64 to a correct time zone aware datetime
                # that's in UTC.
                x = [ datetime.datetime.fromtimestamp( ( irritating - epoch ) / onesec )
                      .astimezone( dateutil.tz.tzlocal() )
                      .astimezone( pytz.utc )
                      for irritating in subdf.index.values ]
                y = fractions.xs( brokerid, level=0 )['fraction'].values
                fig = pyplot.figure( figsize=(12,6), tight_layout=True )
                ax = fig.add_subplot( 1, 1, 1 )
                ax.set_title( f"{broker['brokerName']} {broker['brokerVersion']} {broker['classifierName']}",
                              fontsize=18 )
                ax.set_xlabel( "UTC date", fontsize=16 )
                ax.set_ylabel( "Frac. of alerts classified", fontsize=16 )
                ax.bar( x, y, align='edge', width=6.5 )
                ax.set_xlim( mindate, maxdate )
                ax.set_ylim( 0, 1.1 )
                ax.axhline( 1.0, color='black', linestyle='--' )
                ax.set_yticks( [ 0, 0.2, 0.4, 0.6, 0.8, 1.0 ] )
                ax.set_xticks( xticks )
                ax.set_xticklabels( xticklabels )
                ax.tick_params( 'both', labelsize=14 )
                _logger.info( f"Writing {str( self.outdir / f'{brokerid}.svg' )}" )
                fig.savefig( self.outdir / f"{brokerid}.svg" )
                pyplot.close( fig )

        except Exception as e:
            _logger.exception( e )
            raise e
        finally:
            if conn is not None:
                conn.close()
                conn = None
            
