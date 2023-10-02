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
    help = 'Generate broker completeness-over-time graphs for ELAsTiCC2'
    msgoutdir = _rundir / "../../static/elasticc2/broker_message_completeness"
    dupoutdir = _rundir / "../../static/elasticc2/broker_duplicates"
    proboutdir = _rundir / "../../static/elasticc2/broker_totprob"

    def add_arguments( self, parser ):
        parser.add_argument( '-c', '--use-cached', action='store_true', default=False,
                             help="Don't send database queries, used things cached from previous run." )
    
    def handle( self, *args, **options ):
        _logger.info( "Starting gen_elasticc2_brokercompleteness" )
        
        self.msgoutdir.mkdir( parents=True, exist_ok=True )
        self.dupoutdir.mkdir( parents=True, exist_ok=True )
        self.proboutdir.mkdir( parents=True, exist_ok=True )
        conn = None

        problowers = ( -1., 0., 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99, 1.01 )
        probstr = ','.join( [ str(i) for i in problowers[1:] ] )
        probxticks = [ 0, 1, 3, 5, 7, 9, 11, 12 ]
        probxticklabels = [ '<0', '0.0', '0.2', '0.4', '0.6', '0.8', '1±0.01', '>1.01' ]

        cachedir = _rundir / "gen_brokercompleteness_cache"
        cachedir.mkdir( exist_ok=True )

        try:
            # Jump through hoops to get access to the psycopg2 connection from django
            conn = django.db.connection.cursor().connection

            with conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
                cursor.execute( 'SELECT * FROM elasticc2_brokerclassifier '
                                'ORDER BY brokername,brokerversion,classifiername,classifierparams' )
                brokers = { row["classifier_id"] : row for row in cursor.fetchall() }
                whichbrokers = list( brokers.keys() )
                # whichbrokers = [ 1 ]

                datedbrokermessagecounts = []
                duplicatecounts = []
                totprobcounts = []
                for brokerid in whichbrokers:
                    broker = brokers[brokerid]

                    thisbrokermessagecounts = []
                    _logger.info( f"Counting messages (discarding doubles) for "
                                  f"{broker['brokername']} {broker['brokerversion']} "
                                  f"{broker['classifiername']} {broker['classifierparams']}" )
                    cachefile = cachedir / f'{brokerid}_brokermessagecounts.pkl'
                    mustregen = True
                    if options['use_cached']:
                        if cachefile.is_file():
                            _logger.info( f'Loading cached {cachefile.name}' )
                            thisbrokermessagecounts = pandas.read_pickle( cachefile )
                            mustregen = False
                        else:
                            _logger.info( f"Can't find cache file {cachefile.name}, sending query" )
                    if mustregen:
                        # I really should have put classifier_id in brokermessage, because
                        #   that's unique with the new alert format!
                        q = """
                            SELECT weekdate, COUNT(brokermessage_id) AS n
                            FROM
                            ( SELECT DISTINCT ON ( a.alert_id )
                                  DATE_TRUNC('week',a.alertsenttimestamp) AS weekdate,
                                  m.brokermessage_id
                              FROM elasticc2_ppdbalert a 
                              INNER JOIN elasticc2_brokermessage m ON a.alert_id=m.alert_id
                              INNER JOIN elasticc2_brokerclassification c ON m.brokermessage_id=c.brokermessage_id
                              WHERE a.alertsenttimestamp IS NOT NULL
                                AND c.classifier_id=%(brokerid)s  ) subq
                            GROUP BY weekdate
                            ORDER BY weekdate
                        """
                        cursor.execute( q, { "brokerid": brokerid } )
                        for row in cursor.fetchall():
                            d = { 'brokerid': brokerid }
                            d.update( row )
                            thisbrokermessagecounts.append( d )
                        thisbrokermessagecounts = pandas.DataFrame( thisbrokermessagecounts )
                        thisbrokermessagecounts.to_pickle( cachefile )
                    datedbrokermessagecounts.append( thisbrokermessagecounts )

                    thisduplicatecounts = []
                    _logger.info( f"Counting number of messages per alert id (i.e. duplicate count ) for "
                                  f"{broker['brokername']} {broker['brokerversion']} "
                                  f"{broker['classifiername']} {broker['classifierparams']}" )
                    cachefile = cachedir / f'{brokerid}_duplicates.pkl'
                    mustregen = True
                    if options['use_cached']:
                        if cachefile.is_file():
                            _logger.info( f'Loading cached {cachefile.name}' )
                            thisduplicatecounts = pandas.read_pickle( cachefile )
                            mustregen = False
                        else:
                            _logger.info( f"Can't find cache file {cachefile.name}, sending query" )
                    if mustregen:
                        q = """
                            SELECT n AS nmsg, COUNT(n) AS nmsghist
                            FROM
                            ( SELECT COUNT(brokermessage_id) AS n FROM 
                              ( SELECT DISTINCT ON ( a.alert_id, m.brokermessage_id ) a.alert_id, m.brokermessage_id
                                FROM elasticc2_ppdbalert a
                                INNER JOIN elasticc2_brokermessage m ON a.alert_id=m.alert_id
                                INNER JOIN elasticc2_brokerclassification c ON m.brokermessage_id=c.brokermessage_id
                                WHERE a.alertsenttimestamp IS NOT NULL
                                  AND c.classifier_id=%(brokerid)s
                                GROUP BY a.alert_id, m.brokermessage_id
                              ) subsubq
                              GROUP BY alert_id
                            ) subq
                            GROUP BY n
                            ORDER BY n
                        """
                        cursor.execute( q, { "brokerid": brokerid } )
                        for row in cursor.fetchall():
                            d = { 'brokerid': brokerid }
                            d.update( row )
                            thisduplicatecounts.append( d )
                        thisduplicatecounts = pandas.DataFrame( thisduplicatecounts )
                        thisduplicatecounts.to_pickle( cachefile )
                    duplicatecounts.append( thisduplicatecounts )
                        
                    thistotprobcounts= []
                    _logger.info( f"Building total probability hist for "
                                  f"{broker['brokername']} {broker['brokerversion']} "
                                  f"{broker['classifiername']} {broker['classifierparams']}" )
                    cachefile = cachedir / f'{brokerid}_totprob.pkl'
                    mustregen = True
                    if options['use_cached']:
                        if cachefile.is_file():
                            _logger.info( f'Loading cached {cachefile.name}' )
                            thistotprobcounts = pandas.read_pickle( cachefile )
                            mustregen = False
                        else:
                            _logger.info( f"Can't find cache file {cachefile.name}, sending query" )
                    if mustregen:
                        q = f"""
                            SELECT probbin, COUNT(probbin) AS n
                            FROM 
                            (  SELECT width_bucket( totprob::numeric, ARRAY[{probstr}] ) AS probbin
                               FROM 
                               ( SELECT DISTINCT ON( a.alert_id, m.brokermessage_id )
                                   SUM(c.probability) AS totprob
                                 FROM elasticc2_ppdbalert a
                                 INNER JOIN elasticc2_brokermessage m ON a.alert_id=m.alert_id
                                 INNER JOIN elasticc2_brokerclassification c ON m.brokermessage_id=c.brokermessage_id
                                 WHERE a.alertsenttimestamp IS NOT NULL
                                   AND c.classifier_id=%(brokerid)s
                                 GROUP BY a.alert_id,m.brokermessage_id
                               ) subsubq
                            ) subq
                            GROUP BY probbin
                            ORDER BY probbin
                        """
                        cursor.execute( q, { 'brokerid': brokerid } )
                        for row in cursor.fetchall():
                            d = { 'brokerid': brokerid }
                            d.update( row )
                            thistotprobcounts.append( d )
                        thistotprobcounts = pandas.DataFrame( thistotprobcounts )
                        thistotprobcounts.to_pickle( cachefile )
                    totprobcounts.append( thistotprobcounts )
                    
                _logger.info( "Done with broker queries." )
                datedbrokermessagecounts = pandas.concat( datedbrokermessagecounts, axis=0, ignore_index=True )
                duplicatecounts = pandas.concat( duplicatecounts, axis=0, ignore_index=True )
                totprobcounts = pandas.concat( totprobcounts, axis=0, ignore_index=True )

                _logger.info( "Getting total number of alerts sent by week" )
                cachefile = cachedir / f'totalalerts.pkl'
                mustregen = True
                if options['use_cached']:
                    if cachefile.is_file():
                        _logger.info( f'Loading cached {cachefile.name}' )
                        datedtotalerts = pandas.read_pickle( cachefile )
                        mustregen = False
                    else:
                        _logger.info( f"Can't find cache file {cachefile.name}, sending query" )
                if mustregen:
                    datedtotalerts = []
                    q = """
                        SELECT weekdate, COUNT(alert_id) AS n FROM
                        ( SELECT a.alert_id, DATE_TRUNC('week',a.alertsenttimestamp) AS weekdate
                          FROM elasticc2_ppdbalert a
                          WHERE a.alertsenttimestamp IS NOT NULL
                        ) subq 
                        GROUP BY weekdate
                        ORDER BY weekdate
                        """
                    cursor.execute( q )
                    for row in cursor.fetchall():
                        datedtotalerts.append( row )
                    datedtotalerts = pandas.DataFrame( datedtotalerts ).set_index( 'weekdate' )
                    datedtotalerts.to_pickle( cachefile )
            conn.close()
            conn = None

            # ----------------------------------------
            
            _logger.info( "Calculating fractions of message counts to alerts" )
            tmptot = datedbrokermessagecounts.groupby( [ 'brokerid', 'weekdate' ] )['n'].apply(sum)
            fractions = []
            # I'm sure there's a Pandas way to do this without for loops,
            # but I haven't figured out the Pandas wildcard syntax necessary
            for brokerid in tmptot.index.levels[0]:
                for weekdate in tmptot.xs( brokerid, level=0 ).index.values:
                    fractions.append( { 'brokerid': brokerid, 'weekdate': weekdate,
                                        'fraction': tmptot.loc[brokerid, weekdate]
                                                    / datedtotalerts.loc[weekdate].values[0] } )
            fractions = pandas.DataFrame( fractions ).set_index( [ 'brokerid', 'weekdate' ] )

            # ----------------------------------------

            _logger.info( "Plotting broker message completeness" )
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
                ax.set_title( f"{broker['brokername']} {broker['brokerversion']} {broker['classifiername']}",
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
                _logger.info( f"Writing {str( self.msgoutdir / f'{brokerid}.svg' )}" )
                fig.savefig( self.msgoutdir / f"{brokerid}.svg" )
                pyplot.close( fig )

            # ----------------------------------------

            _logger.info( "Plotting duplicate counts" )
            duplicatecounts.set_index( [ 'brokerid', 'nmsg' ], inplace=True )
            for brokerid in whichbrokers:
                broker = brokers[brokerid]
                subdf = duplicatecounts.xs( brokerid, level=0 ).copy()
                subdf.loc[ 5, 'nmsghist' ] = subdf[ subdf.index>4 ].sum()['nmsghist']
                subdf = subdf[ subdf.index < 6 ]
                subdf.reset_index( inplace=True )
                fig = pyplot.figure( figsize=(12,6), tight_layout=True )
                ax = fig.add_subplot( 1, 1, 1 )
                ax.set_title( f"{broker['brokername']} {broker['brokerversion']} {broker['classifiername']}",
                              fontsize=18 )
                ax.set_xlabel( "Number of messages per alert" )
                ax.set_ylabel( "Count" )
                ax.bar( subdf.nmsg, subdf.nmsghist, align='center', width=0.95 )
                ax.set_xlim( 0.5, 5.5 )
                ax.set_xticks( [1, 2, 3, 4, 5] )
                ax.set_xticklabels( ['1', '2', '3', '4', '≥5' ] )
                ax.tick_params( 'both', labelsize=14 )
                _logger.info( f"Writing {str( self.dupoutdir / f'{brokerid}.svg' )}" )
                fig.savefig( self.dupoutdir / f"{brokerid}.svg" )
                pyplot.close( fig )
                
            # ----------------------------------------

            _logger.info( "Plotting total probability" )
            totprobcounts.set_index( 'brokerid', inplace=True )
            for brokerid in whichbrokers:
                broker = brokers[brokerid]
                subdf = totprobcounts.loc[ brokerid ]
                fig = pyplot.figure( figsize=(12,6), tight_layout=True )
                ax = fig.add_subplot( 1, 1, 1 )
                ax.set_title( f"{broker['brokername']} {broker['brokerversion']} {broker['classifiername']}",
                              fontsize=18 )
                ax.set_xlabel( "Total probability in broker message" )
                ax.set_ylabel( "Count" )
                ax.bar( subdf.probbin, subdf.n, align='center', width=0.95 )
                ax.set_xlim( 0, len(problowers) )
                ax.set_xticks( probxticks )
                ax.set_xticklabels( probxticklabels )
                ax.tick_params( 'both', labelsize=14 )
                _logger.info( f"Writing {str( self.proboutdir / f'{brokerid}.svg' ) }" )
                fig.savefig( self.proboutdir / f"{brokerid}.svg" )
                pyplot.close( fig )
                               
            _logger.info( "All done." )
                
        except Exception as e:
            _logger.exception( e )
            raise e
        finally:
            if conn is not None:
                conn.close()
                conn = None
            
