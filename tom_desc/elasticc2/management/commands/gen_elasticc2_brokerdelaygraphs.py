import sys
import re
import math
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
import cassandra.query

_rundir = pathlib.Path(__file__).parent

_logger = logging.getLogger( __name__ )
_logger.propagate = False
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.INFO )

class CassBrokerMessagePageHandler:
    def __init__( self, future, pgcursor, bucketleft, bucketright, dbucket, lowcutoff=1, highcutoff=9.99e5 ):
        self.future = future
        self.pgcursor = pgcursor
        self.bucketleft = bucketleft
        self.bucketright = bucketright
        self.dbucket = dbucket
        self.lowcutoff = lowcutoff
        self.highcutoff = highcutoff

        self.fullbuckets = {}
        self.brokerbuckets = {}
        self.tombuckets = {}

        # Create a temporary postgres table for storing the alert ids we need
        pgcursor.execute( "CREATE TEMPORARY TABLE temp_alertids( "
                          "  alert_id bigint,"
                          "  classifier_id bigint,"
                          "  brokeringesttimestamp timestamp with timezone,"
                          "  descingesttimestamp timestamp with timezone,"
                          "  msghdrtimestamp timestamp with timezone "
                          ") ON COMMIT DROP" )

        self.finished_event = Event()
        self.error = None

        self.future.add_callbacks( callback=self.handle_page, errback=self.handle_err )

    def finalize:
        self.pgcursor.connection.rollback()

    def handle_page( self, rows ):
        pgcursor.execute( "TRUNCATE TABLE temp_alertids" )

        strio = io.StringIO()
        for row in rows:
            strio.write( f"{row['alert_id']}\t"
                         f"{row['classifier_id']}\t"
                         f"{row['brokeringesttimestamp'].isoformat()}Z\t"
                         f"{row['descingesttimestamp'].isoformat()}Z\t"
                         f"{row['msghdrtimestamp'].isoformat()}Z\n" )
        strio.seek( 0 )
        self.pgcursor.copy_from( strio, 'temp_alertids', size=262144 )

        q = ( f"""SELECT fullbucket AS fullbin,
                           {self.bucketleft}+{self.dbucket}*(fullbucket-1) AS log10_fulldelay_sec,
                           COUNT(fullbucket) AS nfull,
                         brokerbucket AS brokerbin,
                           {self.bucketleft}+{self.dbucket}*(brokerbucket-1) AS log10_brokerdelay_sec,
                           COUNT(fullbucket) AS nbroker,
                         tombucket AS tombin,
                           {self.bucketleft}+{self.dbucket}*(tombucket-1) AS log10_tomdelay_sec,
                           COUNT(tombucket) AS ntom
                  FROM (
                    SELECT width_bucket( LOG( LEAST( GREATEST( {self.lowcutoff},
                                                               EXTRACT(EPOCH FROM fulldelay) ),
                                              {self.highcutoff} ) ),
                                         {self.bucketleft}, {self.bucketright}, {self.nbuckets} ) AS fullbucket,
                           width_bucket( LOG( LEAST( GREATEST( {self.lowcutoff},
                                                     EXTRACT(EPOCH FROM brokerdelay) ),
                                              {self.highcutoff} ) ),
                                         {self.bucketleft}, {self.bucketright}, {self.nbuckets} ) AS brokerbucket,
                           width_bucket( LOG( LEAST( GREATEST( {self.lowcutoff},
                                                               EXTRACT(EPOCH FROM tomdelay) ),
                                              {self.highcutoff} ) ),
                                         {self.bucketleft}, {self.bucketright}, {self.nbuckets} ) AS tombucket

                     FROM (
                        SELECT DISTINCT ON(t.alert_id,t.classifier_id)
                            t.descingesttimestamp - a.alertsenttimestamp AS fulldelay,
                            t.msghdrtimestamp - a.alertsenttimestamp AS brokerdelay,
                            t.descingesttimestamp - t.msghdrtimestamp AS tomdelay
                         FROM temp_alertids
                         INNER JOIN elasticc2_ppdbalert a ON t.alert_id=a.alert_id
                         ORDER BY t.alert_id,t.classifier_id,t.descingesttimestamp
                     ) subsubq
                   ) subq
                   GROUP BY fullbucket, brokerbucket, tombucket
                   ORDER BY fullbucket, brokerbucket, tombucket
        """ )
        self.pgcursor.execute( q )

        for row in self.pgcursor:
            for which in [ 'full', 'broker', 'tom' ]:
                buckets = getattr( self, f'{which}buckets' )
                if row[f'log10_{which}delay_sec'] not in buckets:
                    buckets[ row[f'log10_{which}delay_sec'] ] = row[ f'n{which}' ]
                else:
                    buckets[ row[f'log10_{which}delay_sec'] ] += row[ f'n{which}' ]

        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def handle_error( self, exc ):
        self.error = exc
        self.finished_event.set()


class Command(BaseCommand):
    help = 'Generate broker time delay graphs'
    outdir = _rundir / "../../static/elasticc2/brokertiminggraphs"

    def add_arguments( self, parser) :
        pass





    def get_delayhist( self, brokerids, cursor, bucketleft=0, bucketright=6, dbucket=0.25,
                       t0=datetime.datetime( 2023, 10, 08, tzinfo=pytz.utc ),
                       t1=datetime.datetime( 2023, 10, 15, tzinfo=pytz.utc ),
                       lowcutoff=1, highcutoff=9.99e5):

        # Get all of the brokermessages from these brokerids from cassandra.
        # Go through them in batches, extracting the corresponding
        # timing information from postgres

        fullbuckets = {}
        brokerbuckets = {}
        tombuckets = {}


        weekt0 = t0
        while weekt0 < t1:
            weekt1 = weekt0 + datetime.timedelta( days=7 )

            cassq = cassandray.query.SimpleStatement( "SELECT * FROM tom_desc.cass_broker_message "
                                                      "WHERE classifier_id IN %(ids)s "
                                                      "  AND descingesttimestamp >= %(t0)s "
                                                      "  AND descingesttimestamp <= %(t1)s "
                                                      fetch_size=10000 )
            future = django.db.connection['cassandra'].execute( cassq, { 'ids': tuple( brokerids ) } )
            handler = CassBrokerMessagePageHandler( future, cursor, bucketleft, bucketright, dbucket,
                                                    lowcutoff=lowcutoff, highcutoff=highcutoff )
            handler.finished_event.wait()
            if handler.error:
                raise handler.error

            for mybucket, handlerbucket in zip( [ fullbuckets, brokerbuckets, tombuckets ],
                                                [ handler.fulbuckets, handler.brokerbuckets, handler.tombuckets ] ):
                for delay, count in handlerbucket.items():
                    if delay not in mybucket:
                        mybucket[ delay ] = count
                    else:
                        mybucket[ delay ] += count

                        ROB YOU WERE HERE


    def handle( self, *args, **options ):
        _logger.info( "Starting genbrokerdelaygraphs" )

        self.outdir.mkdir( parents=True, exist_ok=True )
        conn = None

        dbucket = 0.25

        # Jump through hoops to get access to the psycopg2 connection from django
        conn = django.db.connection.cursor().connection
        orig_autocommit = conn.autocommit

        try:
            conn.autocommit = False

            updatetime = datetime.datetime.utcnow().date().isoformat()


            with conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
                cursor.execute( 'SELECT * FROM elasticc2_brokerclassifier '
                                'ORDER BY "brokername","brokerversion","classifiername","classifierparams"' )
                brokers = { row["classifier_id"] : row for row in cursor.fetchall() }
                conn.rollback()

                brokergroups = {}
                for brokerid, row in brokers.items():
                    if row['brokername'] not in brokergroups:
                        brokergroups[row['brokername']] = []
                    brokergroups[row['brokername']].append( row['classifier_id'] )

                whichgroups = [ k for k in brokergroups.keys() ]
                # whichgroups = [ 'Fink' ]

                timingstmp = []
                for brokername in whichgroups:
                    brokerids = brokergroups[ brokername ]
                    _logger.info( f"Sending timings query for {brokername}" ) 
                    rows = self.get_delayhist( brokerids, cursor, dbucket=dbucket )
                    for row in rows:
                        d = { 'broker': brokername }
                        d.update( row )
                        timingstmp.append( d )
                _logger.info( f"Done with queries." )

                import pdb; pdb.set_trace()

            alltimings = pandas.DataFrame( timingstmp ).set_index( [ 'broker', 'whichweek',
                                                                     'fullbin', 'log10_fulldelay_sec',
                                                                     'brokerbin', 'log10_brokerdelay_sec',
                                                                     'tombin', 'log10_tomdelay_sec' ] )

            fulltimings = ( alltimings.groupby( [ 'broker', 'whichweek',
                                                  'fullbin', 'log10_fulldelay_sec' ] )['nfull'].apply('sum')
                            .reset_index().drop( columns='fullbin' )
                            .rename( columns={ 'log10_fulldelay_sec': 'log10_delay_sec', 'nfull': 'n' } )
                            .set_index( ['broker', 'whichweek', 'log10_delay_sec'] ) )
            brokertimings = ( alltimings.groupby( [ 'broker', 'whichweek',
                                                    'brokerbin', 'log10_brokerdelay_sec' ] )['nbroker'].apply('sum')
                             .reset_index().drop( columns='brokerbin' )
                             .rename( columns={ 'log10_brokerdelay_sec': 'log10_delay_sec', 'nbroker': 'n' } )
                             .set_index( ['broker', 'whichweek', 'log10_delay_sec'] ) )
            tomtimings = ( alltimings.groupby( [ 'broker', 'whichweek',
                                                 'tombin', 'log10_tomdelay_sec' ] )['ntom'].apply('sum')
                           .reset_index().drop( columns='tombin' )
                           .rename( columns={ 'log10_tomdelay_sec': 'log10_delay_sec', 'ntom': 'n' } )
                           .set_index( ['broker', 'whichweek', 'log10_delay_sec'] ) )

            sumfulltimings = fulltimings.groupby( ['broker', 'log10_delay_sec'] ).apply('sum')
            sumbrokertimings = brokertimings.groupby( ['broker', 'log10_delay_sec'] ).apply('sum')
            sumtomtimings = tomtimings.groupby( ['broker', 'log10_delay_sec'] ).apply('sum')

            weeks = fulltimings.index.get_level_values('whichweek').unique()
            weekdates = { w: w.date().isoformat() for w in weeks }

            minx = 0
            maxx = 6
            xticks = range( minx, maxx+1, 1 )
            xlabels = [ str(i) for i in xticks ]
            xlabels[0] = f"≤{xlabels[0]}"
            xlabels[-1] = f"≥{xlabels[-1]}"

            timingslist = [ fulltimings, brokertimings, tomtimings ]
            sumlist = [ sumfulltimings, sumbrokertimings, sumtomtimings ]
            titles = [ 'Original Alert to Classification Ingest',
                       'Broker Delay',
                       'TOM Delay' ]

            def plotit( titles, timingslist, brokername, week, outfile ):
                fig = pyplot.figure( figsize=(5*len(timingslist), 4), tight_layout=True )
                for i, timings in enumerate( timingslist ):
                    if week is None:
                        df = timings.xs( brokername )
                    else:
                        df = timings.xs( ( brokername, week ) )
                    ax = fig.add_subplot( 1, len(timingslist), i+1 )
                    ax.set_title( titles[i], fontsize=16 )
                    ax.set_xlabel( r"$\log_{10}(\Delta t (\mathrm{s}))$", fontsize=14 )
                    ax.set_xlim( minx, maxx )
                    ax.set_ylabel( "N", fontsize=14 )
                    ax.bar( df.index.values, df['n'].values, width=dbucket, align='edge' )
                    ax.set_xticks( xticks, labels=xlabels )
                    ax.tick_params( 'both', labelsize=12 )
                _logger.info( f"Writing {str(outfile)})" )
                fig.savefig( outfile )
                pyplot.close( fig )

            for brokername in whichgroups:
                for week in weeks:
                    try:
                        outfile = self.outdir / f'{brokername}_{weekdates[week]}.svg'
                        plotit( titles, timingslist, brokername, week, outfile )
                    except KeyError as e:
                        _logger.warning( f"Key error for broker {brokername}, week {week}" )

                outfile = self.outdir / f'{brokername}_summed.svg'
                plotit( titles, sumlist, brokername, None, outfile )

            with open( self.outdir / "updatetime.txt", "w" ) as ofp:
                ofp.write( updatetime )

            _logger.info( "Done." )

        except Exception as e:
            _logger.exception( e )
            raise e
        finally:
            if conn is not None:
                conn.close()
                conn = None

