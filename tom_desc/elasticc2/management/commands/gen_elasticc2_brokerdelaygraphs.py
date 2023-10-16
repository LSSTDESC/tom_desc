import sys
import io
import re
import math
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

class CassBrokerMessagePageHandler:
    def __init__( self, future, pgcursor, bucketleft, bucketright, dbucket, lowcutoff=1, highcutoff=9.99e5 ):
        self.future = future
        self.pgcursor = pgcursor
        self.bucketleft = bucketleft
        self.bucketright = bucketright
        self.dbucket = dbucket
        self.nbuckets = int( ( bucketright - bucketleft ) / dbucket + 0.5 )
        self.lowcutoff = lowcutoff
        self.highcutoff = highcutoff
        self.nhandled = 0
        self.printevery = 20000
        self.nextprint = 20000
        self.trunctime = 0
        self.striotime = 0
        self.copyfromtime = 0
        self.executetime = 0
        self.rowintime = 0
        self.tottime = None
        self.debugfirst = _logger.getEffectiveLevel() >= logging.DEBUG
        
        self.fullbuckets = {}
        self.brokerbuckets = {}
        self.tombuckets = {}

        # Create a temporary postgres table for storing the alert ids we need
        pgcursor.execute( "CREATE TEMPORARY TABLE temp_alertids( "
                          "  alert_id bigint,"
                          "  classifier_id bigint,"
                          "  brokeringesttimestamp timestamptz,"
                          "  descingesttimestamp timestamptz,"
                          "  msghdrtimestamp timestamptz "
                          ") ON COMMIT DROP" )
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
                         FROM temp_alertids t
                         INNER JOIN elasticc2_ppdbalert a ON t.alert_id=a.alert_id
                     ) subsubq
                   ) subq
                   GROUP BY fullbucket, brokerbucket, tombucket
        """ )
#                         ORDER BY t.alert_id,t.classifier_id,t.descingesttimestamp
#                   ORDER BY fullbucket, brokerbucket, tombucket
        _logger.debug( f"Ugly query: {q}" )
        pgcursor.execute( f"PREPARE bucket_join_alert_tempids AS {q}" )
        
        self.finished_event = threading.Event()
        self.error = None

        self.future.add_callbacks( callback=self.handle_page, errback=self.handle_error )

    def finalize( self ):
        self.pgcursor.connection.rollback()
        resid = None
        if self.tottime is not None:
            self.tottime = time.perf_counter() - self.tottime
            resid = ( self.tottime - self.trunctime - self.striotime
                      - self.copyfromtime - self.executetime - self.rowintime )
        outstr = io.StringIO()
        _logger.info( f"Overall: handled {self.nhandled} rows in {self.tottime} sec:\n"
                      f"      trunctime : {self.trunctime}\n"
                      f"      striotime : {self.striotime}\n"
                      f"   copyfromtime : {self.copyfromtime}\n"
                      f"    executetime : {self.executetime}\n"
                      f"      rowintime : {self.rowintime}\n"
                      f"     (residual) : {resid}\n" )

    def handle_page( self, rows ):
        if self.tottime is None:
            self.tottime = time.perf_counter()
        t0 = time.perf_counter()
        self.pgcursor.execute( "TRUNCATE TABLE temp_alertids" )

        t1 = time.perf_counter()
        strio = io.StringIO()
        for row in rows:
            strio.write( f"{row['alert_id']}\t"
                         f"{row['classifier_id']}\t"
                         f"{row['brokeringesttimestamp'].isoformat()}Z\t"
                         f"{row['descingesttimestamp'].isoformat()}Z\t"
                         f"{row['msghdrtimestamp'].isoformat()}Z\n" )
        strio.seek( 0 )
        t2 = time.perf_counter()
        self.pgcursor.copy_from( strio, 'temp_alertids', size=262144 )

        t3 = time.perf_counter()
        if self.debugfirst:
            self.debugfirst = False
            self.pgcursor.execute( "EXPLAIN ANALYZE EXECUTE bucket_join_alert_tempids" )
            analrows = self.pgcursor.fetchall()
            nl = '\n'
            _logger.debug( f'Analyzed query:\n{nl.join( [ r["QUERY PLAN"] for r in analrows ] )}' )
        self.pgcursor.execute( "EXECUTE bucket_join_alert_tempids" )
        import pdb; pdb.set_trace()
        
        t4 = time.perf_counter()
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

        t5 = time.perf_counter()
        self.nhandled += len( rows )
        self.trunctime += t1 - t0
        self.striotime += t2 - t1
        self.copyfromtime += t3 - t2
        self.executetime += t4 - t3
        self.rowintime += t5 - t4

        if self.nhandled >= self.nextprint:
            self.nextprint += self.printevery
            _logger.info( f"Handled {self.nhandled} rows" )

    def handle_error( self, exc ):
        self.error = exc
        self.finished_event.set()


class Command(BaseCommand):
    help = 'Generate broker time delay graphs'
    outdir = _rundir / "../../static/elasticc2/brokertiminggraphs"

    def add_arguments( self, parser) :
        self.bucketleft = 0
        self.bucketright = 6
        self.dbucket = 0.25
        self.lowcutoff = 1
        self.highcutoff = 9.99e5
        pass

    def makeplot( fullbuckets, brokerbuckets, tombuckets, brokername, weektitle, outfile ):
        fig = pyplot.figure( figsize=(18,4), tight_layout=True )
        for i, ( which, bucket ) in enumerate( zip( [ fullbuckets, brokerbuckets, tombuckets ],
                                                    [ 'Original Alert to Classification Ingest',
                                                      'Broker Delay',
                                                      'TOM Delay' ] ) ):
            ax = fig.add_subplot( 1, 3, i+1 )
            ax.set_title( f'' )
            ax.set_xlim( self.bucketleft, self.bucketright + self.dbucket )
            ax.set_xlabel( r"$\log_{10}(\Delta t (\mathrm{s}))$", fontsize=14 )
            ax.set_ylabel( "N", fontsize=14 )
            xticks = range( 0, 7, 1 )
            xlabels[0] = f'≤{xlabels[0]}'
            xlabels[-1] = f'≥{xlabels[-1]}'
            xlabels = [ str(i) for i in xticks ]
            ax.set_xticks( xticks, labels=xlabels )
            ax.tick_params( 'both', labelsize=12 )
            ax.bar( bucket.keys(), bucket.items(), width=dbucket, align='edge' )
        _logger.info( f"Writing {str(outfile)})" )
        fig.suptitle( f"{brokername} {weektitle}" )
        fig.savefig( outfile )
        pyplot.close( fig )
            
    
    def get_delayhist( self, brokerids, cursor,
                       t0=datetime.datetime( 2023, 10,  8, tzinfo=pytz.utc ),
                       t1=datetime.datetime( 2023, 10, 15, tzinfo=pytz.utc ) ):

        # Get all of the brokermessages from these brokerids from cassandra.
        # Go through them in batches, extracting the corresponding
        # timing information from postgres

        casssession = django.db.connections['cassandra'].connection.session
        casssession.default_fetch_size = 10000
        # Perversely, it took longer per page using the PreparedStatement
        # than it did using a simple statementbelow. ???
        # cassq = casssession.prepare( "SELECT * FROM tom_desc.cass_broker_message "
        #                              "WHERE classifier_id IN ? "
        #                              "  AND descingesttimestamp >= ? "
        #                              "  AND descingesttimestamp < ? "
        #                              "ALLOW FILTERING" )
        
        weeklyfullbuckets = {}
        weeklybrokerbuckets = {}
        weeklytombuckets = {}
        
        cumulfullbuckets = {}
        cumulbrokerbuckets = {}
        cumultombuckets = {}

        weekt0 = t0
        while weekt0 < t1:
            weekt1 = weekt0 + datetime.timedelta( days=7 )

            fullbuckets = {}
            brokerbuckets = {}
            tombuckets = {}

            # future = casssession.execute_async( cassq, ( tuple( brokerids ), weekt0, weekt1 ) )
            cassq = ( "SELECT * FROM tom_desc.cass_broker_message "
                      "WHERE classifier_id IN %(id)s "
                      "  AND descingesttimestamp >= %(t0)s "
                      "  AND descingesttimestamp < %(t1)s "
                      "ALLOW FILTERING" )
            future = casssession.execute_async( cassq, { 'id': tuple(brokerids), 't0': weekt0, 't1': weekt1 } )
            handler = CassBrokerMessagePageHandler( future, cursor, self.bucketleft, self.bucketright, self.dbucket,
                                                    lowcutoff=self.lowcutoff, highcutoff=self.highcutoff )
            handler.finished_event.wait()
            handler.finalize()
            if handler.error:
                _logger.error( handler.error )
                raise handler.error

            for mybucket, cumulbucket, handlerbucket in zip( [ fullbuckets, brokerbuckets, tombuckets ],
                                                             [ cumulfullbuckets, cumulbrokerbuckets,
                                                               cumultombuckets ],
                                                             [ handler.fullbuckets, handler.brokerbuckets,
                                                               handler.tombuckets ] ):
                for delay, count in handlerbucket.items():
                    if delay not in mybucket:
                        mybucket[ delay ] = count
                    else:
                        mybucket[ delay ] += count
                    if delay not in cumulbucket:
                        cumulbucket[ delay ] = count
                    else:
                        cumulbucket[ delay ] += count

            weeklyfullbuckets[ weekt0 ] = fullbuckets
            weeklybrokerbuckets[ weekt0 ] = brokerbuckets
            weeklytombuckets[ weekt0 ] = tombuckets

            weekt0 = weekt1
            
        return ( weeklyfullbuckets, weeklybrokerbuckets, weeklytombuckets,
                 cumulfullbuckets, cumulbrokerbuckets, cumultombuckets )


    def handle( self, *args, **options ):
        _logger.info( "Starting genbrokerdelaygraphs" )

        self.outdir.mkdir( parents=True, exist_ok=True )
        conn = None

        dbucket = 0.25
        t0 = datetime.datetime( 2023, 10, 16, tzinfo=pytz.utc )
        t1 = datetime.datetime( 2023, 10, 23, tzinfo=pytz.utc )
        
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

            for broker in whichgroups:
                _logger.info( f"Doing broker {broker}" )

                with conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
                    ( weeklyfullbuckets,
                      weeklybrokerbuckets,
                      weeklytombuckets,
                      cumulfullbuckets,
                      cumulbrokerbuckets,
                      cumultombuckets ) = self.get_delayhist( brokergroups[ broker ], cursor, t0, t1 )

                    self.makeplot( cumulfullbuckets, cumulbrokerbuckets, cumultombuckets, broker,
                                   '— Cumulative', f'{broker}-cumul.svg' )
                    for week in weeklyfullbuckets.keys():
                        self.makeplot( weeklyfullbuckets[week], weeklybrokerbuckets[week], weeklytombuckets[week],
                                       '— Week of {week.year:4d}-{week.month:02d}-{week.day:02d}',
                                       f'{broker}-{week.year:4d}-{week.month:02d}-{week.day:02d}' )

            _logger.info( "All done." )
        except Exception as e:
            _logger.exception( e )
            _logger.exception( traceback.format_exc() )
            import pdb; pdb.set_trace()
            raise e
        finally:
            if conn is not None:
                conn.autocommit = orig_autocommit
                conn.close()
                conn = None

