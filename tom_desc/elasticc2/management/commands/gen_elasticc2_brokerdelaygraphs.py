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

def makesubdf( bucketnums ):
    rows = []
    for which in [ 'full', 'broker', 'tom' ]:
        for buck in bucketnums:
            rows.append( { 'which': which, 'buck': buck, 'count': 0 } )
    return pandas.DataFrame( rows )

def calcbucks( bucketleft, bucketright, dbucket ):
    # Given postgres' width_bucket handling, everything *less than* bucketleft will be in bucket 0
    # Anything between bucketleft + (n-1)*dbucket and bucketleft + n*dbucket will be in bucket n
    # Anything >= bucketleft + nbuckets*dbucket will be in bucket nbuckets + 1
    nbuckets = ( bucketright - bucketleft ) / dbucket
    if ( float(int(nbuckets)) != nbuckets ):
        raise ValueError( f"Can't divide {bucketleft} to {bucketright} evenly by {dbucket}" )
    nbuckets = int( nbuckets )
    bucketnums = numpy.array( range( nbuckets+2 ) )
    bucketleftedges = bucketleft + ( bucketnums - 1 ) * dbucket
    return nbuckets, bucketnums, bucketleftedges
    

class CassBrokerMessagePageHandler:
    def __init__( self, future, pgcursor, bucketleft, bucketright, dbucket, lowcutoff=1, highcutoff=9.99e5 ):
        self.future = future
        self.pgcursor = pgcursor
        self.bucketleft = bucketleft
        self.bucketright = bucketright
        self.dbucket = dbucket
        self.nbuckets, self.bucketnums, self.bucketleftedges = calcbucks( self.bucketleft,
                                                                          self.bucketright,
                                                                          self.dbucket )
        self.lowcutoff = lowcutoff
        self.highcutoff = highcutoff
        self.nhandled = 0
        self.printevery = 20000
        self.nextprint = 20000
        self.trunctime = 0
        self.striotime = 0
        self.copyfromtime = 0
        self.executetime = 0
        self.pandastime = 0
        self.futuretime = 0
        self.tottime = None
        self.debugfirst = _logger.getEffectiveLevel() >= logging.DEBUG
        
        # Create a temporary postgres table for storing the alert ids we need
        pgcursor.execute( "CREATE TEMPORARY TABLE temp_alertids( "
                          "  alert_id bigint,"
                          "  classifier_id bigint,"
                          "  brokeringesttimestamp timestamptz,"
                          "  descingesttimestamp timestamptz,"
                          "  msghdrtimestamp timestamptz "
                          ") ON COMMIT DROP" )

        # Precompile the postgres query we're going to use
        pgcursor.execute( "DEALLOCATE ALL" )
        q = ( f"""SELECT DISTINCT ON(t.alert_id,t.classifier_id)
                         EXTRACT(EPOCH FROM t.descingesttimestamp - a.alertsenttimestamp)::float AS fulldelay,
                         EXTRACT(EPOCH FROM t.msghdrtimestamp - a.alertsenttimestamp)::float AS brokerdelay,
                         EXTRACT(EPOCH FROM t.descingesttimestamp - t.msghdrtimestamp)::float AS tomdelay
                  FROM temp_alertids t
                  INNER JOIN elasticc2_ppdbalert a ON t.alert_id=a.alert_id
        """ )
        _logger.debug( f"(no-longer-so-) Ugly query: {q}" )
        pgcursor.execute( f"PREPARE bucket_join_alert_tempids AS {q}" )

        self.df = makesubdf( self.bucketnums ).set_index( [ 'which', 'buck' ] )
                
        self.finished_event = threading.Event()
        self.error = None

        self.future.add_callbacks( callback=self.handle_page, errback=self.handle_error )

    def finalize( self ):
        self.pgcursor.connection.rollback()
        resid = None
        if self.tottime is not None:
            self.tottime = time.perf_counter() - self.tottime
            resid = ( self.tottime - self.trunctime - self.striotime
                      - self.copyfromtime - self.executetime - self.pandastime - self.futuretime )
        outstr = io.StringIO()
        _logger.info( f"Overall: handled {self.nhandled} rows in {self.tottime} sec:\n"
                      f"      trunctime : {self.trunctime}\n"
                      f"      striotime : {self.striotime}\n"
                      f"   copyfromtime : {self.copyfromtime}\n"
                      f"    executetime : {self.executetime}\n"
                      f"     pandastime : {self.pandastime}\n"
                      f"     futuretime : {self.futuretime}\n"
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

        t4 = time.perf_counter()
        tmpdf = pandas.DataFrame( self.pgcursor.fetchall() )

        if len(tmpdf) > 0:
            tmpdf.clip( lower=10**self.bucketleft, upper=10**self.bucketright, inplace=True )
            tmpdf = tmpdf.apply( numpy.log10 )
            fullhist, binedges = numpy.histogram( tmpdf['fulldelay'],
                                                  range=(self.bucketleft,self.bucketright+self.dbucket),
                                                  bins=self.nbuckets+1 )
            if not ( binedges == numpy.arange( self.bucketleft, self.bucketright+2*self.dbucket,
                                               self.dbucket ) ).all():
                raise ValueError( "Unexpected bins." )
            brokerhist, binedges = numpy.histogram( tmpdf['brokerdelay'],
                                                    range=(self.bucketleft,self.bucketright+self.dbucket),
                                                    bins=self.nbuckets+1 )
            tomhist, binedges = numpy.histogram( tmpdf['tomdelay'],
                                                 range=(self.bucketleft,self.bucketright+self.dbucket),
                                                 bins=self.nbuckets+1 )
            curdf = None
            for which, hist in zip( [ 'full', 'broker', 'tom' ], [ fullhist, brokerhist, tomhist ] ):
                whichdf = pandas.DataFrame( { 'which': which,
                                              'buck': numpy.array( ( binedges / self.dbucket + 1 )[:-1], dtype=int ),
                                              'count': hist } )
                if curdf is None:
                    curdf = whichdf
                else:
                    curdf = pandas.concat( [ curdf, whichdf ] )

            curdf.set_index( [ 'which', 'buck' ], inplace=True )
            # Sadly, this will convert ints to floats, but, oh well
            self.df = self.df.add( curdf, fill_value=0 )

        t5 = time.perf_counter()
        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

        t6 = time.perf_counter()
        self.nhandled += len( rows )
        self.trunctime += t1 - t0
        self.striotime += t2 - t1
        self.copyfromtime += t3 - t2
        self.executetime += t4 - t3
        self.pandastime += t5 - t4
        self.futuretime += t6 - t5

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

    def makeplots( self ):
        brokers = set( self.df.index.get_level_values( 'broker' ) )
        weeks = set( self.df.index.get_level_values( 'week' ) )

        whichtitle = { 'full': "Orig. Alert to Tom Ingestion",
                       'broker': "Broker Delay",
                       'tom': "Tom Delay" }
        
        for broker in brokers:
            for week in weeks:
                fig = pyplot.figure( figsize=(18,4), tight_layout=True )
                for i, which in enumerate( [ 'full', 'broker', 'tom' ] ):
                    subdf = self.df.xs( ( broker, week, which ), level=( 'broker', 'week', 'which' ) )
                    ax = fig.add_subplot( 1, 3, i+1 )
                    ax.set_title( whichtitle[ which ], fontsize=18 )
                    ax.set_xlim( self.bucketleft, self.bucketright + self.dbucket )
                    ax.set_xlabel( r"$\log_{10}(\Delta t (\mathrm{s}))$", fontsize=14 )
                    ax.set_ylabel( "N", fontsize=14 )
                    tickvals = numpy.arange( 0, 7, 1 )
                    # +1 since the lowest bucket in postgres is 1 (0 being < the lowest bucket)
                    xticks = tickvals / self.dbucket + self.bucketleft + 1
                    xlabels = [ str(i) for i in tickvals ]
                    xlabels[0] = f'≤{xlabels[0]}'
                    xlabels[-1] = f'≥{xlabels[-1]}'
                    ax.set_xticks( xticks, labels=xlabels )
                    ax.tick_params( 'both', labelsize=12 )
                    ax.bar( subdf.index.values[1:], subdf['count'].values[1:], width=1, align='edge' )
                outfile = self.outdir / f'{broker}-{week}.svg'
                _logger.info( f"Writing {outfile}" )
                if week == 'cumulative':
                    fig.suptitle( broker, fontsize=20 )
                else:
                    fig.suptitle( f"{broker}, {week} UTC", fontsize=20 )
                fig.savefig( outfile )
                pyplot.close( fig )

    def makedf( self, weeklabs, brokernames ):
        self.df = None
        rows = []
        weeklabs = copy.deepcopy( weeklabs )
        weeklabs.insert( 0, 'cumulative' )
        for week in weeklabs:
            for bname in brokernames:
                df = makesubdf( self.bucketnums )
                df[ 'broker' ] = bname
                df[ 'week' ] = week
                if self.df is None:
                    self.df = df
                else:
                    self.df = pandas.concat( [ self.df,  df ] )
        self.df.set_index( [ 'broker', 'week', 'which', 'buck' ], inplace=True )

    def handle( self, *args, **options ):
        _logger.info( "Starting genbrokerdelaygraphs" )

        conn = None
        # Jump through hoops to get access to the psycopg2 connection from django
        conn = django.db.connection.cursor().connection
        orig_autocommit = conn.autocommit

        try:
            just_read_pickle = False
            updatetime = None
            
            if not just_read_pickle:

                casssession = django.db.connections['cassandra'].connection.session
                casssession.default_fetch_size = 10000
                # Perversely, it took longer per page using the PreparedStatement
                # than it did using a simple statementbelow. ???
                # cassq = casssession.prepare( "SELECT * FROM tom_desc.cass_broker_message "
                #                              "WHERE classifier_id IN ? "
                #                              "  AND descingesttimestamp >= ? "
                #                              "  AND descingesttimestamp < ? "
                #                              "ALLOW FILTERING" )

                conn.autocommit = False

                updatetime = datetime.datetime.utcnow().date().isoformat()

                self.outdir.mkdir( parents=True, exist_ok=True )

               # Determine time buckets and weeks

                self.nbuckets, self.bucketnums, self.bucketleftedges = calcbucks( self.bucketleft,
                                                                                  self.bucketright,
                                                                                  self.dbucket )

                t0 = datetime.datetime( 2023, 10, 16, tzinfo=pytz.utc )
                t1 = datetime.datetime( 2023, 10, 19, tzinfo=pytz.utc )
                dt = datetime.timedelta( days=1 )
                weeks = []
                week = t0
                while ( week < t1 ):
                    weeks.append( week )
                    week += dt
                weeklabs = [ f'[{w.year:04d}-{w.month:02d}-{w.day:02d} , '
                             f'{(w+dt).year:04d}-{(w+dt).month:02d}-{(w+dt).day:02d})' for w in weeks ]

                with conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
                    # Figure out which brokers we have
                    cursor.execute( 'SELECT * FROM elasticc2_brokerclassifier '
                                    'ORDER BY "brokername","brokerversion","classifiername","classifierparams"' )
                    brokers = { row["classifier_id"] : row for row in cursor.fetchall() }
                    conn.rollback()

                    brokergroups = {}
                    for brokerid, row in brokers.items():
                        if row['brokername'] not in brokergroups:
                            brokergroups[row['brokername']] = []
                        brokergroups[row['brokername']].append( row['classifier_id'] )

                    # Choose the brokers to actually work on ( for debugging purposes )
                    whichgroups = [ k for k in brokergroups.keys() ]
                    # whichgroups = [ 'Fink' ]

                    # This is the master df that we'll append to as we
                    # iterate through brokers and weeks
                    self.makedf( weeklabs, whichgroups )

                    for broker in whichgroups:
                        for week, weeklab in zip( weeks, weeklabs ):
                            _logger.info( f"Doing broker {broker} week {weeklab}..." )

                            # Extract the data from the database for this broker
                            # and week (the CassBrokerMessagePageHandler will
                            # send a postgres query for each page returned from
                            # Cassandra)

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

