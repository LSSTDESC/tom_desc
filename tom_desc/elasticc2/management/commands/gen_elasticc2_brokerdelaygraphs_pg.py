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
    nbuckets = ( bucketright - bucketleft ) / dbucket
    if ( float(int(nbuckets)) != nbuckets ):
        raise ValueError( f"Can't divide {bucketleft} to {bucketright} evenly by {dbucket}" )
    nbuckets = int( nbuckets )
    bucketnums = numpy.array( range( nbuckets+2 ) )
    bucketleftedges = bucketleft + ( bucketnums - 1 ) * dbucket
    return nbuckets, bucketnums, bucketleftedges
    
class Command(BaseCommand):
    help = 'Generate broker time delay graphs'
    outdir = _rundir / "../../static/elasticc2/brokertiminggraphs"

    def add_arguments( self, parser) :
        parser.add_argument( "--t0", default="2023-10-16",
                             help="First day to look at (YYYY-MM-DD) (default: 2023-10=16)" )
        parser.add_argument( "--t1", default="2023-10-19",
                             help="One past the last day to look at (YYYY-MM-DD) (default: 2023-10-19)" )
        parser.add_argument( "--dt", default=7, type=int, help="Step in days (default: 7)" )
        
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

    def makedf( self, weeklabs, brokernames, bucketnums ):
        self.df = None
        rows = []
        weeklabs = copy.deepcopy( weeklabs )
        weeklabs.insert( 0, 'cumulative' )
        for week in weeklabs:
            for bname in brokernames:
                df = makesubdf( bucketnums )
                df[ 'broker' ] = bname
                df[ 'week' ] = week
                if self.df is None:
                    self.df = df
                else:
                    self.df = pandas.concat( [ self.df,  df ] )
        self.df.set_index( [ 'broker', 'week', 'which', 'buck' ], inplace=True )

    def handle( self, *args, **options ):
        _logger.info( "Starting genbrokerdelaygraphs" )

        bucketleft = 0
        bucketright = 6
        dbucket = 0.25

        self.bucketleft = bucketleft
        self.bucketright = bucketright
        self.dbucket = dbucket

        conn = None
        # Jump through hoops to get access to the psycopg2 connection from django
        conn = django.db.connection.cursor().connection
        orig_autocommit = conn.autocommit

        try:
            just_read_pickle = False
            updatetime = None
            
            if not just_read_pickle:

                conn.autocommit = False

                updatetime = datetime.datetime.utcnow().date().isoformat()

                self.outdir.mkdir( parents=True, exist_ok=True )

                # Determine time buckets and weeks.  Although I'm not using the
                # postgres width_bucket stuff any more, I did once upon a time,
                # so there are vestigal definitions here.
                #
                # Given postgres' width_bucket handling, everything *less than* bucketleft will be in bucket 0
                # Anything between bucketleft + (n-1)*dbucket and bucketleft + n*dbucket will be in bucket n
                # Anything >= bucketleft + nbuckets*dbucket will be in bucket nbuckets + 1
                nbuckets = ( bucketright - bucketleft ) / dbucket
                if ( float(int(nbuckets)) != nbuckets ):
                    raise ValueError( f"Can't divide {bucketleft} to {bucketright} evenly by {dbucket}" )
                nbuckets = int( nbuckets )
                bucketnums = numpy.array( range( nbuckets+2 ) )
                # bucketleftedges = bucketleft + ( bucketnums - 1 ) * dbucket
               
                t0 = pytz.utc.localize( datetime.datetime.fromisoformat( options['t0'] ) )
                t1 = pytz.utc.localize( datetime.datetime.fromisoformat( options['t1'] ) )
                dt = datetime.timedelta( days=options['dt'] )
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
                    self.makedf( weeklabs, whichgroups, bucketnums )

                    for broker in whichgroups:
                        for week, weeklab in zip( weeks, weeklabs ):
                            _logger.info( f"Doing broker {broker} week {weeklab}..." )

                            cursor.execute( "SELECT "
                                            "  EXTRACT(EPOCH FROM m.descingesttimestamp "
                                            "                     - a.alertsenttimestamp)::float AS fulldelay,"
                                            "  EXTRACT(EPOCH FROM m.msghdrtimestamp "
                                            "                     - a.alertsenttimestamp)::float AS brokerdelay,"
                                            "  EXTRACT(EPOCH FROM m.descingesttimestamp "
                                            "                     - m.msghdrtimestamp)::float AS tomdelay "
                                            "FROM elasticc2_brokermessage m "
                                            "INNER JOIN elasticc2_ppdbalert a ON m.alert_id=a.alert_id "
                                            "WHERE m.classifier_id IN %(id)s "
                                            "  AND descingesttimestamp >= %(t0)s "
                                            "  AND descingesttimestamp < %(t1)s",
                                           { 'id': tuple( brokergroups[broker] ),
                                             't0': week,
                                             't1': week+dt }
                                            )
                            tmpdf = pandas.DataFrame( cursor.fetchall() )
                            if len(tmpdf) == 0:
                                continue

                            tmpdf.clip( lower=10**bucketleft, upper=10**bucketright, inplace=True )
                            tmpdf = tmpdf.apply( numpy.log10 )
                            fullhist, binedges = numpy.histogram( tmpdf['fulldelay'],
                                                                  range=(bucketleft, bucketright+dbucket),
                                                                  bins=nbuckets+1 )
                            if not ( binedges == numpy.arange( bucketleft, bucketright+2*dbucket,
                                                               dbucket ) ).all():
                                raise ValueError( "Unexpected bins." )
                            brokerhist, binedges = numpy.histogram( tmpdf['brokerdelay'],
                                                                    range=(bucketleft, bucketright+dbucket),
                                                                    bins=nbuckets+1 )
                            tomhist, binedges = numpy.histogram( tmpdf['tomdelay'],
                                                                 range=(bucketleft, bucketright+dbucket),
                                                                 bins=nbuckets+1 )
                            curdf = None
                            for which, hist in zip( [ 'full', 'broker', 'tom' ], [ fullhist, brokerhist, tomhist ] ):
                                whichdf = pandas.DataFrame( { 'which': which,
                                                              'buck': numpy.array( ( binedges / dbucket + 1 )[:-1],
                                                                                   dtype=int ),
                                                              'count': hist } )
                                if curdf is None:
                                    curdf = whichdf
                                else:
                                    curdf = pandas.concat( [ curdf, whichdf ] )

                            curdf['broker'] = broker
                            curdf['week'] = weeklab
                            curdf.set_index( [ 'broker', 'week', 'which', 'buck' ], inplace=True )
                            # Sadly, this will convert ints to floats, but, oh well
                            self.df = self.df.add( curdf, fill_value=0 )


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
                conn.rollback()
                conn.autocommit = orig_autocommit
                conn.close()
                conn = None

