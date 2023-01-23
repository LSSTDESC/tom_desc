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
    outdir = _rundir / "../../static/elasticc/brokertiminggraphs"

    def add_arguments( self, parser) :
        pass

    def get_delayhist( self, brokerids, cursor, bucketleft=0, bucketright=6, dbucket=0.25,
                       lowcutoff=1, highcutoff=9.99e5):
        queries = []
        subdicts = []

        # ROB!  Play with Postgres to make sure floor is right here

        nbuckets = int( math.floor( ( bucketright - bucketleft ) / dbucket ) )

        q = ( f"""SELECT whichweek,
                         fullbucket AS fullbin,
                           {  bucketleft}+{dbucket}*(fullbucket-1) AS log10_fulldelay_sec,
                           COUNT(fullbucket) AS nfull,
                         brokerbucket AS brokerbin,
                           {bucketleft}+{dbucket}*(brokerbucket-1) AS log10_brokerdelay_sec,
                           COUNT(fullbucket) AS nbroker,
                         tombucket AS tombin,
                           {bucketleft}+{dbucket}*(tombucket-1) AS log10_tomdelay_sec,
                           COUNT(tombucket) AS ntom
                  FROM (
                    SELECT whichweek,
                           width_bucket( LOG( LEAST( GREATEST( {lowcutoff},
                                                               EXTRACT(EPOCH FROM fulldelay) ),
                                              {highcutoff} ) ),
                                         {bucketleft}, {bucketright}, {nbuckets} ) AS fullbucket,
                           width_bucket( LOG( LEAST( GREATEST( {lowcutoff},
                                                     EXTRACT(EPOCH FROM brokerdelay) ),
                                              {highcutoff} ) ),
                                         {bucketleft}, {bucketright}, {nbuckets} ) AS brokerbucket,
                           width_bucket( LOG( LEAST( GREATEST( {lowcutoff},
                                                               EXTRACT(EPOCH FROM tomdelay) ),
                                              {highcutoff} ) ),
                                         {bucketleft}, {bucketright}, {nbuckets} ) AS tombucket
                                
                    FROM (
                      SELECT DISTINCT ON (m."brokerMessageId")
                         DATE_TRUNC('week',m."descIngestTimestamp") AS whichweek,
                         m."descIngestTimestamp"-a."alertSentTimestamp" AS fulldelay,
                         m."msgHdrTimestamp"-a."alertSentTimestamp" AS brokerdelay,
                         m."descIngestTimestamp"-m."msgHdrTimestamp" AS tomdelay
                      FROM elasticc_brokermessage m
                      INNER JOIN elasticc_diaalert a ON m."alertId"=a."alertId"
                      INNER JOIN elasticc_brokerclassification c ON c."brokerMessageId"=m."brokerMessageId"
                      WHERE c."classifierId" IN %(brokers)s
                      ORDER BY m."brokerMessageId"
                    ) subsubq
                  ) subq
                  GROUP BY whichweek,fullbucket,brokerbucket,tombucket
                  ORDER BY whichweek,fullbucket,brokerbucket,tombucket""" )
        cursor.execute( q, { 'brokers': tuple( brokerids ) } )
        return list( cursor.fetchall() )
    
    def handle( self, *args, **options ):
        _logger.info( "Starting genbrokerdelaygraphs" )
        
        self.outdir.mkdir( parents=True, exist_ok=True )
        conn = None

        dbucket = 0.25
        
        try:
            updatetime = datetime.datetime.utcnow().date().isoformat()
            
            # Jump through hoops to get access to the psycopg2 connection from django
            conn = django.db.connection.cursor().connection

            with conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
                cursor.execute( 'SELECT * FROM elasticc_brokerclassifier '
                                'ORDER BY "brokerName","brokerVersion","classifierName","classifierParams"' )
                brokers = { row["classifierId"] : row for row in cursor.fetchall() }

                brokergroups = {}
                for brokerid, row in brokers.items():
                    if row['brokerName'] not in brokergroups:
                        brokergroups[row['brokerName']] = []
                    brokergroups[row['brokerName']].append( row['classifierId'] )

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
            
