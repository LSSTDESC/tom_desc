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

    def get_delayhist( self, brokerids, thedelay, cursor ):
        queries = []
        subdicts = []

        bucketleft = -2
        bucketright = 8
        dbucket = 1
        # ROB!  Play with Postgres to make sure floor is right here

        nbuckets = int( math.floor( ( bucketright - bucketleft ) / dbucket ) )

        q = f"""SELECT bucky AS bin, {bucketleft}+{dbucket}*(bucky-1) AS log10_delay_sec, 
                       COUNT(bucky) AS n FROM (
                   SELECT width_bucket( LOG( GREATEST( 0.001, EXTRACT(EPOCH FROM delay) ) ),
                                        {bucketleft}, {bucketright}, {nbuckets} ) AS bucky
                   FROM (
                     SELECT DISTINCT ON (m."brokerMessageId") {thedelay} AS delay
                     FROM elasticc_brokermessage m
                     INNER JOIN elasticc_diaalert a ON m."alertId"=a."alertId"
                     INNER JOIN elasticc_brokerclassification c ON c."brokerMessageId"=m."brokerMessageId"
                     WHERE c."classifierId" IN %(brokers)s
                     ORDER BY m."brokerMessageId"
                  ) subsubq
                ) subq
                GROUP BY bucky
                ORDER BY bucky"""
        cursor.execute( q, { 'brokers': tuple( brokerids ) } )
        return list( cursor.fetchall() )
    
    def handle( self, *args, **options ):
        _logger.info( "Starting genbrokerdelaygraphs" )
        
        self.outdir.mkdir( parents=True, exist_ok=True )
        conn = None

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
                
                whichgroups = list( brokergroups.keys() )
                # whichgroups = [ 'Fink' ]

                timings = { 'full': None,
                            'brokerdelay': None,
                            'tomdelay': None }
                diffs = { 'full': 'm."descIngestTimestamp"-a."alertSentTimestamp"',
                          'brokerdelay': 'm."msgHdrTimestamp"-a."alertSentTimestamp"',
                          'tomdelay': 'm."descIngestTimestamp"-m."msgHdrTimestamp"',
                          }
                for which in timings.keys():
                    timingstmp = []
                    for brokername in whichgroups:
                        brokerids = brokergroups[ brokername ]
                        _logger.info( f"Sending {which} query for {brokername}" ) 
                        rows = self.get_delayhist( brokerids, diffs[which], cursor )
                        for row in rows:
                            d = { 'broker': brokername }
                            d.update( row )
                            timingstmp.append( d )
                    _logger.info( f"Done with {which} queries." )

                    timings[which] = pandas.DataFrame( timingstmp ).set_index( [ 'broker', 'log10_delay_sec' ] )
                
            minx = -3
            maxx = 8
            titles = { 'full': 'Original Alert to Classification Ingest',
                       'brokerdelay': 'Broker Delay',
                       'tomdelay': 'TOM Delay' }
            
            for brokername in whichgroups:
                fig = pyplot.figure( figsize=(5*len(timings), 4), tight_layout=True )
                for i, which in enumerate( timings.keys() ):
                    df = timings[which].xs( brokername, level='broker' )
                    ax = fig.add_subplot( 1, len(timings), i+1 )
                    ax.set_title( titles[which], fontsize=16 )
                    ax.set_xlabel( r"$\log_{10}(\Delta t (\mathrm{s}))$", fontsize=14 )
                    ax.set_xlim( minx, maxx )
                    ax.set_ylabel( "N", fontsize=14 )
                    ax.bar( df.index.values, df['n'].values, width=1.0, align='edge' )
                    ax.tick_params( 'both', labelsize=12 )
                _logger.info( f"Writing {str( self.outdir / f'{brokername}.svg' )}" )
                fig.savefig( self.outdir / f"{brokername}.svg" )
                pyplot.close( fig )

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
            
