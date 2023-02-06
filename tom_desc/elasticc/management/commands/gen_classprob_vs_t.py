import sys
import math
import pathlib
import datetime
import logging
import psycopg2
import psycopg2.extras
from pprint import pformat
import django.db
from django.core.management.base import BaseCommand, CommandError
import pandas
import numpy
import matplotlib
import seaborn

_rundir = pathlib.Path(__file__).parent
sys.path.insert(0, str(_rundir) )
from _confmatrix import ConfMatrixClient

_logger = logging.getLogger( __name__ )
_logger.propagate = False
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.INFO )

class Command(BaseCommand):
    help = 'Generate av. probability as a function of class and time'
    outdir = ( _rundir / "../../static/elasticc/probmetrics/" ).resolve()

    def mogrify( self, query, args={} ):
        with self.dbconn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
            return cursor.mogrify( query, args ).decode( 'UTF-8' )
    
    def query( self, query, args={} ):
        with self.dbconn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
            cursor.execute( query, args )
            return list( cursor.fetchall() )

    def handle( self, *args, **options ):
        self.outdir.mkdir( exist_ok=True, parents=True )

        self.dbconn = django.db.connection.cursor().connection
        
        query = f'''
           SELECT * FROM elasticc_brokerclassifier
           ORDER BY "brokerName", "brokerVersion", "classifierName", "classifierId"
        '''
        rows = self.query( query )
        # _logger.debug( pformat(rows) )

        classifiers = { row['classifierId']: f'{row["brokerName"]} {row["brokerVersion"]} {row["classifierName"]}'
                        for row in rows
                      }
        # For debugging, crop down to just a couple of classifiers
        #                 if row['classifierId'] in (40, 44) }
        
        with open( self.outdir / "classifiers.csv", "w" ) as ofp:
            for cfer, cfername in classifiers.items():
                ofp.write( f"{cfer},\"{cfername}\"\n" )
        
        query = ( 'SELECT DISTINCT ON("classId") "classId",description '
                  'FROM elasticc_gentypeofclassid '
                  'ORDER BY "classId"' )
        rows = self.query( query )
        classname = { row["classId"]: row["description"] for row in rows }
        with open( self.outdir / "classnames.csv", "w" ) as ofp:
            for cls, clsnm in classname.items():
                ofp.write( f"{cls},\"{clsnm}\"\n" )

        # For debugging, limit the number of things we get back
        # limit = 10000
        limit = None
        bucket0 = -30.
        bucket1 = 100.
        bucketw = 5
        nbuckets = ( bucket1 - bucket0 ) / bucketw
        if nbuckets != math.floor( nbuckets ):
            raise ValueError( "nbuckets isn't a integer" )
        nbuckets = int( nbuckets )
        
        for cfer, cfername in classifiers.items():
            _logger.info( f"Running query for {cfername}" )
            
            query = f'''
              SELECT "classId","trueClassId",deltatbin,AVG(probability) as probability 
              FROM (
                SELECT v."diaObjectId",v."classId",v."alertId",v."trueClassId",v.probability,
                  width_bucket( s."midPointTai"-ot.peakmjd, %(bucket0)s, %(bucket1)s, %(nbuckets)s ) AS deltatbin
                FROM elasticc_view_sourceclassifications v 
                INNER JOIN elasticc_diasource s ON v."diaSourceId"=s."diaSourceId" 
                INNER JOIN elasticc_diaobjecttruth ot ON v."diaObjectId"=ot."diaObjectId"
                WHERE v."classifierId"=%(cfer)s
                ORDER BY v."diaObjectId" {f"LIMIT {limit}" if limit is not None else ""}
              ) subq
              GROUP BY "classId","trueClassId",deltatbin
            '''
            args = { 'cfer': cfer, 'bucket0': bucket0, 'bucket1': bucket1, 'nbuckets': nbuckets }
            # _logger.warn( self.mogrify( query, args ) )
            rows = self.query( query, args )

            _logger.info( f"...query done for {cfername}" )

            data = pandas.DataFrame( rows ).set_index( [ 'trueClassId', 'classId' ] )
            data['deltatround'] = data['deltatbin'].apply( lambda x: (x-0.5) * bucketw + bucket0 )
            data.drop( 'deltatbin', axis=1, inplace=True )

            for trueclass in data.index.get_level_values( 'trueClassId' ).unique().values:
                ocvs = self.outdir / f"{cfer}_{trueclass}.cvs"
                osvg = self.outdir / f"{cfer}_{trueclass}.svg"
                trueclassname = classname[ trueclass ]
                trueclassdf = data.xs( trueclass, level='trueClassId' ).reset_index()
                trueclassdf['class'] = trueclassdf['classId'].apply( lambda i: classname[i] )
                trueclassdf = trueclassdf[ ( trueclassdf['deltatround'] >= bucket0 ) &
                                           ( trueclassdf['deltatround'] <= bucket1 ) ]
                trueclassdf.set_index( [ 'classId', 'deltatround' ], inplace=True )
                
                # Fill in missing Δt values so that seaborn.heatmap will give us a full x-axis
                # Pandas reindex is broken with multiindexes, so we do it the long way
                # trueclassdf = trueclassdf.reindex( numpy.arange( bucket0, bucket1, bucketw ) + bucketw/2.,
                #                                    level='deltatround' )
                adds = []
                for t in numpy.arange( bucket0, bucket1, bucketw ) + bucketw/2.:
                    for cid in trueclassdf.index.get_level_values( 'classId' ).values:
                        if not (cid, t) in trueclassdf.index:
                            adds.append( { 'classId': cid, 'deltatround': t,
                                           'class': classname[cid], 'probability': 0. } )
                if len(adds) > 0:
                    adds = pandas.DataFrame( adds ).set_index( [ 'classId', 'deltatround' ] )
                    trueclassdf = pandas.concat( [ trueclassdf, adds ] )

                trueclassdf.to_csv( ocvs )

                fig = matplotlib.pyplot.figure( figsize=(6, 6), tight_layout=True )
                fig.suptitle( cfername )
                ax = fig.add_subplot( 1, 1, 1 )
                pt = pandas.pivot_table( trueclassdf, values='probability', columns='deltatround', index='class' )
                seaborn.heatmap( pt, vmin=0, vmax=1, ax=ax )
                ax.set_title( f"True class {trueclass} ({trueclassname})" )
                ax.set_xlabel( "Δt (days)" )
                ax.set_ylabel( "broker class" )
                fig.savefig( osvg )
                matplotlib.pyplot.close( fig )

            _logger.info( f"...done making plots for {cfername}" )
