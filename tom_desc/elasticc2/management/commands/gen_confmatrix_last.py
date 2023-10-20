import sys
import pathlib
import logging
import datetime
import traceback

import numpy
import psycopg2
import psycopg2.extras
import pandas

from matplotlib import pyplot
import sklearn.metrics
import seaborn

import django.db
from django.core.management.base import BaseCommand, CommandError

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
    help = "Generate confusion matrices using latest classification from each classifier"
    outdir = ( _rundir / "../../static/elasticc2/confmatrix_lastclass" ).resolve()

    @numpy.vectorize
    def conf_annotation( count, fraction ):
        percent = numpy.round( fraction * 100 )
        if count < 1_000_000:
            count_str = str( count )
        else:
            count_str = f'{count:.3g}'
        return f'{percent}%\n{count_str}'
    
    def handle( self, *args, **options ):
        _logger.info( "Starting gen_confmatrix_last" )

        if not self.outdir.is_dir():
            if self.outdir.exists():
                raise FileExistsError( f"{self.outdir} exists but is not a directory!" )
            self.outdir.mkdir( parents=True, exist_ok=True )

        # if options['wipe']:
        #     _logger.info( f"Wiping out {self.outdir}" )
        #     for f in self.outdir.glob( '*.svg' ):
        #         f.unlink()
        #     f = self.outdir / "updatetime.txt"
        #     if f.exists():
        #         f.unlink()

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

            # Figure out true types

            cursor.execute( "SELECT classid,gentype,description FROM elasticc2_gentypeofclassid "
                            "ORDER BY classid" )
            tmp = pandas.DataFrame( cursor.fetchall() )
            
            nogentypeclasses = tmp[ tmp['gentype'].isnull() ].set_index( 'classid' )
            gentypeclasses = tmp[ ~tmp['gentype'].isnull() ]
            trueclasses = pandas.DataFrame( gentypeclasses.groupby( 'classid' ).agg( list )[ 'gentype' ] )
            tmp = pandas.DataFrame( gentypeclasses.groupby( 'classid' ).first() )
            trueclasses[ 'description' ] = tmp[ 'description' ]
            trueclasses = pandas.concat( [ trueclasses, nogentypeclasses ] )
            trueclasses.sort_values( 'classid', inplace=True )

            # Iterate over classifiers, and then within classifiers, by truetype

            # ****
            # cfers = { x: y for x, y in cfers.items() if x == 8 }
            # ****

            for cferid, cfer in cfers.items():
                brokerdesc = ( f'{cfer["brokername"]} {cfer["brokerversion"]} '
                               f'{cfer["classifiername"]} {cfer["classifierparams"]}' )

                massivedf = None
                
                for trueclass in trueclasses.index.values:
                    if not isinstance( trueclasses.loc[ trueclass, 'gentype' ], list ):
                        continue
                    _logger.info( f"Doing true type {trueclasses.loc[trueclass,'description']} for {brokerdesc}" )
                    
                    q = ( "SELECT DISTINCT ON ( a.diaobject_id ) "
                          "  a.diaobject_id,m.classid,m.probability "
                          "FROM elasticc2_ppdbalert a "
                          "INNER JOIN elasticc2_diaobjecttruth ot ON a.diaobject_id=ot.diaobject_id "
                          "INNER JOIN elasticc2_brokermessage m ON a.alert_id=m.alert_id "
                          "INNER JOIN elasticc2_ppdbdiasource s ON a.diasource_id=s.diasource_id "
                          "WHERE a.alertsenttimestamp IS NOT NULL "
                          "  AND ot.gentype IN %(gentypes)s "
                          "  AND m.classifier_id=%(cfer)s "
                          "ORDER BY a.diaobject_id, s.midpointtai DESC" )
                    cursor.execute( q, { 'gentypes': tuple( [ int(i) for i in
                                                              trueclasses.loc[ trueclass, 'gentype' ] ] ),
                                         'cfer': cferid } )
                    _logger.info( "...query run,fetching." )
                    # There's probably a python performance hit here, as psycopg2 wll
                    # make lists, and then we have to parse those lists back
                    # into a numeric array
                    tmpdf = pandas.DataFrame(
                        { 'diaobject_id': row['diaobject_id'],
                          'classid': numpy.array( row['classid'] ),
                          'probability': numpy.array( row['probability'] )
                         }
                        for row in cursor.fetchall() )
                    _logger.info( "...dataframe fetched." )
                    if len(tmpdf) == 0:
                        continue

                    # Extract the class with the highest probability for each row
                    tmpdf[ 'classid' ] = tmpdf.apply( lambda row: row['classid'][ row['probability'].argmax() ],
                                                      axis=1 )
                    tmpdf.drop( 'probability', axis=1 )
                    tmpdf[ 'class' ] = tmpdf[ 'classid' ].apply( lambda x : trueclasses.loc[ x, 'description' ] )
                    tmpdf.drop( 'classid', axis=1 )
                    tmpdf[ 'trueclass' ] = trueclasses.loc[ trueclass, 'description' ]
                    
                    if massivedf is None:
                        massivedf = tmpdf
                    else:
                        massivedf = pandas.concat( [ massivedf, tmpdf ], ignore_index=True )

                _logger.info( f"Building confidence matrix for {brokerdesc}" )
                countmatrix = sklearn.metrics.confusion_matrix( massivedf['trueclass'], massivedf['class'],
                                                                labels=trueclasses.description )


                # Filter out empty rows and columns
                rowkeeps = []
                colkeeps = []
                for i in range( len( trueclasses.description ) ):
                    # y is row, x is column, but array is indexed row, column
                    if not ( countmatrix[i, :] == 0 ).all():
                        rowkeeps.append( i )
                    if not ( countmatrix[:, i] == 0 ).all():
                        colkeeps.append( i )
                # This numpy magic is... obscure.  I hope I did it right.
                rowkeeps = numpy.array( rowkeeps )
                colkeeps = numpy.array( colkeeps )
                countmatrix = countmatrix[ rowkeeps[:, numpy.newaxis], colkeeps[numpy.newaxis, :] ]
                xlabels = trueclasses.description.values[ colkeeps ]
                ylabels = trueclasses.description.values[ rowkeeps ]
                    
                # Normalize countmatrix by true type
                # (Do axis=1 to normalize by predicted type)
                fracmatrix = countmatrix / countmatrix.sum( axis=0 )

                annotations = self.conf_annotation( countmatrix, fracmatrix )

                fig = pyplot.figure( figsize=(10, 10), tight_layout=True )
                ax = fig.add_subplot( 1, 1, 1 )
                seaborn.heatmap( fracmatrix, ax=ax, cmap='Blues', vmin=0, vmax=1,
                                 annot=annotations, fmt='s', annot_kws={"fontsize": 6},
                                 xticklabels=xlabels, yticklabels=ylabels )
                ax.set_xlabel( "Predicted Class", fontsize=14 )
                ax.set_ylabel( "True Class", fontsize=14 )
                ax.set_title( f"{brokerdesc}\nBased on latest source classified", fontsize=18 )
                fig.savefig( self.outdir / f'{cferid}.svg' )
                pyplot.close( fig )
                _logger.info( f"Done with {brokerdesc}" )
                
        except Exception as e:
            _logger.error( traceback.format_exc() )
            import pdb; pdb.set_trace()
            pass
        


                    
            
