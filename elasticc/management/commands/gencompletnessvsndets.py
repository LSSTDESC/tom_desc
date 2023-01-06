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
    outdir = _rundir / "../../static/elasticc/brokercompletenessvsndets"

    def add_arguments( self, parser) :
        pass

    def handle( self, *args, **options ):
        _logger.info( "Starting gencompletenessvsnedets" )
        
        self.outdir.mkdir( parents=True, exist_ok=True )
        conn = None

        ndetcutoff = 20
        ndetlabelspace = 4

        try:
            # Jump through hoops to get access to the psycopg2 connection from django
            conn = django.db.connection.cursor().connection

            with conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
                cursor.execute( 'SELECT * FROM elasticc_brokerclassifier '
                                'ORDER BY "brokerName","brokerVersion","classifierName","classifierParams"' )
                brokers = { row["classifierId"] : row for row in cursor.fetchall() }
                whichbrokers = list( brokers.keys() )

                # Get denominator

                _logger.info( "Getting total number of alerts sent" )
                totdata = []
                q = """SELECT psc.ndetections,COUNT(psc."diaSourceId") AS n
                       FROM elasticc_view_prevsourcecounts psc
                       INNER JOIN elasticc_diaalert a
                         ON psc."diaSourceId"=a."diaSourceId"
                       WHERE a."alertSentTimestamp" IS NOT NULL
                       GROUP BY psc.ndetections
                       ORDER BY psc.ndetections;
                    """
                cursor.execute( q )
                for row in cursor.fetchall():
                    totdata.append( row )
                totdata = pandas.DataFrame( totdata ).set_index( 'ndetections' )

                # Sum ndets>=20

                tmp = totdata.reset_index()
                lots = tmp[ tmp.ndetections >= ndetcutoff ]['n'].sum()
                totdata = totdata.reset_index()
                totdata = totdata[ totdata.ndetections<ndetcutoff ].set_index( 'ndetections' )
                totdata = pandas.concat( [ totdata, pandas.DataFrame( [ { 'ndetections': ndetcutoff,
                                                                          'n': lots } ] ).set_index('ndetections') ] )

                # Get broker info
                
                for brokerid in whichbrokers:
                    brokerdata = []
                    broker = brokers[brokerid]
                    _logger.info( f"Querying for {broker['brokerName']} {broker['brokerVersion']} "
                                  f"{broker['classifierName']} {broker['classifierParams']}" )
                    q = """SELECT ndetections,COUNT("diaSourceId") AS n
                           FROM
                           ( SELECT DISTINCT ON (spc."diaSourceId")
                             spc."diaSourceId",spc.ndetections
                             FROM elasticc_view_prevsourcecounts spc
                             INNER JOIN elasticc_brokermessage m ON spc."diaSourceId"=m."diaSourceId"
                             INNER JOIN elasticc_brokerclassification c ON m."brokerMessageId"=c."brokerMessageId"
                             WHERE c."classifierId"=%(brokerid)s
                             ORDER BY spc."diaSourceId"
                           ) subq
                           GROUP BY ndetections
                           ORDER BY ndetections
                        """
                    cursor.execute( q, { "brokerid": brokerid } )
                    brokerdata = pandas.DataFrame( cursor.fetchall() ).set_index( 'ndetections' )

                    # Sum all ndets>=20
                    # (Is there a more direct Pandas way to do this?)
                
                    tmp = brokerdata.reset_index()
                    lots = tmp[ tmp.ndetections >= ndetcutoff ]['n'].sum()
                    brokerdata = brokerdata.reset_index()
                    brokerdata = brokerdata[ brokerdata.ndetections<ndetcutoff ].set_index( 'ndetections' )
                    brokerdata = pandas.concat( [ brokerdata, ( pandas.DataFrame( [ { 'ndetections': ndetcutoff,
                                                                                    'n': lots } ] )
                                                                .set_index('ndetections') ) ] )

                    xticks = numpy.arange( 0, ndetcutoff+ndetlabelspace, ndetlabelspace )
                    xlabels = [ str(x) for x in xticks ]
                    xlabels[-1] = f"{ndetcutoff}+"
                    frac = brokerdata / totdata
                    fig = pyplot.figure( figsize=(12,6), tight_layout=True )
                    ax = fig.add_subplot( 1, 1, 1 )
                    ax.set_title( f"{broker['brokerName']} {broker['brokerVersion']} {broker['classifierName']}",
                                  fontsize=18 )
                    ax.set_xlabel( "N. Detections in Alert", fontsize=16 )
                    ax.set_ylabel( "Frac. of alerts classified", fontsize=16 )
                    ax.bar( frac.index.values, frac.n, align='center', width=0.9 )
                    ax.set_xlim( 0, ndetcutoff+1  )
                    ax.set_ylim( 0, 1.1 )
                    ax.axhline( 1.0, color='black', linestyle='--' )
                    ax.set_yticks( [ 0, 0.2, 0.4, 0.6, 0.8, 1.0 ] )
                    ax.set_xticks( xticks )
                    ax.set_xticklabels( xlabels )
                    ax.tick_params( 'both', labelsize=14 )
                    _logger.info( f"Writing {str( self.outdir / f'{brokerid}.svg' )}" )
                    fig.savefig( self.outdir / f"{brokerid}.svg" )
                    pyplot.close( fig )
            
            _logger.info( "Done with gencompletenessvsndets." )

        except Exception as e:
            _logger.exception( e )
            raise e
        finally:
            if conn is not None:
                conn.close()
                conn = None
            
