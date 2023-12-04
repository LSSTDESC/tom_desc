import sys
import pathlib
import shutil
import logging

import psycopg2
import psycopg2.extras
import numpy
import pandas

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
    help = "Generate files with summaries of confusion vs. t"
    destdir = _rundir / "../../summary_data/confusionvst"
    outdir = destdir.parent / f"{destdir.name}_working"

    def handle( self, *args, **options ):
        _logger.info( "Starting gen_elasticc2_confusionvst" )

        maxrelday = 90
        minrelday = -20

        conn = None
        # Jump through hoops to get the psycopg2 connection from django
        conn = django.db.connection.cursor().connection

        try:
            self.outdir.mkdir( parents=True, exist_ok=True )
            self.destdir.mkdir( parents=True, exist_ok=True )

            for p in self.outdir.iterdir():
                p.unlink()

            with conn.cursor() as cursor:
            # Figure out which brokers we have
                fields = [ 'classifier_id', 'brokername', 'brokerversion', 'classifiername', 'classifierparams' ]
                cursor.execute( f"SELECT {','.join(fields)} "
                                f"FROM elasticc2_brokerclassifier "
                                f"ORDER BY brokername,brokerversion,classifiername,classifierparams" )
                rows = cursor.fetchall()
                self.brokers = [ { i: j for i, j in zip(fields, row) } for row in rows ]

                # Figure out which truetypes we have
                cursor.execute( "SELECT gentype,classid,description,exactmatch "
                                "FROM elasticc2_classidofgentype "
                                "ORDER BY classid,gentype" )
                rows = cursor.fetchall()
                self.classes = {}
                for row in rows:
                    gentype, classid, description, exactmatch = row
                    if classid not in self.classes:
                        self.classes[classid] = { 'gentypes': [],
                                                  'description': description,
                                                  'exactmatch': exactmatch }
                    thisclass = self.classes[classid]
                    if ( thisclass['description'] != description ) or ( thisclass['exactmatch'] != exactmatch ):
                        _logger.warning( f"Classid {classid} has inconsistent description and/or exactmatch" )
                    thisclass['gentypes'].append( gentype )


                # ****
                # self.brokers = [ b for b in self.brokers if b['classifier_id']==12 ]
                # ****

                for broker in self.brokers:
                    brokerid = broker['classifier_id']
                    _logger.info( f"Starting broker {brokerid}: {broker['brokername']} {broker['brokerversion']} "
                                  f"{broker['classifiername']} {broker['classifierparams']}" )
                    for thisclassid, thisclass in self.classes.items():
                        if not thisclass['exactmatch']:
                            continue
                        _logger.info( f"Starting class id {thisclassid} for broker {brokerid}" )

                        fields = [ 's.midpointtai', 's.filtername', 's.psflux', 's.snr',
                                   's.diaobject_id', 's.diasource_id',
                                   'm.classid', 'm.probability', 'm.descingesttimestamp', 'm.alert_id',
                                   't.zcmb','t.peakmjd', 't.gentype' ]
                        _logger.debug( "....sending query" )
                        cursor.execute( f"SELECT {','.join(fields)} "
                                        f"FROM elasticc2_diaobjecttruth t "
                                        f"INNER JOIN elasticc2_ppdbdiasource s ON t.diaobject_id=s.diaobject_id "
                                        f"INNER JOIN elasticc2_brokermessage m ON s.diasource_id=m.diasource_id "
                                        f"WHERE m.classifier_id=%(cls)s AND t.gentype IN %(gentypes)s",
                                        { 'cls': brokerid, 'gentypes': tuple( thisclass['gentypes'] ) } )
                        _logger.debug( "....pulling data" )
                        rows = cursor.fetchall()
                        _logger.debug( "....pandfying" )
                        df = pandas.DataFrame( rows, columns=fields )

                        _logger.debug( "....processing" )
                        # Filter out duplicates, keeping the most recently received broker message
                        df = ( df.sort_values( ['s.diaobject_id', 's.midpointtai', 'm.descingesttimestamp'] )
                               .reset_index()
                               .groupby( [ 's.diaobject_id', 's.diasource_id' ] ).agg( 'last' )
                               .reset_index() )
                        df[ 'deltat' ] = df[ 's.midpointtai' ] - df[ 't.peakmjd' ]
                        df[ 'relday' ] = df[ 'deltat' ].apply( numpy.floor ).astype( numpy.int16 )

                        # Just keep things within the desired relday range
                        df = df[ ( df[ 'relday' ] >= minrelday ) & ( df[ 'relday' ] <= maxrelday ) ]

                        # Get a dataframe with just object info
                        objdf = ( df.loc[:, ['s.diaobject_id', 't.zcmb', 't.peakmjd', 't.gentype'] ]
                                  .groupby( 's.diaobject_id' ).agg( 'first' ) )

                        # Get a dataframe with just source info
                        srcdf = ( df.loc[:, [ 's.diaobject_id', 's.diasource_id', 's.midpointtai', 'deltat',
                                              'relday', 's.filtername', 's.psflux','s.snr' ] ]
                                  .groupby( 's.diasource_id' ).agg( 'first' ) )

                        # Make the classification df
                        cifydf = ( df.loc[ :, [ 's.diasource_id', 'm.classid', 'relday', 'm.probability' ] ]
                                   .explode( [ 'm.classid', 'm.probability' ] )
                                   .set_index( [ 's.diasource_id', 'm.classid' ] ) )

                        # Make the "mean class probability" df
                        classprobdf = ( cifydf[ [ 'relday', 'm.classid', 'm.probability' ] ]
                                        .groupby( [ 'relday', 'm.classid' ] ).agg( numpy.average ) )

                        # Make the "max probability" df
                        maxprobdf = ( cifydf[ [ 'relday', 'm.probability', 'm.classid' ] ]
                                      .sort_values( [ 'relday', 'm.probability' ] )
                                      .groupby( 'relday' )
                                      .agg( 'last' )[ 'm.classid' ] )

                        # Save all the pickels

                        _logger.debug( "....pickling" )
                        for ofname, df in zip( [ 'objdf.pkl', 'srcdf.pkl', 'cifydf.pkl',
                                                 'classprobdf.pkl', 'maxprobdf.pkl' ],
                                               [ objdf, srcdf, cifydf, classprobdf, maxprobdf ] ):
                            df.to_pickle( self.outdir / f'{brokerid}_{thisclassid}_{ofname}' )

            for p in self.destdir.iterdir():
                p.unlink()
            for p in self.outdir.iterdir():
                shutil.move( p, self.destdir / p.name )

        except Exception as ex:
            _logger.exception( ex )
            raise ex


