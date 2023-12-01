import sys
import pathlib
import logging

import psycopg2
import psycopg2.extras
import django.db

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
    destdir = _rundir / "../../static/elasticc2/confusionvst"
    outdir = destdir.parent / f"{destdir.name}_working"

    def handle( self, *args, **options ):
        _logger.info( "Starting gen_elasticc2_confusionvst" )

        conn = None
        # Jump through hoops to get the psycopg2 connection from django
        conn = django.db.connection.cursor().connection

        try:
            self.outdir.mkdir( parents=True, exist_ok=True )
            self.destdir.mkdir( parents=True, exist_ok=True )

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
                if classid not in classes:
                    self.classes[classid] = { 'gentypes': [], 'description': description, 'exactmatch': exactmatch }
                thisclass = self.classes[classid]
                if ( thisclass['description'] != description ) or ( thisclass['exactmatch'] != exactmatch ):
                    _logger.warning( f"Classid {classid} has inconsistent description and/or exactmatch" )
                thisclass['gentypes'].append( gentype )

            for broker in self.brokers:
                brokerid = broker['classifier_id']
                _logger.info( f"Starting broker {brokerid}: {broker['brokername']} {broker['brokerversion']} "
                              f"{broker['classifiername']} {broker['classifierparams']}" )
                for thisclass in self.classes:
                    if not thisclass.exactmatch:
                        continue
                    _logger.info( f"Starting class id {thisclass['classid']} for broker {brokerid}" )

                    fields = [ 's.midpointtai','s.filtername','s.flux','s.snr',
                               'm.diasource_id','m.classid','m.probability','t.zcmb','t.peakmjd' ]
                    _logger.debug( "....sending query" )
                    cursor.execute( f"SELECT {','.join(fields) "
                                    f"FROM elasticc2_diaobjecttruth t "
                                    f"INNER JOIN elasticc2_ppdbdiasource s ON t.diaobject_id=s.diaobject_id "
                                    f"INNER JOIN elasticc2_brokermessage m ON s.diasource_id=m.diasource_id "
                                    f"WHERE m.classifier_id=%(cls)s AND t.gentype IN %(gentypes)s",
                                    { 'cls': thisclass['classid'], 'gentypes': tuple( thisclass['gentypes'] ) } )
                    _logger.debug( "....pulling data" )
                    rows = cursor.fetchall()
                    
                    
                    
            
