import sys
import pathlib
import datetime
import logging
import psycopg2
import psycopg2.extras
import django.db
from django.core.management.base import BaseCommand, CommandError

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
    help = 'Generate Configuration Matrices in the static directory'
    outdir = ( _rundir / "../../static/elasticc/confmatrices/" ).resolve()
    templdir = ( _rundir / "../../templates/elasticc/" ).resolve()
    
    def add_arguments( self, parser ):
        parser.add_argument('--norm', default='true', choices=['true', 'pred', 'all'],
                            help='how to normalize confusion matrices')
        parser.add_argument('--definition', default='last_best', choices=['last_best', 'best'],
                            help=( 'definition of classification: '
                                   '"best" means having the largest probability over all classifier messages, '
                                   '"last_best" means having the largest probability for the most recent alert ' ) )
        
    def handle( self, *args, **options ):
        self.outdir.mkdir( parents=True, exist_ok=True )

        # Jump through hoops to get access to the psycopg2 connection from django
        conn = django.db.connection.cursor().connection
        client = ConfMatrixClient( conn )
        _logger.info( "===== Classifiers =====" )
        for cid,c in client.classifiers.items():
            _logger.info( f"Logger {cid}: {c}" )
        _logger.info( "=======================" )

        with open( self.templdir / "confmatrix_update.txt" , "w" ) as ofp:
            ofp.write( datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M UTC' ) )
        
        for cid in client.classifiers.keys():
            matrix = client.get_classifications( classifier_id=cid, definition=options['definition'] )
            client.plot_matrix( matrix[cid], extension="svg", norm=options['norm'],
                                plotdir=self.outdir, namebyid=True )
            
        
