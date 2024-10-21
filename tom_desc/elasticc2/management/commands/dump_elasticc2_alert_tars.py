import sys
import io
import time
import pathlib
import tarfile
import logging
import multiprocessing

import django.db
from django.core.management.base import BaseCommand, CommandError
from django_extensions.management.signals import post_command
import elasticc2.models

_rundir = pathlib.Path(__file__).parent
sys.path.insert(0, str(_rundir) )
from _alertreconstructor import AlertReconstructor

class Command(BaseCommand):
    help="Dump ELAsTiCC2 alerts to tar files"

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )
        self.logger = logging.getLogger( "dump_elasticc2_alerts_tars" )
        self.logger.propagate = False
        if not self.logger.hasHandlers():
            logout = logging.StreamHandler( sys.stderr )
            self.logger.addHandler( logout )
            formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                           datefmt='%Y-%m-%d %H:%M:%S' )
            logout.setFormatter( formatter )
        # self.logger.setLevel( logging.INFO )
        self.logger.setLevel( logging.DEBUG )

    def add_arguments( self, parser ):
        parser.add_argument( '--start-day', type=float, default=None, required=True,
                             help=( "Start with alerts at this midPointTai.  You probably want this to "
                                    "be an integer value." ) )
        parser.add_argument( '-d', '--through-day', type=float, default=None, required=True,
                             help=( "Run through this day (i.e. anything with midPointTai < thorugh_day+1). "
                                    "You probably want this to be an integer value." ) )
        parser.add_argument( '-s', '--alert-schema', default=f'{_rundir}/elasticc.v0_9_1.alert.avsc',
                             help='File with AVRO schema' )
        parser.add_argument( '-l', '--log-every', default=1000, type=int,
                             help="Log alerts saved at this interval; 0=don't log" )
        parser.add_argument( '-n', '--num-reconstruct-processes', default=6, type=int,
                             help="Run this many alert reconstruction subprocesses (default 6)" )
        parser.add_argument( '-o', '--outdir', default='.', help="Directory to write output files." )
        parser.add_argument( '-g', '--gentype', type=int, default=None,
                             help="Only write objects with this gentype (default: write all)" )
        parser.add_argument( '-c', '--clobber', action='store_true', default=False,
                             help="Clobber existing tar files?  (Default: existing files raise an exception.)" )
        parser.add_argument( '--do', action='store_true', default=False,
                             help="Actually do it (otherwise, it's a dry run)" )


    def handle( self, *args, **options ):
        if options['gentype'] is not None:
            raise NotImplementedError( "--gentype option not implemented" )

        now = time.time()

        # Launch the alert reconstructor processes

        # Need to make sure that each subprocesses gets its own
        # database connection.  To that end, close the django
        # connections so that there aren't any cached ones.
        django.db.connections.close_all()

        def launchReconstructor( pipe ):
            reconstructor = AlertReconstructor( self, pipe, options['alert_schema'] )
            reconstructor.go()

        procinfo = {}
        freeprocs = set()
        busyprocs = set()
        self.logger.info( f'Launching {options["num_reconstruct_processes"]} alert reconstruction subprocesses.' )
        for i in range( options['num_reconstruct_processes'] ):
            parentconn, childconn = multiprocessing.Pipe()
            proc = multiprocessing.Process( target=lambda: launchReconstructor( childconn ), daemon=True )
            proc.start()
            procinfo[ proc.pid ] = { 'proc': proc,
                                     'parentconn': parentconn,
                                     'childconn': childconn }
            freeprocs.add( proc.pid )

        outdir = pathlib.Path( options['outdir'] )
        outdir.mkdir( exist_ok=True, parents=True )
        if not outdir.is_dir():
            raise RuntimeError( "Failed to find or create directory {outdir.resolve()}" )

        for mjd in range( int(options['start_day']), int(options['through_day'])+1 ):
            alerts = ( elasticc2.models.PPDBAlert.objects
                       .filter( diasource__midpointtai__gte=mjd )
                       .filter( diasource__midpointtai__lt=mjd+1 )
                       .order_by( 'diasource__midpointtai' ) )
            if len(alerts) == 0:
                continue

            tarpath = outdir / f'{mjd}.tar'
            tarobj = None

            if options['do']:
                if tarpath.exists():
                    if options['clobber']:
                        tarpath.unlink()
                    else:
                        raise FileExistsError( f"Can't create {tarpath.resolve()}, it already exists." )
                self.logger.info( f"Dumping {alerts.count()} alerts for MJD {mjd} to {tarpath}" )
                tarobj = tarfile.TarFile( tarpath, mode='w' )
            else:
                self.logger.info( f"Simuilating dumping {alerts.count()} alerts for MJD {mjd} to {tarpath} "
                                  f"(but not really writing anything)." )


            alerts = alerts.all()

            alertdex = 0
            nextlog = 0
            donealerts = set()
            while ( alertdex < len(alerts) ) or ( len(busyprocs) > 0 ):
                if ( options['log_every'] > 0 ) and ( alertdex >= nextlog ):
                    self.logger.info( f"Have saved {alertdex} of {len(alerts)} for MJD {mjd}" )
                nextlog += options['log_every']

                # Submit alerts to any free process
                while ( alertdex < len(alerts) ) and ( len(freeprocs) > 0 ):
                    pid = freeprocs.pop()
                    busyprocs.add( pid )
                    procinfo[pid]['parentconn'].send( { 'command': 'do',
                                                        'alertdex': alertdex,
                                                        'alert': alerts[alertdex] } )
                    alertdex += 1

                # Check for response from busy procsses
                doneprocs = set()
                for pid in busyprocs:
                    if not procinfo[pid]['parentconn'].poll():
                        continue
                    doneprocs.add( pid )

                    msg = procinfo[pid]['parentconn'].recv()
                    if ( 'response' not in msg ) or ( msg['response'] != 'alert produced' ):
                        raise ValueError( f"Unexpected response from child process: {msg}" )

                    alertid = msg['alertid']
                    respalertdex = msg['alertdex']
                    if alertid in donealerts:
                        raise RuntimeError( f'{msg["alertid"]} got processed more than once' )
                    donealerts.add( alertid )

                    if options['do']:
                        arcname = f'{alertid}.avro'
                        with io.BytesIO( msg['fullhistory'] ) as bio:
                            ti = tarfile.TarInfo( name=arcname )
                            ti.size = bio.getbuffer().nbytes
                            ti.mtime = now
                            ti.mode = 0o664
                            tarobj.addfile( ti, bio )

                for pid in doneprocs:
                    busyprocs.remove( pid )
                    freeprocs.add( pid )

            tarobj.close()

        self.logger.info( "All done." )

