import sys
import os
import io
import re
import time
import datetime
import uuid
import pathlib
import logging
import traceback
import collections
import multiprocessing
import numpy
import psycopg2.extras
from astropy.table import Table
import astropy.io
import django.db

# _rundir = pathlib.Path( __file__ ).parent

from django.core.management.base import BaseCommand, CommandError

from fastdb_dev.models import (
    ProcessingVersions,
    HostGalaxy,
    Snapshots,
    DiaObject,
    SnapshotTags,
    LastUpdateTime,
    DiaSource,
    DiaForcedSource,
    DStoPVtoSS,
    DFStoPVtoSS,
)

# # https://stackoverflow.com/questions/4716533/how-to-attach-debugger-to-a-python-subproccess
# import pdb
# class ForkablePdb(pdb.Pdb):

#     _original_stdin_fd = sys.stdin.fileno()
#     _original_stdin = None

#     def __init__(self):
#         pdb.Pdb.__init__(self, nosigint=True)

#     def _cmdloop(self):
#         current_stdin = sys.stdin
#         try:
#             if not self._original_stdin:
#                 self._original_stdin = os.fdopen(self._original_stdin_fd)
#             sys.stdin = self._original_stdin
#             self.cmdloop()
#         finally:
#             sys.stdin = current_stdin


class ColumnMapper:
    @classmethod
    def diaobject_map_columns( cls, tab ):
        """Map from the HEAD.FITS.gz files to the diaobject table"""
        mapper = { 'SNID': 'dia_object',
                   'IAUC': 'dia_object_iau_name',
                   'DEC': 'decl',
                  }
        lcs = { 'RA' }

        cls._map_columns( tab, mapper, lcs )

    @classmethod
    def diasource_map_columns( cls, tab ):
        """Map from the PHOT.FITS.gz files to the diasource table"""
        mapper = { 'MJD': 'mid_point_tai',
                   'BAND': 'filter_name',
                   'FLUXCAL': 'ps_flux',
                   'FLUXCALERR': 'ps_flux_err',
                   # This next one isn't part of the table, but I need it for further processing
                   'PHOTFLAG': 'photflag'
                  }
        lcs = {}
        cls._map_columns( tab, mapper, lcs )

    @classmethod
    def _map_columns( cls, tab, mapper, lcs ):
        yanks = []
        renames = {}
        for col in tab.columns:
            if col in mapper:
                renames[ col ] = mapper[ col ]
            elif col in lcs:
                renames[ col ] = col.lower()
            else:
                yanks.append( col )
                next

        for oldname, newname in renames.items():
            tab.rename_column( oldname, newname )

        for yank in yanks:
            tab.remove_column( yank )

# ----------------------------------------------------------------------

class FITSFileHandler( ColumnMapper ):
    def __init__( self, parent, pipe ):
        self.pipe = pipe

        # Copy settings from parent
        for attr in [ 'max_sources_per_object', 'photflag_detect', 
                      'snana_zeropoint', 'alert_zeropoint', 't0',
                      'processing_version', 'snapshot',
                      'really_do', 'verbose' ]:
            setattr( self, attr, getattr( parent, attr ) )

        self.logger = logging.getLogger( f"logger {os.getpid()}" )
        self.logger.propagate = False
        loghandler = logging.FileHandler( f'{os.getpid()}.log' )
        self.logger.addHandler( loghandler )
        formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        loghandler.setFormatter( formatter )
        if self.verbose:
            self.logger.setLevel( logging.DEBUG )
        else:
            self.logger.setLevel( logging.INFO )

    def listener( self ):
        done = False
        while not done:
            try:
                msg = self.pipe.recv()
                if msg['command'] == 'die':
                    done = True
                elif msg['command'] == 'do':
                    retval = self.load_one_file( msg['headfile'], msg['photfile'] )
                    self.pipe.send( { 'response': 'done',
                                      'headfile': msg['headfile'],
                                      'photfile': msg['photfile'],
                                      'retval': retval
                                     }
                                   )
            except EOFError:
                done = True

    def load_one_file( self, headfile, photfile ):
        try:
            self.logger.info( f"PID {os.getpid()} reading {headfile.name}" )

            orig_head = Table.read( headfile )
            # SNID was written as a string, we need it to be a bigint
            orig_head['SNID'] = orig_head['SNID'].astype( numpy.int64 )
            head = Table( orig_head )

            if len(head) == 0:
                return { 'ok': True, 'msg': '0-length headfile' }

            phot = Table.read( photfile )

            # Load the DiaObject table

            self.diaobject_map_columns( head )
            head.add_column( False, name='isddf' )
            now = datetime.datetime.now( tz=datetime.UTC )
            head.add_column( now, name='validity_start' )
            head.add_column( 0., name='ra_sigma' )
            head.add_column( 0., name='decl_sigma' )
            head.add_column( now, name='insert_time' )
            head.add_column( now, name='update_time' )
            head.add_column( 0, name='nobs' )
            head.add_column( 0, name='fake_id' )
            head['dia_object_iau_name'] = [ None if i.strip()=='NULL' else i.strip()
                                            for i in head['dia_object_iau_name'] ]
            head.add_column( 0, name='season' )
            head.add_column( -1., name='ra_dec_tai' )
            
            # Get observation counts.
            # Figure out the season in a cheesy way; just look at the first detection,
            #   and set the season in 182.5 day gaps following the start of the survey.
            # Just set ra_dec_tai as that of the first observation.  For SNANA sims,
            #   it's gratuitous anyway
            for origrow, row in zip( orig_head, head ):
                pmin = origrow['PTROBS_MIN'] - 1
                pmax = origrow['PTROBS_MAX'] - 1
                row['nobs'] = pmax + 1 - pmin
                row['season'] = int( numpy.floor( ( phot['MJD'][pmin] - self.t0 ) / 182.5 ) )
                row['ra_dec_tai'] = phot['MJD'][pmin]

            for col in DiaObject._create_kws:
                if col not in head.columns:
                    head.add_column( None, name=col )

            if self.really_do:
                nobj = DiaObject.bulk_insert_or_upsert( dict( head ), assume_no_conflict=True )
                self.logger.info( f"PID {os.getpid()} loaded {nobj} objects from {headfile.name}" )
            else:
                nobj = len(head)
                self.logger.info( f"PID {os.getpid()} would try to load {nobj} objects" )

            # Calculate some derived fields we'll need

            phot['FLUXCAL'] *= 10 ** ( ( self.alert_zeropoint - self.snana_zeropoint ) / 2.5 )
            phot['FLUXCALERR'] *= 10 ** ( ( self.alert_zeropoint - self.snana_zeropoint ) / 2.5 )

            self.diasource_map_columns( phot )
            phot.add_column( numpy.int64(-1), name='dia_object' )
            phot.add_column( numpy.int64(-1), name='dia_forced_source' )
            phot['snr'] = phot['ps_flux'] / phot['ps_flux_err']
            phot['filter_name'] = [ i.strip() for i in phot['filter_name'] ]
            phot.add_column( uuid.uuid4(), name='uuid' )
            phot.add_column( self.processing_version, name='processing_version' )
            phot.add_column( now, name='insert_time' )
            phot.add_column( now, name='update_time' )
            phot.add_column( 0, name='season' )
            phot.add_column( 1, name='valid_flag' )
            phot.add_column( -1., name='ra' )
            phot.add_column( -100., name='decl' )
            phot.add_column( 0, name='broker_count' )
            
            # Load the DiaForcedSource table

            for obj, headrow in zip( orig_head, head ):
                # All the -1 is because the files are 1-indexed, but astropy is 0-indexed
                pmin = obj['PTROBS_MIN'] -1
                pmax = obj['PTROBS_MAX'] -1
                nobs = pmax + 1 - pmin
                if ( pmax - pmin + 1 ) > self.max_sources_per_object:
                    self.logger.error( f'SNID {obj["SNID"]} in {headfile.name} has {pmax-mpin+1} sources, '
                                       f'which is more than max_sources_per_object={self.max_sources_per_object}' )
                    raise RuntimeError( "Too many sources" )
                phot['uuid'][pmin:pmax+1] = [ str(uuid.uuid4()) for i in range(nobs) ]
                phot['dia_object'][pmin:pmax+1] = obj['SNID']
                phot['dia_forced_source'][pmin:pmax+1] = ( obj['SNID'] * self.max_sources_per_object
                                                            + numpy.arange( pmax - pmin + 1 ) )
                phot['ra'][pmin:pmax+1] = obj['RA']
                phot['decl'][pmin:pmax+1] = obj['DEC']
                        
                # Setting the season to the season of the object; this may not be the right thing!
                phot['season'][pmin:pmax+1] = headrow['season']

            # The phot table has separators, so there will still be some junk data in there I need to purge
            phot = phot[ phot['dia_object'] >= 0 ]

            for col in DiaForcedSource._create_kws:
                if col not in phot.columns:
                    phot.add_column( None, name=col )

            if self.really_do:
                nfrc = DiaForcedSource.bulk_insert_or_upsert( phot, assume_no_conflict=True )
                self.logger.info( f"PID {os.getpid()} loaded {nfrc} forced photometry points from {photfile.name}" )
            else:
                nfrc = len(phot)
                self.logger.info( f"PID {os.getpid()} would try to load {nfrc} forced photometry points" )

            # Load the dfs_to_pv_to_ss table
            dfstopvtoss = Table()
            dfstopvtoss['dia_forced_source'] = phot['dia_forced_source']
            dfstopvtoss['uuid'] = [ uuid.uuid4() for i in range(len(phot['dia_forced_source'])) ]
            dfstopvtoss.add_column( self.snapshot, name='snapshot_name' )
            dfstopvtoss.add_column( self.processing_version, name='processing_version' )
            dfstopvtoss.add_column( now, name='insert_time' )
            dfstopvtoss.add_column( now, name='update_time' )
            dfstopvtoss.add_column( 1, name='valid_flag' )

            if self.really_do:
                nfps = DFStoPVtoSS.bulk_insert_or_upsert( dfstopvtoss, assume_no_conflict=True )
                self.logger.info( f"PID {os.getpid()} loaded {nfps} DFStoPVtoSS from {photfile.name}" )
            else:
                nfps = len(dfstopvtoss)
                self.logger.info( f"PID {os.getpid()} would try to load {nfps} rows into DFStoPVtoSS" )
            
            # Load the DiaSource table

            phot.rename_column( 'dia_forced_source', 'dia_source' )
            phot = phot[ ( phot['photflag'] & self.photflag_detect ) !=0 ]

            for col in DiaSource._create_kws:
                if col not in phot.columns:
                    phot.add_column( None, name=col )

            if self.really_do:
                nsrc = DiaSource.bulk_insert_or_upsert( phot, assume_no_conflict=True )
                self.logger.info( f"PID {os.getpid()} loaded {nsrc} sources from {photfile.name}" )
            else:
                nsrc = len(phot)
                self.logger.info( f"PID {os.getpid()} would try to load {nsrc} sources" )

            # Load the ds_to_pv_to_ss table

            dstopvtoss = Table()
            dstopvtoss['dia_source'] = phot['dia_source']
            dstopvtoss['uuid'] = [ uuid.uuid4() for i in range(len(phot['dia_source'])) ]
            dstopvtoss.add_column( self.snapshot, name='snapshot_name' )
            dstopvtoss.add_column( self.processing_version, name='processing_Version' )
            dstopvtoss.add_column( now, name='insert_time' )
            dstopvtoss.add_column( now, name='update_time' )
            dstopvtoss.add_column( 1, name='valid_flag' )

            if self.really_do:
                nsps = DStoPVtoSS.bulk_insert_or_upsert( dstopvtoss, assume_no_conflict=True )
                self.logger.info( f"PID {os.getpid()} loaded {nsps} DStoPVtoSS from {photfile.name}" )
            else:
                nsps = len(dstopvtoss)
                self.logger.info( f"PID {os.getpid()} would try to load {nsps} rows into DStoPVtoSS" )
            
            return { 'ok': True, 'msg': ( f"Loaded {nobj} objects, {nsrc} sources, {nfrc} forced, "
                                          f"{nfps} dfs_to_pv_toss, {nsps} dv_to_pv_to_ss" ) }
        except Exception as e:
            self.logger.error( f"Exception loading {headfile}: {traceback.format_exc()}" )
            return { "ok": False, "msg": traceback.format_exc() }


# ----------------------------------------------------------------------

class FITSLoader( ColumnMapper ):
    def __init__( self, nprocs, directories, files=[],
                  max_sources_per_object=100000, photflag_detect=4096,
                  snana_zeropoint=27.5, alert_zeropoint=31.4, t0=60796.,
                  processing_version=None, snapshot=None,
                  really_do=False, verbose=False, logger=logging.getLogger( "load_snana_fits") ):

        self.nprocs = nprocs
        self.directories = directories
        self.files = files
        self.max_sources_per_object=max_sources_per_object
        self.photflag_detect = photflag_detect
        self.snana_zeropoint = snana_zeropoint
        self.alert_zeropoint = alert_zeropoint
        self.t0 = t0
        self.processing_version = processing_version
        self.snapshot = snapshot
        self.really_do = really_do
        self.logger = logger
        self.sublogger = None
        self.verbose = verbose

    def disable_indexes_and_fks( self ):
        # Disable all indexes and foreign keys on the relevant
        # tables to speed up loading

        tableindexes = {}
        indexreconstructs = []
        tableconstraints = {}
        constraintreconstructs = []
        tablepkconstraints = {}
        primarykeys = {}
        pkreconstructs = []
        conn = django.db.connection.cursor().connection
        cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )

        tables = [ 'processing_versions', 'host_galaxy', 'snapshots', 'snapshot_tags', 'last_update_time',
                   'dia_object', 'dia_source', 'dia_forced_source', 'ds_to_pv_to_ss', 'dfs_to_pv_to_ss' ]

        pkmatcher = re.compile( '^ *PRIMARY KEY \((.*)\) *$' )
        pkindexmatcher = re.compile( ' USING .* \((.*)\) *$' )
        for table in tables:
            tableconstraints[table] = []
            cursor.execute( f"SELECT table_name, conname, condef, contype "
                            f"FROM "
                            f"  ( SELECT conrelid::regclass::text AS table_name, conname, "
                            f"           pg_get_constraintdef(oid) AS condef, contype "
                            f"    FROM pg_constraint "
                            f"  ) subq "
                            f"WHERE table_name='{table}'" )
            rows = cursor.fetchall()
            for row in rows:
                if row['contype'] == 'p':
                    if table in primarykeys:
                        raise RuntimeError( f"{table} has multiple primary keys!" )
                    match = pkmatcher.search( row['condef'] )
                    if match is None:
                        raise RuntimeError( f"Failed to parse {row['condef']} for primary key" )
                    primarykeys[table] = match.group(1)
                    tablepkconstraints[table] = row['conname']
                    pkreconstructs.insert( 0, f"ALTER TABLE {table} ADD CONSTRAINT {row['conname']} {row['condef']};" )
                else:
                    tableconstraints[table].append( row['conname'] )
                    constraintreconstructs.insert( 0, ( f"ALTER TABLE {table} ADD CONSTRAINT {row['conname']} "
                                                        f"{row['condef']};" ) )

        # Make sure we found the primary key for all tables
        missing = []
        for table in tables:
            if table not in primarykeys:
                missing.append( table )
        if len(missing) > 0:
            raise RuntimeError( f'Failed to find primary key for: {[",".join(missing)]}' )

        # Now do table indexes
        for table in tables:
            tableindexes[table] = []
            cursor.execute( f"SELECT * FROM pg_indexes WHERE tablename='{table}'" )
            rows = cursor.fetchall()
            for row in rows:
                match = pkindexmatcher.search( row['indexdef'] )
                if match is None:
                    raise RuntimeError( f"Error parsing index def {row['indexdef']}" )
                if match.group(1) == primarykeys[table]:
                    # The primary key index will be deleted when
                    #  the primary key constraint is deleted
                    continue
                if row['indexname'] in tableconstraints[table]:
                    # It's possible the index is already present in table constraints,
                    #   as a UNIQUE constraint will also create an index.
                    continue
                tableindexes[table].append( row['indexname'] )
                indexreconstructs.insert( 0, f"{row['indexdef']};" )                
                    
        with open( "load_snana_fits_reconstruct_indexes_constraints.sql", "w" ) as ofp:
            for row in pkreconstructs:
                ofp.write( f"{row}\n" )
            for row in indexreconstructs:
                ofp.write( f"{row}\n" )
            for row in constraintreconstructs:
                ofp.write( f"{row}\n" )

        for table in tableconstraints.keys():
            self.logger.warning( f"Dropping non-pk constraints from {table}" )
            for constraint in tableconstraints[table]:
                cursor.execute( f"ALTER TABLE {table} DROP CONSTRAINT {constraint}" )

        for table in tableindexes.keys():
            self.logger.warning( f"Dropping indexes from {table}" )
            for dex in tableindexes[table]:
                cursor.execute( f"DROP INDEX {dex}" )

        for table, constraint in tablepkconstraints.items():
            self.logger.warning( f"Dropping primary key from {table}" )
            cursor.execute( f"ALTER TABLE {table} DROP CONSTRAINT {constraint}" )

        conn.commit()

    def recreate_indexes_and_fks( self, commandfile='load_snana_fits_reconstruct_indexes_constraints.sql' ):
        with open( commandfile ) as ifp:
            commands = ifp.readlines()

        conn = django.db.connection.cursor().connection
        cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )
        for command in commands:
            self.logger.info( f"Running {command}" )
            cursor.execute( command )

        conn.commit()


    def load_all_directories( self ):
        now = datetime.datetime.now( tz=datetime.UTC )

        # Make sure all HEAD.FITS.gz and PHOT.FITS.gz files exist
        direcheadfiles = {}
        direcphotfiles = {}
        for directory in self.directories:
            self.logger.info( f"Verifying directory {directory}" )
            direc = pathlib.Path( directory )
            if not direc.is_dir():
                raise RuntimeError( f"{str(direc)} isn't a directory" )

            headre = re.compile( '^(.*)HEAD\.FITS\.gz' )
            if len( self.files ) == 0:
                headfiles = list( direc.glob( '*HEAD.FITS.gz' ) )
            else:
                headfiles = [ direc / h for h in self.files ]
            photfiles = []
            for headfile in headfiles:
                match = headre.search( headfile.name )
                if match is None:
                    raise ValueError( f"Failed to parse {headfile.name} for *.HEAD.FITS.gz" )
                photfile = direc / f"{match.group(1)}PHOT.FITS.gz"
                if not headfile.is_file():
                    raise FileNotFoundError( f"Can't read {headfile}" )
                if not photfile.is_file():
                    raise FileNotFoundError( f"Can't read {photfile}" )
                photfiles.append( photfile )

            direcheadfiles[ direc ] = headfiles
            direcphotfiles[ direc ] = photfiles

        # Make the ProcessingVersions and Snapshots objects
        pv = ProcessingVersions.objects.filter( pk=self.processing_version )
        if len(pv) == 0:
            pv = ProcessingVersions( version=self.processing_version,validity_start=now )
            pv.save()

        ss = Snapshots.objects.filter( pk=self.snapshot )
        if len(ss) == 0:
            ss = Snapshots( name=self.snapshot, insert_time=now )
            ss.save()

        # Do the long stuff
            
        self.logger.info( f'Launching {self.nprocs} processes to load the db.' )

        # Need to make sure that each subprocess gets its own database
        # connection.  To that end, close the django connections so
        # there aren't any cached ones.
        django.db.connections.close_all()

        def launchFITSFileHandler( pipe ):
            hndlr = FITSFileHandler( self, pipe )
            hndlr.listener()

        freeprocs = set()
        busyprocs = set()
        procinfo = {}
        for i in range(self.nprocs):
            parentconn, childconn = multiprocessing.Pipe()
            proc = multiprocessing.Process( target=lambda: launchFITSFileHandler( childconn ) )
            proc.start()
            procinfo[ proc.pid ] = { 'proc': proc,
                                     'parentconn': parentconn,
                                     'childconn': childconn }
            freeprocs.add( proc.pid )

        # Go through the directories and load everyhting
        for directory in self.directories:
            self.logger.info( f"Loading files in {directory}" )
            direc = pathlib.Path( directory )
            headfiles = direcheadfiles[ direc ]
            photfiles = direcphotfiles[ direc ]
            donefiles = set()
            errfiles = set()

            fileptr = 0
            done = False
            while not done:
                # Tell any free processes what to do:
                while ( len(freeprocs) > 0 ) and ( fileptr < len(headfiles) ):
                    pid = freeprocs.pop()
                    busyprocs.add( pid )
                    procinfo[pid]['parentconn'].send( { 'command': "do",
                                                        'headfile': headfiles[fileptr],
                                                        'photfile': photfiles[fileptr]
                                                       } )
                    fileptr += 1

                # Check for response from busy processes
                doneprocs = set()
                for pid in busyprocs:
                    try:
                        # ROB TODO : recv() blocks.
                        # Use poll()
                        msg = procinfo[pid]['parentconn'].recv()
                    except queue.Empty:
                        continue
                    if msg['response'] != 'done':
                        raise ValueError( f"Unexpected response from child process: {msg}" )
                    if msg['headfile'] in donefiles:
                        raise RuntimeError( f"{msg['headfile']} got processed twice" )
                    donefiles.add( msg['headfile'] )
                    if msg['retval']['ok']:
                        self.logger.info( f"{msg['headfile']} done: {msg['retval']['msg']}" )
                    else:
                        errfiles.add( msg['headfile'] )
                        self.logger.error( f"{msg['headfile']} failed {msg['retval']['msg']}" )
                    doneprocs.add( pid )

                for pid in doneprocs:
                    busyprocs.remove( pid )
                    freeprocs.add( pid )

                if ( len(busyprocs) == 0 ) and ( fileptr >= len(headfiles) ):
                    # Everything has been submitted, we're waiting for
                    # no processes, so we're done.
                    done = True
                else:
                    # Sleep before polling if there's nothing ready to submit
                    # and we're just waiting for responses from busy processes
                    if ( len(freeprocs) == 0 ) or ( fileptr >= len(headfiles) ):
                        time.sleep( 1 )

            if len(donefiles) != len(headfiles):
                raise RuntimeError( f"Something bad has happened; there are {len(headfiles)} headfiles, "
                                    f"but only {len(donefiles)} donefiles!" )

        # Close all the processes we started

        for pid, info in procinfo.items():
            info['parentconn'].send( { 'command': 'die' } )
        time.sleep( 1 )
        for pid, info in procinfo.items():
            info['proc'].close()


# **********************************************************************

class Command(BaseCommand):
    help = "Load SNANA FITS file into fastdb_dev tables"

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )
        self.logger = logging.getLogger( "load_fastdb_dev_snana_fits" )
        self.logger.propagate = False
        if not self.logger.hasHandlers():
            logout = logging.StreamHandler( sys.stderr )
            self.logger.addHandler( logout )
            formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                           datefmt='%Y-%m-%d %H:%M:%S' )
            logout.setFormatter( formatter )
        self.logger.setLevel( logging.INFO )

    def add_arguments( self, parser ):
        parser.add_argument( '-n', '--nprocs', default=5, type=int,
                             help=( "Number of worker processes to load; make sure that the number of CPUs "
                                    "available is at least this many plus one." ) )
        parser.add_argument( '-d', '--directories', default=[], nargs='+', required=True,
                             help="Directory to find the HEAD and PHOT fits files" )
        parser.add_argument( '-f', '--files', default=[], nargs='+',
                             help="Names of HEAD.fits[.[fg]z] files; default is to read all in directory" )
        parser.add_argument( '--verbose', action='store_true', default=False,
                             help="Set log level to DEBUG (default INFO)" )
        parser.add_argument( '-m', '--max-sources-per-object', default=100000, type=int,
                             help=( "Maximum number of sources for a single object.  Used to generate "
                                    "source ids, so make it big enough." ) )
        parser.add_argument( '-p', '--photflag-detect', default=4096, type=int,
                             help=( "The bit (really, 2^the bit) that indicates if a source is detected" ) )
        parser.add_argument( '-z', '--snana-zeropoint', default=27.5, type=float,
                             help="Zeropoint to move all photometry to" )
        parser.add_argument( '-a', '--alert-zeropoint', default=31.4, type=float,
                             help="Zeropoint to use for alerts (and to store in the source table)" )
        parser.add_argument( '--processing-version', '--pv', default=None, required=True,
                             help="String value of the processing version to set for all objects" )
        parser.add_argument( '-s', '--snapshot', default=None,
                             help="If given, create this snapshot and put all loaded sources/forced sources in it" )
        parser.add_argument( '-t', '--mjd-start', default=60796., type=float,
                             help="Time that the whole campaign starts (for season determination)" )
        parser.add_argument( '--dont-disable-indexes-fks', action='store_true', default=False,
                             help="Don't temporarily disable indexes and foreign keys (by default will)" )
        parser.add_argument( '--do', action='store_true', default=False,
                             help="Actually do it (otherwise, slowly reads FITS files but doesn't affect db" )


    def handle( self, *args, **options ):
        if options['verbose']:
            self.logger.setLevel( logging.DEBUG )

        fitsloader = FITSLoader( options['nprocs'], options['directories'],
                                 logger=self.logger,
                                 files=options['files'],
                                 processing_version=options['processing_version'],
                                 snapshot=options['snapshot'],
                                 max_sources_per_object=options['max_sources_per_object'],
                                 photflag_detect=options['photflag_detect'],
                                 snana_zeropoint=options['snana_zeropoint'],
                                 alert_zeropoint=options['alert_zeropoint'],
                                 t0=options['mjd_start'],
                                 really_do=options['do'],
                                 verbose=options['verbose'] )

        # Disable indexes and foreign keys
        if not options[ "dont_disable_indexes_fks" ]:
            self.logger.warning( "Disabling all indexes and foreign keys before loading" )
            fitsloader.disable_indexes_and_fks()

        # Do
        fitsloader.load_all_directories()

        # Recreate all indexes
        if not options[ "dont_disable_indexes_fks" ]:
            self.logger.warning( "Recreating indexes and foreign keys" )
            fitsloader.recreate_indexes_and_fks()
