import sys
import os
import io
import re
import time
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

from elasticc2.models import (
    TrainingDiaObject,
    TrainingDiaForcedSource,
    TrainingDiaSource,
    TrainingAlert,
    TrainingDiaObjectTruth,
    PPDBDiaObject,
    PPDBDiaForcedSource,
    PPDBDiaSource,
    PPDBAlert,
    DiaObjectTruth )

class ColumnMapper:
    @classmethod
    def diaobject_map_columns( cls, tab ):
        """Map from the HEAD.FITS.gz files to the diaobject table"""
        mapper = { 'SNID': 'diaobject_id',
                   'DEC': 'decl',
                   'REDSHIFT_FINAL': 'z_final',
                   'REDSHIFT_FINAL_ERR': 'z_final_err',
                   'HOSTGAL_SPECZ': 'hostgal_zspec',
                   'HOSTGAL_SPECZ_ERR': 'hostgal_zspec_err',
                   'HOSTGAL_PHOTOZ': 'hostgal_zphot',
                   'HOSTGAL_PHOTOZ_ERR': 'hostgal_zphot_err',
                   'HOSTGAL2_SPECZ': 'hostgal2_zspec',
                   'HOSTGAL2_SPECZ_ERR': 'hostgal2_zspec_err',
                   'HOSTGAL2_PHOTOZ': 'hostgal2_zphot',
                   'HOSTGAL2_PHOTOZ_ERR': 'hostgal2_zphot_err',
                  }
        lcs = { 'RA', 'MWEBV', 'MWEBV_ERR', 'simversion' }
        for hostgal in ( "", "2" ):
            for q in ( 'Q000', 'Q010', 'Q020', 'Q030', 'Q040', 'Q050',
                       'Q060', 'Q070', 'Q080', 'Q090', 'Q100' ):
                lcs.add( f'HOSTGAL{hostgal}_ZPHOT_{q}' )
            for f in ( 'MAG_u', 'MAG_g', 'MAG_r', 'MAG_i', 'MAG_z', 'MAG_Y',
                       'MAGERR_u', 'MAGERR_g', 'MAGERR_r', 'MAGERR_i', 'MAGERR_z', 'MAGERR_Y',
                       'ELLIPTICITY', 'SQRADIUS', 'SNSEP', 'RA', 'DEC' ):
                lcs.add( f'HOSTGAL{hostgal}_{f}' )

        cls._map_columns( tab, mapper, lcs )

    @classmethod
    def diasource_map_columns( cls, tab ):
        """Map from the PHOT.FITS.gz files to the diasource table"""
        mapper = { 'MJD': 'midpointtai',
                   'BAND': 'filtername',
                   'FLUXCAL': 'psflux',
                   'FLUXCALERR': 'psfluxerr',
                   # This next one isn't part of the table, but I need it for further processing
                   'PHOTFLAG': 'photflag'
                  }
        lcs = { }
        cls._map_columns( tab, mapper, lcs )

    @classmethod
    def diaobjecttruth_map_columns( cls, tab ):
        """Map from the DUMP file to the elasticc2_diaobjecttruth table"""
        mapper = { 'CID': 'diaobject_id',
                   'NON1A_INDEX': 'sim_template_index'
                  }
        lcs = { 'LIBID', 'SIM_SEARCHEFF_MASK', 'GENTYPE', 'ZCMB',
                'ZHELIO', 'ZCMB_SMEAR', 'RA', 'DEC', 'MWEBV', 'GALID',
                'GALZPHOT', 'GALZPHOTERR', 'GALSNSEP', 'GALSNDDLR',
                'RV', 'AV', 'MU', 'LENSDMU', 'PEAKMJD',
                'MJD_DETECT_FIRST', 'MJD_DETECT_LAST', 'DTSEASON_PEAK',
                'PEAKMAG_u', 'PEAKMAG_g', 'PEAKMAG_r', 'PEAKMAG_i',
                'PEAKMAG_z', 'PEAKMAG_Y', 'SNRMAX', 'SNRMAX2',
                'SNRMAX3', 'NOBS', 'NOBS_SATURATE' }
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
        for attr in [ 'simversion', 'max_sources_per_object', 'photflag_detect',
                      'snana_zeropoint', 'alert_zeropoint', 'really_do', 'verbose', 'tableset' ]:
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
            if self.tableset == 'ppdb':
                DiaObject = PPDBDiaObject
                DiaSource = PPDBDiaSource
                DiaForcedSource = PPDBDiaForcedSource
                Alert = PPDBAlert
            elif self.tableset == 'training':
                DiaObject = TrainingDiaObject
                DiaSource = TrainingDiaSource
                DiaForcedSource = TrainingDiaForcedSource
                Alert = TrainingAlert
            else:
                raise ValueError( f"Unknown tableset {self.tableset}" )

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
            head.add_column( self.simversion, name='simversion' )
            head.add_column( False, name='isddf' )

            # Figure out which objects are "DDF" objects
            for origrow, row in zip( orig_head, head ):
                pmin = origrow['PTROBS_MIN'] - 1
                pmax = origrow['PTROBS_MAX'] - 1
                # The right criterion for DDF isn't obvious
                # Practically speaking, the issue was alerts
                # with way too much photometry in them.  So,
                # limit based on the total count.
                if ( pmax + 1 - pmin ) > 1000:
                    row['isddf'] = True

                # Another option would be to look at the
                # maximum (or mean) number of observations
                # in a single day.  (The 0.625 offsets
                # to noon Chile time to divide nights.)
                # mjds = numpy.floor( phot['MJD'][pmin:pmax+1] - 0.625 ).astype( numpy.int64 )
                # hist, histbin = numpy.histogram( mjds, bins=range( mjds[0], mjds[-1]+1 ) )
                # hist = hist[ hist > 0 ]
                # if ( hist.mean() >= 10 ) or ( hist.max() >= 30 ):
                #     row['isddf'] = True

            if self.really_do:
                nobj = DiaObject.bulk_insert_onlynew( dict( head ) )
                self.logger.info( f"PID {os.getpid()} loaded {nobj} objects from {headfile.name}" )
            else:
                nobj = len(head)
                self.logger.info( f"PID {os.getpid()} would try to load {nobj} objects" )

            # Calculate some derived fields we'll need

            phot['FLUXCAL'] *= 10 ** ( ( self.alert_zeropoint - self.snana_zeropoint ) / 2.5 )
            phot['FLUXCALERR'] *= 10 ** ( ( self.alert_zeropoint - self.snana_zeropoint ) / 2.5 )

            self.diasource_map_columns( phot )
            phot.add_column( numpy.int64(-1), name='diaobject_id' )
            phot.add_column( numpy.int64(-1), name='diaforcedsource_id' )
            phot['snr'] = phot['psflux'] / phot['psfluxerr']
            phot['filtername'] = [ i.strip() for i in phot['filtername'] ]
            phot.add_column( -999., name='ra' )
            phot.add_column( -999., name='decl' )

            # Load the DiaForcedSource table

            for obj in orig_head:
                # All the -1 is because the files are 1-indexed, but astropy is 0-indexed
                pmin = obj['PTROBS_MIN'] -1
                pmax = obj['PTROBS_MAX'] -1
                if ( pmax - pmin + 1 ) > self.max_sources_per_object:
                    self.logger.error( f'SNID {obj["SNID"]} in {headfile.name} has {pmax-mpin+1} sources, '
                                       f'which is more than max_sources_per_object={self.max_sources_per_object}' )
                    raise RuntimeError( "Too many sources" )
                phot['diaobject_id'][pmin:pmax+1] = obj['SNID']
                phot['diaforcedsource_id'][pmin:pmax+1] = ( obj['SNID'] * self.max_sources_per_object
                                                            + numpy.arange( pmax - pmin + 1 ) )
                phot['ra'][pmin:pmax+1] = obj['RA']
                phot['decl'][pmin:pmax+1] = obj['DEC']

            # The phot table has separators, so there will still be some junk data in there I need to purge
            phot = phot[ phot['diaobject_id'] >= 0 ]

            if self.really_do:
                nfrc = DiaForcedSource.bulk_insert_onlynew( phot )
                self.logger.info( f"PID {os.getpid()} loaded {nfrc} forced photometry points from {photfile.name}" )
            else:
                nfrc = len(phot)
                self.logger.info( f"PID {os.getpid()} would try to load {nfrc} forced photometry points" )

            # Load the DiaSource table

            phot.rename_column( 'diaforcedsource_id', 'diasource_id' )
            phot = phot[ ( phot['photflag'] & self.photflag_detect ) !=0 ]

            if self.really_do:
                nsrc = DiaSource.bulk_insert_onlynew( phot )
                self.logger.info( f"PID {os.getpid()} loaded {nsrc} sources from {photfile.name}" )
            else:
                nsrc = len(phot)
                self.logger.info( f"PID {os.getpid()} would try to load {nsrc} sources" )

            # Load the alert table based on this

            alerts = { 'alert_id': phot[ 'diasource_id' ],
                       'diasource_id': phot[ 'diasource_id' ],
                       'diaobject_id': phot[ 'diaobject_id' ] }
            if self.really_do:
                nalrt = Alert.bulk_insert_onlynew( alerts )
                self.logger.info( f"PID {os.getpid()} loaded {nalrt} alerts" )
            else:
                nalrt = len(alerts['alert_id'])
                self.logger.info( f"PID {os.getpid()} would try to load {nalrt} alerts" )

            return { "ok": True, "msg": f"Loaded {nobj} objs, {nsrc} src, {nfrc} forced, {nalrt} alerts" }
        except Exception as e:
            self.logger.error( f"Exception loading {headfile}: {traceback.format_exc()}" )
            return { "ok": False, "msg": traceback.format_exc() }


# ----------------------------------------------------------------------

class FITSLoader( ColumnMapper ):
    def __init__( self, nprocs, directories, files=[], simversion='2023-04-01',
                  max_sources_per_object=100000, photflag_detect=4096,
                  snana_zeropoint=27.5, alert_zeropoint=31.4, tableset='ppdb',
                  really_do=False, verbose=False, logger=logging.getLogger( "load_snana_fits") ):

        if tableset not in [ 'ppdb', 'training' ]:
            raise ValueError( f'tableset must be one of ppdb, training, not {tableset}' )

        self.nprocs = nprocs
        self.directories = directories
        self.files = files
        self.simversion = simversion
        self.max_sources_per_object=max_sources_per_object
        self.photflag_detect = photflag_detect
        self.snana_zeropoint = snana_zeropoint
        self.alert_zeropoint = alert_zeropoint
        self.tableset = tableset
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

        if self.tableset == 'training':
            tables = [ 'elasticc2_trainingalert', 'elasticc2_trainingdiaobject',
                       'elasticc2_trainingdiasource', 'elasticc2_trainingdiaforcedsource,'
                       'elasticc2_trainingdiaobjecttruth' ]
        else:
            tables = [ 'elasticc2_ppdbalert', 'elasticc2_ppdbdiaobject',
                       'elasticc2_ppdbdiasource', 'elasticc2_ppdbdiaforcedsource',
                       'elasticc2_diaobjecttruth' ]

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
                    pkreconstructs.insert( 0, f"ALTER TABLE {table} ADD {row['condef']};" )
                else:
                    tableconstraints[table].append( row['conname'] )
                    constraintreconstructs.insert( 0, f"ALTER TABLE {table} ADD {row['condef']};" )

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
                if match.group(1) != primarykeys[table]:
                    # The primary key index will be deleted when
                    #  the primary key constraint is deleted
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
        if self.tableset == 'ppdb':
            Truth = DiaObjectTruth
        elif self.tableset == 'training':
            Truth = TrainingDiaObjectTruth
        else:
            raise ValueError( f"Unknown tableset {self.tableset}" )

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

            # Read the dump file

            dumpfile = direc / f"{direc.name}.DUMP"
            if not dumpfile.is_file():
                raise RuntimeError( f"Can't read {dumpfile}" )
            dump = astropy.io.ascii.read( dumpfile )

            # Load the object truth table

            self.diaobjecttruth_map_columns( dump )
            self.logger.info( f"There are {len(dump)} rows in the dump file" )
            if self.really_do:
                # This one is slow because it does a SELECT IN
                # truthobjs = Truth.bulk_load_or_create( dump )
                self.logger.info( f"Trying to load {len(dump)} rows to object truth table" )
                truthobjs = Truth.objects.bulk_create( [ Truth( **(Truth.data_to_createdict( i )) ) for i in dump ],
                                                         ignore_conflicts=True )
                self.logger.info( f"Loaded {len(truthobjs)} of {len(dump)} rows to object truth table" )

            self.logger.info( f'Done with directory {direc}' )

        # Close all the processes we started

        for pid, info in procinfo.items():
            info['parentconn'].send( { 'command': 'die' } )
        time.sleep( 1 )
        for pid, info in procinfo.items():
            info['proc'].close()


# **********************************************************************

class Command(BaseCommand):
    help = "Load SNANA FITS file into DiaObject/Source/ForcedSoruce/Truth tables"

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )
        self.logger = logging.getLogger( "load_snana_fits" )
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
        parser.add_argument( '-s', '--simversion', default='2023-04-01',
                             help="What to stick in the simversion column" )
        parser.add_argument( '-m', '--max-sources-per-object', default=100000, type=int,
                             help=( "Maximum number of sources for a single object.  Used to generate "
                                    "source ids, so make it big enough." ) )
        parser.add_argument( '-p', '--photflag-detect', default=4096, type=int,
                             help=( "The bit (really, 2^the bit) that indicates if a source is detected" ) )
        parser.add_argument( '-z', '--snana-zeropoint', default=27.5, type=float,
                             help="Zeropoint to move all photometry to" )
        parser.add_argument( '-a', '--alert-zeropoint', default=31.4, type=float,
                             help="Zeropoint to use for alerts (and to store in the source table)" )
        parser.add_argument( '--ppdb', action='store_true', default=False,
                             help="Load the PPDB* tables" )
        parser.add_argument( '--train', action='store_true', default=False,
                             help='Load the Train* tables' )
        parser.add_argument( '--dont-disable-indexes-fks', action='store_true', default=False,
                             help="Don't temporarily disable indexes and foreign keys (by default will)" )
        parser.add_argument( '--do', action='store_true', default=False,
                             help="Actually do it (otherwise, slowly reads FITS files but doesn't affect db" )


    def handle( self, *args, **options ):
        if options['verbose']:
            self.logger.setLevel( logging.DEBUG )

        if not ( options['ppdb'] or options['train'] ):
            raise RuntimeError( "Must give one of --ppdb or --train" )
        if options['ppdb'] and options['train']:
            raise RuntimeError( "Must give only one of --pdb or --train" )

        fitsloader = FITSLoader( options['nprocs'], options['directories'],
                                 logger=self.logger,
                                 tableset='ppdb' if options['ppdb'] else 'training',
                                 files=options['files'],
                                 simversion=options['simversion'],
                                 max_sources_per_object=options['max_sources_per_object'],
                                 photflag_detect=options['photflag_detect'],
                                 snana_zeropoint=options['snana_zeropoint'],
                                 alert_zeropoint=options['alert_zeropoint'],
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
