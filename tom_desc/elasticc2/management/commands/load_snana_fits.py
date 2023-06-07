import sys
import io
import re
import pathlib
import logging
import numpy
from astropy.table import Table
import astropy.io

# _rundir = pathlib.Path( __file__ ).parent

from django.core.management.base import BaseCommand, CommandError

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
        parser.add_argument( '-d', '--directory', default=None, required=True,
                             help="Directory to find the HEAD and PHOT fits files" )
        parser.add_argument( '-f', '--files', default=[], nargs='+',
                             help="Names of HEAD.fits[.[fg]z] files; default is to read all in directory" )
        parser.add_argument( '--verbose', action='store_true', default=False,
                             help="Set log level to DEBUG (default INFO)" )
        parser.add_argument( '-s', '--simversion', default='2023-04-01',
                             help="What to stick in the simversion column" )
        parser.add_argument( '-m', '--max-sources-per-object', default=2000, type=int,
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
        parser.add_argument( '--do', action='store_true', default=False,
                             help="Actually do it (otherwise, slowly reads FITS files but doesn't affect db" )
        

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
                       'Q060', 'q070', 'Q080', 'Q090', 'Q100' ):
                lcs.add( f'HOSTGAL{hostgal}_ZPHOT_{q}' )
            for f in ( 'MAG_u', 'MAG_g', 'MAG_r', 'MAG_i', 'MAG_z', 'MAG_Y',
                       'MAGERR_u', 'MAGERR_g', 'MAGERR_r', 'MAGERR_i', 'MAGERR_z', 'MAGERR_Y',
                       'ELLIPTICITY', 'SQRADIUS', 'SNSEP', 'RA', 'DEC' ):
                lcs.add( f'HOSTGAL{hostgal}_{f}' )
        
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
        
    def handle( self, *args, **options ):
        if options['verbose']:
            self.logger.setLevel( logging.DEBUG )

        if not ( options['ppdb'] or options['train'] ):
            raise RuntimeError( "Must give one of --ppdb or --train" )
        if options['ppdb'] and options['train']:
            raise RuntimeError( "Must give only one of --pdb or --train" )

        if options['ppdb']:
            from elasticc2.models import PPDBDiaObject as DiaObject
            from elasticc2.models import PPDBDiaForcedSource as DiaForcedSource
            from elasticc2.models import PPDBDiaSource as DiaSource
            from elasticc2.models import PPDBAlert as Alert
            from elasticc2.models import DiaObjectTruth as Truth
        elif options['train']:
            from elasticc2.models import TrainDiaObject as DiaObject
            from elasticc2.models import TrainDiaForcedSource as DiaForcedSource
            from elasticc2.models import TrainDiaSource as DiaSource
            from elasticc2.models import TrainAlert as Alert
            from elasticc2.models import TrainDiaObjectTruth as Truth
        else:
            raise RuntimeError( "This should never happen." )

        self.simversion = options['simversion']

        # Read the dump file
        
        direc = pathlib.Path( options['directory'] )
        if not direc.is_dir():
            raise RuntimeError( f"{str(direc)} isn't a directory" )
        dumpfile = direc / f"{direc.name}.DUMP"
        if not dumpfile.is_file():
            raise RuntimeError( f"Can't read {dumpfile}" )
        dump = astropy.io.ascii.read( dumpfile )

        if len( options['files'] ) == 0:
            headfiles = list( direc.glob( '*HEAD.FITS.gz' ) )
        else:
            headfiles = options['files']
            headfiles = [ direc / h for h in headfiles ]

        # Make sure all HEAD.FITS.gz and PHOT.FITS.gz files exist
            
        headre = re.compile( '^(.*)HEAD\.FITS\.gz' )
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

        # Load the HEAD/PHOT files

        self.logger.debug( f'Headfiles: {[headfiles]}' )
        self.logger.debug( f'Photfiles: {[photfiles]}' )
        
        for headfile, photfile in zip( headfiles, photfiles ):
            self.logger.info( f"Reading {headfile.name}" )

            orig_head = Table.read( headfile )
            # SNID was written as a string, we need it to be a bigint
            orig_head['SNID'] = orig_head['SNID'].astype( numpy.int64 )
            head = Table( orig_head )

            if len(head) == 0:
                continue
            
            phot = Table.read( photfile )

            # Load the PPDBDiaObject table

            self.diaobject_map_columns( head )
            head.add_column( options['simversion'], name='simversion' )
            
            if options['do']:
                n = DiaObject.bulk_insert_onlynew( dict( head ) )
                self.logger.info( f"Loaded {n} objects from {headfile.name}" )
            else:
                self.logger.info( f"Would try to load {len(head)} objects" )

            # Calculate some derived fields we'll need

            phot['FLUXCAL'] *= 10 ** ( ( options['alert_zeropoint'] - options['snana_zeropoint'] ) / 2.5 )
            phot['FLUXCALERR'] *= 10 ** ( ( options['alert_zeropoint'] - options['snana_zeropoint'] ) / 2.5 )
            
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
                phot['diaobject_id'][pmin:pmax+1] = obj['SNID']
                phot['diaforcedsource_id'][pmin:pmax+1] = ( obj['SNID'] * options['max_sources_per_object']
                                                            + numpy.arange( pmax - pmin + 1 ) )
                phot['ra'][pmin:pmax+1] = obj['RA']
                phot['decl'][pmin:pmax+1] = obj['DEC']

            # The phot table has separators, so there will still be some junk data in there I need to purge
            phot = phot[ phot['diaobject_id'] >= 0 ]
                
            if options['do']:
                n = DiaForcedSource.bulk_insert_onlynew( phot )
                self.logger.info( f"Loaded {n} forced photometry points from {photfile.name}" )
            else:
                self.logger.info( f"Would try to load {len(phot)} forced photometry points" )
            
            # Load the DiaSource table

            phot.rename_column( 'diaforcedsource_id', 'diasource_id' )
            phot = phot[ ( phot['photflag'] & options['photflag_detect'] ) !=0 ]

            if options['do']:
                n = DiaSource.bulk_insert_onlynew( phot )
                self.logger.info( f"Loaded {n} sources from {photfile.name}" )
            else:
                self.logger.info( f"Would try to load {len(phot)} sources" )

            # Load the alert table based on this

            alerts = { 'alert_id': phot[ 'diasource_id' ],
                       'diasource_id': phot[ 'diasource_id' ],
                       'diaobject_id': phot[ 'diaobject_id' ] }
            if options['do']:
                n = Alert.bulk_insert_onlynew( alerts )
                self.logger.info( f"Loaded {n} alerts" )
            else:
                self.logger.info( f"Would try to load {len(alerts['alert_id'])} alerts" )
                
        # Load the object truth table
            
        self.diaobjecttruth_map_columns( dump )
        self.logger.info( f"Trying to load {len(dump)} rows to object truth table" )
        if options['do']:
            truthobjs = Truth.bulk_load_or_create( dump )
            self.logger.info( f"Loaded between 0 and {len(truthobjs)} rows to object truth table" )
