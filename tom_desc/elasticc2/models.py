import sys
import math
import time
import datetime
import pytz
import logging
import itertools
import psqlextra.types
import psqlextra.models
import psycopg2.extras
import django.db
from django.db import models
from guardian.shortcuts import assign_perm
from django.contrib.auth.models import Group

import uuid
from cassandra.cqlengine import columns
from django_cassandra_engine.models import DjangoCassandraModel

# NOTE FOR ROB
#
# Many schema are similar to elasticc.
# However, I'm not inheriting, because I think that would
# break things horribly.  All the refernces to other models
# in what I inherit is probably going to be in the elasticc
# namespace, not in the elasticc2 namespace, which will cause
# no end of confusion.
#
# I considered making an elasticc_base class, but chances are
# that if we do ELAsTiCC 3, schema will change enough again
# to make it not worth the effor I'd put in now.
#
# So, lots of copied code.


# My customizations:
# Support for the Q3c indexing scheme, index names up to 63 characters
# (django limits to 30, postgres has more), and the Createable base class
# that defines the "create" and "load_or_create" methods for bulk
# upserting.
from db.models import q3c_ang2ipix, LongNameBTreeIndex, Createable, Float32Field

# Link to tom targets
import tom_targets.models

_logger = logging.getLogger(__name__)
_logout = logging.StreamHandler( sys.stderr )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' )
_logout.setFormatter( _formatter )
_logger.propagate = False
_logger.addHandler( _logout )
# _logger.setLevel( logging.INFO )
_logger.setLevel( logging.DEBUG )

# ======================================================================
# ======================================================================
# ======================================================================
# Base tables for PPDB and Training tables

class BaseDiaObject(Createable):
    diaobject_id = models.BigIntegerField( primary_key=True, unique=True, db_index=True )
    isddf = models.BooleanField( default=False )
    simversion = models.TextField( null=True )
    ra = models.FloatField( )
    decl = models.FloatField( )
    mwebv = Float32Field( null=True )
    mwebv_err = Float32Field( null=True )
    z_final = Float32Field( null=True )
    z_final_err = Float32Field( null=True )
    hostgal_ellipticity = Float32Field( null=True )
    hostgal_sqradius = Float32Field( null=True )
    hostgal_zspec = Float32Field( null=True )
    hostgal_zspec_err = Float32Field( null=True )
    hostgal_zphot = Float32Field( null=True )
    hostgal_zphot_err = Float32Field( null=True )
    hostgal_zphot_q000 = Float32Field( null=True)
    hostgal_zphot_q010 = Float32Field( null=True )
    hostgal_zphot_q020 = Float32Field( null=True )
    hostgal_zphot_q030 = Float32Field( null=True )
    hostgal_zphot_q040 = Float32Field( null=True )
    hostgal_zphot_q050 = Float32Field( null=True )
    hostgal_zphot_q060 = Float32Field( null=True )
    hostgal_zphot_q070 = Float32Field( null=True )
    hostgal_zphot_q080 = Float32Field( null=True )
    hostgal_zphot_q090 = Float32Field( null=True )
    hostgal_zphot_q100 = Float32Field( null=True )
    hostgal_mag_u = Float32Field( null=True )
    hostgal_mag_g = Float32Field( null=True )
    hostgal_mag_r = Float32Field( null=True )
    hostgal_mag_i = Float32Field( null=True )
    hostgal_mag_z = Float32Field( null=True )
    hostgal_mag_y = Float32Field( null=True )
    hostgal_ra = Float32Field( null=True )
    hostgal_dec = Float32Field( null=True )
    hostgal_snsep = Float32Field( null=True )
    hostgal_magerr_u = Float32Field( null=True )
    hostgal_magerr_g = Float32Field( null=True )
    hostgal_magerr_r = Float32Field( null=True )
    hostgal_magerr_i = Float32Field( null=True )
    hostgal_magerr_z = Float32Field( null=True )
    hostgal_magerr_y = Float32Field( null=True )
    hostgal2_ellipticity = Float32Field( null=True )
    hostgal2_sqradius = Float32Field( null=True )
    hostgal2_zspec = Float32Field( null=True )
    hostgal2_zspec_err = Float32Field( null=True )
    hostgal2_zphot = Float32Field( null=True )
    hostgal2_zphot_err = Float32Field( null=True )
    hostgal2_zphot_q000 = Float32Field( null=True )
    hostgal2_zphot_q010 = Float32Field( null=True )
    hostgal2_zphot_q020 = Float32Field( null=True )
    hostgal2_zphot_q030 = Float32Field( null=True )
    hostgal2_zphot_q040 = Float32Field( null=True )
    hostgal2_zphot_q050 = Float32Field( null=True )
    hostgal2_zphot_q060 = Float32Field( null=True )
    hostgal2_zphot_q070 = Float32Field( null=True )
    hostgal2_zphot_q080 = Float32Field( null=True )
    hostgal2_zphot_q090 = Float32Field( null=True )
    hostgal2_zphot_q100 = Float32Field( null=True )
    hostgal2_mag_u = Float32Field( null=True )
    hostgal2_mag_g = Float32Field( null=True )
    hostgal2_mag_r = Float32Field( null=True )
    hostgal2_mag_i = Float32Field( null=True )
    hostgal2_mag_z = Float32Field( null=True )
    hostgal2_mag_y = Float32Field( null=True )
    hostgal2_ra = Float32Field( null=True )
    hostgal2_dec = Float32Field( null=True )
    hostgal2_snsep = Float32Field( null=True )
    hostgal2_magerr_u = Float32Field( null=True )
    hostgal2_magerr_g = Float32Field( null=True )
    hostgal2_magerr_r = Float32Field( null=True )
    hostgal2_magerr_i = Float32Field( null=True )
    hostgal2_magerr_z = Float32Field( null=True )
    hostgal2_magerr_y = Float32Field( null=True )

    class Meta:
        abstract = True
        indexes = [
            LongNameBTreeIndex( q3c_ang2ipix( 'ra', 'decl' ),
                                name='idx_%(app_label)s_%(class)s_q3c' ),
        ]

    _pk = 'diaobject_id'

    # WARNING : I later assume that these are in the same order as _create_kws in DiaObject
    _create_kws = [ _pk, 'isddf', 'simversion', 'ra', 'decl', 'mwebv', 'mwebv_err', 'z_final', 'z_final_err' ]
    for _gal in [ "", "2" ]:
        _create_kws.append( f'hostgal{_gal}_zspec' )
        _create_kws.append( f'hostgal{_gal}_zspec_err' )
        _create_kws.append( f'hostgal{_gal}_zphot' )
        _create_kws.append( f'hostgal{_gal}_zphot_err' )
        _create_kws.append( f'hostgal{_gal}_ra' )
        _create_kws.append( f'hostgal{_gal}_dec' )
        _create_kws.append( f'hostgal{_gal}_snsep' )
        _create_kws.append( f'hostgal{_gal}_ellipticity' )
        _create_kws.append( f'hostgal{_gal}_sqradius' )
        for _phot in [ 'q000', 'q010', 'q020', 'q030', 'q040', 'q050', 'q060', 'q070', 'q080', 'q090', 'q100' ]:
            _create_kws.append( f'hostgal{_gal}_zphot_{_phot}' )
        for _band in [ 'u', 'g', 'r', 'i', 'z', 'y' ]:
            for _err in [ '', 'err' ]:
                _create_kws.append( f'hostgal{_gal}_mag{_err}_{_band}' )

    _dict_kws = _create_kws


class BaseDiaSource(Createable):
    diasource_id = models.BigIntegerField( primary_key=True, unique=True, db_index=True )

    # IMPORTANT : subclasses need a foreign key into the right object table
    # diaobject = models.ForeignKey( BaseDiaObject, db_column='diaobject_id',
    #                                on_delete=models.CASCADE, null=True )

    midpointtai = models.FloatField( db_index=True )
    filtername = models.TextField()
    ra = models.FloatField( )
    decl = models.FloatField( )
    psflux = Float32Field()
    psfluxerr = Float32Field()
    snr = Float32Field( )

    class Meta:
        abstract = True
        indexes = [
            LongNameBTreeIndex( q3c_ang2ipix( 'ra', 'decl' ),
                                name='idx_%(app_label)s_%(class)s_q3c' ),
        ]

    _pk = 'diasource_id'
    # WARNING : I later assume that these are in the same order as _create_kws in DiaSource
    _create_kws = [ _pk, 'diaobject_id', 'midpointtai', 'filtername', 'ra', 'decl',
                    'psflux', 'psfluxerr', 'snr' ]
    _dict_kws = _create_kws

class BaseDiaForcedSource(Createable):
    diaforcedsource_id = models.BigIntegerField( primary_key=True, unique=True, db_index=True )

    # IMPORTANT : subclasses need a freigh key into the right object table
    # diaobject = models.ForeignKey( BaseDiaObject, db_column='diaobject_id', on_delete=models.CASCADE )

    midpointtai = models.FloatField( db_index=True )
    filtername = models.TextField()
    psflux = Float32Field()
    psfluxerr = Float32Field()

    class Meta:
        abstract = True

    _pk = 'diaforcedsource_id'

    # WARNING : I later assume that these are in the same order as _create_kws in DiaSource
    _create_kws = [ _pk, 'diaobject_id', 'midpointtai', 'filtername', 'psflux', 'psfluxerr' ]
    _dict_kws = _create_kws


# Alerts that will be sent out as a simulation of LSST alerts.
# All alerts to be sent are stored here.  If they have actually been
# sent, then alertSentTimestamp will be non-NULL.
#
# Find the current simulation time by finding the maximum midPointTai of
# all diaSource objects for alerts that have been sent.  (I should
# probably cache that somewhere, perhaps with a materialized view that I
# then update daily (or more often?))
class BaseAlert(Createable):
    alert_id = models.BigIntegerField( primary_key=True, unique=True, db_index=True )
    alertsenttimestamp = models.DateTimeField( null=True, db_index=True )

    # IMPORTANT: subclasses need foreigh keys into diaobject and diasource
    # diasource = models.ForeignKey( BaseDiaSource, db_column='diasource_id',
    #                                 on_delete=models.CASCADE, null=True )
    # diaobject = models.ForeignKey( BaseDiaObject, db_column='diaobject_id',
    #                                 on_delete=models.CASCADE, null=True )
    # cutoutDifference
    # cutoutTemplate

    class Meta:
        abstract = True

    _pk = 'alert_id'

    # WARNING : I later assume that these are in the same order as _create_kws in DiaSource
    _create_kws = [ _pk, 'diasource_id', 'diaobject_id' ]
    _dict_kws = [ _pk, 'diasource_id', 'diaobject_id', 'alertsenttimestamp' ]

    # IMPORTANT : subclasses need the following three lines, with the right class names put in
    # _objectclass = BaseDiaObject
    # _sourceclass = BaseDiaSource
    # _forcedsourceclass = BaseDiaForcedSource

    _sourcetime = 0
    _objecttime = 0
    _objectoverheadtime = 0
    _prvsourcetime = 0
    _prvforcedsourcetime = 0

    _hackqueryshown = 0

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )
        self._objectfields = None
        self._objectfieldmap = None

    def reconstruct( self, objsources=None, objforced=None ):
        """Reconstruct the dictionary that represents this alert.

        It's not just a matter of dumping fields, as it also has to decide if the alert
        should include previous photometry and previous forced photometry, and then
        has to pull all that from the database.

        For efficiency, some data can be passed in:

        objsources : a list of dictionaries with the fields from the ..DiaSource objects, sorted by midpointtai
        objforced : same thing, but ...DiaForcedSource

        both objsources and objforced are expected to have *all* sources for the object, not just
        ones that would be in this alert.  They'll be filtered here.

        """
        alert = { "alertId": self.alert_id,
                  "diaSource": {},
                  "prvDiaSources": [],
                  "prvDiaForcedSources": [],
                  "diaObject": {},
                 }

        gratuitous = django.db.connection.cursor()
        conn = gratuitous.connection
        cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )

        t0 = time.perf_counter()
        sourcefields = [ "diaSourceId", "diaObjectId", "midPointTai",
                         "filterName", "ra", "decl", "psFlux", "psFluxErr", "snr" ]
        sourcefieldmap = { i: i.lower() for i in sourcefields }
        sourcefieldmap["diaSourceId"] = 'diasource_id'
        sourcefieldmap["diaObjectId"] = 'diaobject_id'
        for field in sourcefields:
            alert["diaSource"][field] = getattr( self.diasource, sourcefieldmap[ field ] )
        self.__class__._sourcetime += time.perf_counter() - t0

        t0 = time.perf_counter()
        if self._objectfields is None:
            self._objectfields = [ "diaObjectId", "simVersion", "ra", "decl", "mwebv", "mwebv_err",
                                   "z_final", "z_final_err" ]
            for suffix in [ "", "2" ]:
                for hgfield in [ "ellipticity", "sqradius", "zspec", "zspec_err", "zphot", "zphot_err",
                                 "zphot_q000", "zphot_q010", "zphot_q020", "zphot_q030", "zphot_q040",
                                 "zphot_q050", "zphot_q060", "zphot_q070", "zphot_q080", "zphot_q090", "zphot_q100",
                                 "mag_u", "mag_g", "mag_r", "mag_i", "mag_z", "mag_Y",
                                 "ra", "dec", "snsep",
                                 "magerr_u", "magerr_g", "magerr_r", "magerr_i", "magerr_z", "magerr_Y" ]:
                    self._objectfields.append( f"hostgal{suffix}_{hgfield}" )
            self._objectfieldmap = { i: i.lower() for i in self._objectfields }
            self._objectfieldmap["diaObjectId"] = "diaobject_id"
        self.__class__._objectoverheadtime += time.perf_counter() - t0

        for field in self._objectfields:
            alert["diaObject"][field] = getattr( self.diaobject, self._objectfieldmap[ field ] )
        self.__class__._objecttime += time.perf_counter() - t0

        t0 = time.perf_counter()
        if objsources is None:
            objsources = ( self._sourceclass.objects
                           .filter( diaobject_id=self.diasource.diaobject_id )
                           .order_by( "midpointtai" )
                           .values() )
        for prevsource in objsources:
            if prevsource['diasource_id'] == self.diasource.diasource_id: break
            newprevsource = {}
            for field in sourcefields:
                newprevsource[field] = prevsource[ sourcefieldmap[ field ] ]
                                    #= getattr( prevsource, sourcefieldmap[ field ] )
            alert["prvDiaSources"].append( newprevsource )
        self.__class__._prvsourcetime += time.perf_counter() - t0 

        # If this source is the same night as the original detection, then
        # there will be no forced source information

        t0 = time.perf_counter()
        forcedsourcefields = [ "diaForcedSourceId", "diaObjectId", "midPointTai",
                               "filterName", "psFlux", "psFluxErr" ]
        forcedsourcefieldmap = { i: i.lower() for i in forcedsourcefields }
        forcedsourcefieldmap[ "diaForcedSourceId" ] = "diaforcedsource_id"
        forcedsourcefieldmap[ "diaObjectId" ] = "diaobject_id"

        if self.diasource.midpointtai - objsources[0]['midpointtai'] > 0.5:
            if objforced is None:
                objforced = ( self._forcedsourceclass.objects
                              .filter( diaobject_id=self.diasource.diaobject_id )
                              .order_by( 'midpointtai' )
                              .values() )
                if self.__class__._hackqueryshown < 10:
                    _logger.info( f"alert reconstruct forced source query: {objforced.query}" )
                    self.__class__._hackqueryshown += 1
            # objforced = [ i for i in objforced if
            #               i['midpointtai'] >= objsources[0]['midpointtai']-30
            #              and i['midpointtai'] <= self.diasource.midpointtai ]
            # _logger.warn( f"Found {len(objforced)} previous" )
            for forced in objforced:
                if forced['midpointtai'] < objsources[0]['midpointtai'] - 30:
                    continue
                if forced['midpointtai'] >= self.diasource.midpointtai:
                    break
                newforced = {}
                for field in forcedsourcefields:
                    newforced[field] = forced[ forcedsourcefieldmap[ field ] ]
                                    #=getattr( forced, forcedsourcefieldmap[ field ] )
                alert["prvDiaForcedSources"].append( newforced )
        # else:
        #     _logger.warn( "Not adding previous" )
        self.__class__._prvforcedsourcetime += time.perf_counter() - t0

        return alert


# ======================================================================
# Truth tables.  Of course, LSST won't really have these, but
# we have them for our simulation.

class BaseObjectTruth(Createable):

    # IMPORTANT: subclasses need a foreign key into the right diabobject table
    # diaobject = models.OneToOneField( BaseDiaObject, db_column='diaobject_id',
    #                                   on_delete=models.CASCADE, null=False, primary_key=True )
    libid = models.IntegerField( )
    sim_searcheff_mask = models.IntegerField( )
    gentype = models.IntegerField( db_index=True )
    sim_template_index = models.IntegerField( db_index=True )
    zcmb = Float32Field( db_index=True )
    zhelio = Float32Field( db_index=True )
    zcmb_smear = Float32Field( )
    ra = models.FloatField( )
    dec = models.FloatField( )
    mwebv = Float32Field( )
    galid = models.BigIntegerField( null=True )
    galzphot = Float32Field( null=True )
    galzphoterr = Float32Field( null=True )
    galsnsep = Float32Field( null=True )
    galsnddlr = Float32Field( null=True )
    rv = Float32Field( )
    av = Float32Field( )
    mu = Float32Field( )
    lensdmu = Float32Field( )
    peakmjd = Float32Field( db_index=True ) 
    mjd_detect_first = models.FloatField( db_index=True )
    mjd_detect_last = models.FloatField( db_index=True )
    dtseason_peak = Float32Field( )
    peakmag_u = Float32Field( )
    peakmag_g = Float32Field( )
    peakmag_r = Float32Field( )
    peakmag_i = Float32Field( )
    peakmag_z = Float32Field( )
    peakmag_y = Float32Field( )
    snrmax = Float32Field( )
    snrmax2 = Float32Field( )
    snrmax3 = Float32Field( )
    nobs = models.IntegerField( )
    nobs_saturate = models.IntegerField( )

    class Meta:
        abstract = True

    _pk = 'diaobject_id'
    _create_kws = [ 'diaobject_id', 'libid', 'sim_searcheff_mask', 'gentype', 'sim_template_index',
                    'zcmb', 'zhelio', 'zcmb_smear', 'ra', 'dec', 'mwebv', 'galid', 'galzphot',
                    'galzphoterr', 'galsnsep', 'galsnddlr', 'rv', 'av', 'mu', 'lensdmu', 'peakmjd',
                    'mjd_detect_first', 'mjd_detect_last', 'dtseason_peak', 'peakmag_u', 'peakmag_g',
                    'peakmag_r', 'peakmag_i', 'peakmag_z', 'peakmag_y', 'snrmax', 'snrmax2', 'snrmax3',
                    'nobs', 'nobs_saturate' ]
    _dict_kws = _create_kws

    # IMPORTANT : subclasses need the following line, with the right class name put in
    # _objectclass = BaseDiaObject

    # This is a little bit ugly.  For my own dubious reasons, I wanted
    # to be able to pass in things with diaobject_id that weren't
    # actually in the database (to save myself some pain on the other
    # end).  So, filter those out here before calling the Createable's
    # bulk_load_or_create
    @classmethod
    def bulk_load_or_create( cls, data, kwmap=None ):
        """Pass an array of dicts."""
        pks = [ i['diaobject_id'] for i in data ]
        diaobjs = list( cls._objectclass.objects.filter( pk__in=pks ) )
        objids = set( [ i.diaobject_id for i in diaobjs ] )
        datatoload = [ i for i in data if i['diaobject_id'] in objids ]
        if len(datatoload) > 0:
            return super().bulk_load_or_create( datatoload, kwmap=kwmap )
        else:
            return []


# ======================================================================
# ======================================================================
# ======================================================================
# PPDB simulation tables
#
# These have stuff that, in our simulaton, is currently at or will in
# the future be at the PPDB.  (We preload the tables with everything
# elasticc will do ahead of time, so the tables will hold things that
# are in the future relative to the current simulated date.)

# One thing I'm not clear on : how often will PPDB update the ra/dec
# fields of their objects?  Think.  For elasticc, this doesn't matter,
# because we don't model scatter of ra/decl.

class PPDBDiaObject(BaseDiaObject):

    class Meta(BaseDiaObject.Meta):
        abstract = False

# This class is also a simulation of the PPDB.  In actual LSST, we'll
# copy down all sources and forced sources for an object once that
# object is flagged by one of the brokers as something we're interested
# in.  (And, then, at some regular period, we'll have to get updates.)
class PPDBDiaSource(BaseDiaSource):
    diaobject = models.ForeignKey( PPDBDiaObject, db_column='diaobject_id',
                                   on_delete=models.CASCADE, null=True )

    class Meta(BaseDiaSource.Meta):
        abstract = False

# Same status as DiaSource (see comment above)
class PPDBDiaForcedSource(BaseDiaForcedSource):
    diaobject = models.ForeignKey( PPDBDiaObject, db_column='diaobject_id', on_delete=models.CASCADE )

    class Meta(BaseDiaForcedSource.Meta):
        abstract = False

# Alerts that will be sent out as a simulation of LSST alerts.
# All alerts to be sent are stored here.  If they have actually been
# sent, then alertSentTimestamp will be non-NULL.
#
# Find the current simulation time by finding the maximum midPointTai of
# all diaSource objects for alerts that have been sent.  (I should
# probably cache that somewhere, perhaps with a materialized view that I
# then update daily (or more often?))
class PPDBAlert(BaseAlert):
    diasource = models.ForeignKey( PPDBDiaSource, db_column='diasource_id',
                                   on_delete=models.CASCADE, null=True )
    diaobject = models.ForeignKey( PPDBDiaObject, db_column='diaobject_id',
                                   on_delete=models.CASCADE, null=True )

    class Meta(BaseAlert.Meta):
        abstract = False

    _objectclass = PPDBDiaObject
    _sourceclass = PPDBDiaSource
    _forcedsourceclass = PPDBDiaForcedSource

# ======================================================================

class DiaObjectTruth(BaseObjectTruth):
    diaobject = models.OneToOneField( PPDBDiaObject, db_column='diaobject_id',
                                      on_delete=models.CASCADE, null=False, primary_key=True )

    _objectclass = PPDBDiaObject

    class Meta(BaseObjectTruth.Meta):
        abstract = False


# ======================================================================
# ======================================================================
# ======================================================================
# Training set tables

class TrainingDiaObject(BaseDiaObject):
    class Meta(BaseDiaObject.Meta):
        abstract = False

class TrainingDiaSource(BaseDiaSource):
    diaobject = models.ForeignKey( TrainingDiaObject, db_column='diaobject_id',
                                   on_delete=models.CASCADE, null=True )

    class Meta(BaseDiaSource.Meta):
        abstract = False

class TrainingDiaForcedSource(BaseDiaForcedSource):
    diaobject = models.ForeignKey( TrainingDiaObject, db_column='diaobject_id', on_delete=models.CASCADE )

    class Meta(BaseDiaForcedSource.Meta):
        abstract = False

class TrainingAlert(BaseAlert):
    diasource = models.ForeignKey( TrainingDiaSource, db_column='diasource_id',
                                   on_delete=models.CASCADE, null=True )
    diaobject = models.ForeignKey( TrainingDiaObject, db_column='diaobject_id',
                                   on_delete=models.CASCADE, null=True )

    class Meta(BaseAlert.Meta):
        abstract = False

    _objectclass = TrainingDiaObject
    _sourceclass = TrainingDiaSource
    _forcedsourceclass = TrainingDiaForcedSource


class TrainingDiaObjectTruth(BaseObjectTruth):
    diaobject = models.OneToOneField( TrainingDiaObject, db_column='diaobject_id',
                                      on_delete=models.CASCADE, null=False, primary_key=True )

    _objectclass = TrainingDiaObject

    class Meta(BaseObjectTruth.Meta):
        abstract = False

# ======================================================================
# ======================================================================
# ======================================================================
# Classification translation between our taxonomy and the
# truth tables.  gentype is what shows up in the object
# truth table, and classId is what brokers tell us.  It's not a 1:1
# mapping, alas.
#
# If given a classId, to find all the corresponding gentypes, select
# from ClassIdofGentype.  For one- and two-digit classIds, this will
# return a lot of matches, as any gentype in that general category will
# be returned.  For one-digit classIds, "categorymatch" and "exactmatch"
# should both be false.  For two-digit classIds, "categorymatch" should
# be true and "exactmatch" should be false.  For three-digit classIds,
# both are true.  This table is most useful when trying to decide if
# there's a partial match to the truth table one- and two-digit
# classIds.
#
# The easier to use table is GentypeOfClassId.  gentype is NULL for one-
# and two-digit classIds in this table.  Non-null gentypes are unique in
# this table.  So, given a gentype, you can find the single exact
# classId that corresponds to that gentype.  However, multiple gentypes
# correspond to the same classId, so if you select on a three-digit
# classId, you may get multiple gentypes back.  This is the table to
# join to when trying to match three-digit classIds to the truth table.


class GentypeOfClassId(models.Model):
    id = models.AutoField( primary_key=True )
    classid = models.IntegerField( db_index=True )
    gentype = models.IntegerField( db_index=True, null=True, unique=True )
    description = models.TextField()

class ClassIdOfGentype(models.Model):
    id = models.AutoField( primary_key=True )
    gentype = models.IntegerField( db_index=True )
    classid = models.IntegerField( db_index=True )
    exactmatch = models.BooleanField( default=False )
    categorymatch = models.BooleanField( default=False )
    generalmatch = models.BooleanField( default=False )
    broadmatch = models.BooleanField( default=False )
    description = models.TextField()


# ======================================================================
# ======================================================================
# ======================================================================
# Local information.  These represent things that we know about thanks
# to having been alerted to them by a Broker.  When we hear about a new
# object or source from a broker, we will query the PPDB (i.e. the PPDB*
# tables) and create a new DiaObject or DiaSource as necessary.

class DiaObject(BaseDiaObject):

    class Meta(BaseDiaObject.Meta):
        abstract = False

class DiaSource(BaseDiaSource):
    diaobject = models.ForeignKey( DiaObject, db_column='diaobject_id',
                                   on_delete=models.CASCADE, null=True )

    class Meta(BaseDiaSource.Meta):
        abstract = False

class DiaForcedSource(BaseDiaForcedSource):
    diaobject = models.ForeignKey( DiaObject, db_column='diaobject_id', on_delete=models.CASCADE )

    class Meta(BaseDiaForcedSource.Meta):
        abstract = False

# Store linkages between TOM targets and DiaObject entries.  This could
# probably just be a nullable column in DiaObject (since it's 1:1), but
# I'm doing it this way so that the PPDB simulation tables are cleanly
# separated.
#
# I should probably put in a unique constraint?
#
# If I'm not mistaken, django automatically creates indexes for
# ForeignKey fields.
#
# If we did this the "pure TOM way", we would store all of the DiaObject
# information as extra data for a target.  However, I suspect that's not
# how we're really going to want to do it, because we want to have
# pre-existing structure, and not have to search a heterogeneous JSON
# dictionary for all of that (which I think is effectively what the
# target extra data is).  Does this subvert use of the TOM target UI and
# API interfaces, though?  Thought required.
class DiaObjectOfTarget(models.Model):
    diaobject = models.ForeignKey( DiaObject, db_column='diaobject_id', on_delete=models.CASCADE, null=False )
    tomtarget = models.ForeignKey( tom_targets.models.Target, db_column="tomtarget_id",
                                   on_delete=models.CASCADE, null=False )

    @classmethod
    def maybe_new_elasticc_targets( cls, objids, ras, decs ):
        """Given a list of objects (with coordinates), add new TOM targets for objects that don't already exist
        """
        # _logger.debug( f"objids={objids}" )
        preexisting = cls.objects.filter( diaobject_id__in=objids )
        preexistingids = [ o.diaobject_id for o in preexisting ]
        newobjs = [ ( objids[i], ras[i], decs[i] )
                    for i in range(len(objids))
                    if objids[i] not in preexistingids ]

        # NOTE : I could use Django's bulk_create() here in order to make
        # the database queries more efficient.  However, that would bypass
        # any hooks that tom_targets has added to its save() method,
        # which scares me.
        # _logger.debug( f'newobjs = {newobjs}' )
        newtargs = []
        newobjids = []
        for newobj in newobjs:
            targ = tom_targets.models.Target(
                name = f"ELAsTiCC2 {newobj[0]}",
                type = "SIDEREAL",
                ra = newobj[1],
                dec = newobj[2],
                epoch=2000
            )
            targ.save()
            newtargs.append( targ )
        _logger.debug( f'len(newtargs) = {len(newtargs)}' )
        # _logger.debug( f"Saved {len(newtargs)} new tom targets" )
        public = Group.objects.filter( name='Public' ).first()
        assign_perm( 'tom_targets.view_target', public, newtargs )

        newlinks = []
        for targ, newobj in zip( newtargs, newobjs ):
            newlinks.append( cls( diaobject_id=newobj[0],
                                  tomtarget_id=targ.id ) )
        if len(newlinks) > 0:
            addedlinks = cls.objects.bulk_create( newlinks )
            # _logger.debug( f"Bulk created {len(newlinks)} links" )


# ======================================================================
# Broker information

# Brokers send avro alerts (or the equivalent) with schema in:
# https://github.com/LSSTDESC/elasticc/blob/main/alert_schema/elasticc.v0_9.brokerClassification.avsc
#
# Each one of these alerts will be saved as a BrokerMessage
#
# The classifications array of that message will be saved as many rows to BrokerClassifications,
# creating new entries in BrokerClassifier as necessary.
#
# We will also call DiaObjectOfTarget.maybe_new_elasticc_targets the object in the broker message

class BrokerMessage(models.Model):
    """Model for the message attributes of an ELAsTiCC broker alert."""

    brokermessage_id = models.BigAutoField(primary_key=True)
    streammessage_id = models.BigIntegerField(null=True)
    topicname = models.CharField(max_length=200, null=True)

    alert_id = models.BigIntegerField()
    diasource_id = models.BigIntegerField()
    # diaSource = models.ForeignKey( DiaSource, on_delete=models.PROTECT, null=True )

    # timestamps as datetime.datetime (DateTimeField)
    msghdrtimestamp = models.DateTimeField(null=True)
    descingesttimestamp = models.DateTimeField(auto_now_add=True)  # auto-generated
    elasticcpublishtimestamp = models.DateTimeField(null=True)
    brokeringesttimestamp = models.DateTimeField(null=True)

    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index( fields=[ 'topicname', 'streammessage_id' ] ),
            models.Index( fields=[ 'alert_id' ] ),
            models.Index( fields=[ 'diasource_id' ] ),
        ]


    def to_dict( self ):
        resp = {
            'brokermessage_id': self.brokermessage_id,
            'streammessage_id': self.streammessage_id,
            'alert_id': self.alert_id,
            'diasource_id': self.diasource_id,
            'msghdrtimestamp': self.msghdrtimestamp.isoformat(),
            'descingesttimestamp': self.descingesttimestamp.isoformat(),
            'elasticcpublishtimestamp': int( self.elasticcpublishtimestamp.timestamp() * 1e3 ),
            'brokeringesttimestamp': int( self.brokeringesttimestamp.timestamp() * 1e3 ),
            'brokername': "<unknown>",
            'brokerversion': "<unknown>",
            'classifications': []
            }
        clsfctions = BrokerClassification.objects.all().filter( dbmessage=self )
        first = True
        for classification in clsfctions:
            clsifer = classification.classifier
            if first:
                resp['brokername'] = clsifer.brokername
                resp['brokerversion'] = clsifer.brokerversion
                first = False
            else:
                if ( ( clsifer.brokername != resp['brokername'] ) or
                     ( clsifer.brokerversion != resp['brokerversion'] ) ):
                    raise ValueError( "Mismatching brokername and brokerversion in the database! "
                                      "This shouldn't happen!" )
            resp['classifications'].append( { 'classifiername': clsifer.classifiername,
                                              'classifierparams': clsifer.classifierparams,
                                              'classid': classification.classid,
                                              'probability': classification.probability } )
        return resp


    @staticmethod
    def load_batch( messages, logger=_logger ):
        """Load an array of messages into BrokerMessage and associated tables.

        This is the ONLY way you should add BrokerMessages.  Creating one manually and
        using save will bypass the tom target creation.  I should really add a post-save
        hook to do that so it works, huh.  TODO ROB: figure out how to do that in django.

        messages is an array of dicts with keys topic, msgoffset, and msg.
        topic is string, msgoffset is an integer, and msg is a dict that should
        match the elasticc.v0_9.brokerClassification.avsc schema

        Returns information about numbers of things actually added.
        """

        # It's a hard problem to decide if a message already
        # exists.  The things that are in the BrokerMessage object
        # could (by chance) be duplicated for different brokers.
        # I need to think about this, but for now I'm going to assume
        # that all messages coming in are new messages.  In pratice,
        # this does mean that we get duplicate messages, but we'll
        # just have to deal with that when processing the data.

        # NOTE: See https://docs.djangoproject.com/en/3.0/ref/models/querysets/#bulk-create
        # the caveats on bulk-create.  I'm assuming that addedmsgs will have the right
        # value of brokermessage_id.  According to that page, this is only true for
        # Postgres... which is what I'm using...

        logger.debug( f'In BrokerMessage.load_batch, received {len(messages)} messages.' );

        messageobjects = {}
        kwargses = []
        sourceids = set()
        utc = pytz.timezone( "UTC" )
        for msg in messages:
            timestamp = msg['timestamp']
            if len( msg['msg']['classifications'] ) == 0:
                logger.debug( "Message with no classifications" )
                continue
            keymess = ( f"{msg['msgoffset']}_{msg['topic']}_{msg['msg']['alertId']}" )
            if keymess not in messageobjects.keys():
                kwargs = { 'streammessage_id': msg['msgoffset'],
                           'topicname': msg['topic'],
                           'alert_id': msg['msg']['alertId'],
                           'diasource_id': msg['msg']['diaSourceId'],
                           'msghdrtimestamp': timestamp,
                           'descingesttimestamp': datetime.datetime.now(),
                           'elasticcpublishtimestamp': msg['msg']['elasticcPublishTimestamp'],
                           'brokeringesttimestamp': msg['msg']['brokerIngestTimestamp'],
                }
                kwargses.append( kwargs )
                sourceids.add( msg['msg']['diaSourceId'] )
            else:
                logger.error( f'Key {keymess} showed up more than once in a message batch!' )
        logger.debug( f'Adding {len(sourceids)} sourceids to the brokersourceids table' )
        BrokerSourceIds.add_batch( sourceids )
        logger.debug( f'Bulk creating {len(kwargses)} messages.' )
        if len(kwargses) > 0:
            # This is byzantine, but I'm copying django documentation here
            objs = ( BrokerMessage( **k ) for k in kwargses )
            batch = list( itertools.islice( objs, len(kwargses) ) )
            if batch is None:
                raise RunTimeError( "Something bad has happened." )
            addedmsgs = BrokerMessage.objects.bulk_create( batch, len(kwargses) )
            for addedmsg in addedmsgs:
                keymess = f"{addedmsg.streammessage_id}_{addedmsg.topicname}_{addedmsg.alert_id}"
                messageobjects[ keymess ] = addedmsg
        else:
            addedmsgs = []


        # Figure out which classifiers already exist.
        # I need to figure out if there's a way to tell Django
        # to do a WHERE...IN on tuples.  For now, hopefully this
        # Q object thing won't be a disaster

        classifiers = {}

        logger.debug( f"Looking for pre-existing classifiers" )
        cferconds = models.Q()
        i = 0
        condcache = set()
        for msg in messages:
            i += 1
            sigstr = ( f"{msg['msg']['brokerName']}_{msg['msg']['brokerVersion']}_"
                       f"{msg['msg']['classifierName']}_{msg['msg']['classifierParams']}" )
            if sigstr not in condcache:
                newcond = ( models.Q( brokername = msg['msg']['brokerName'] ) &
                            models.Q( brokerversion = msg['msg']['brokerVersion'] ) &
                            models.Q( classifiername = msg['msg']['classifierName'] ) &
                            models.Q( classifierparams = msg['msg']['classifierParams'] ) )
                cferconds |= newcond
            condcache.add( sigstr )
        curcfers = BrokerClassifier.objects.filter( cferconds )
        for cur in curcfers:
            keycfer = ( f"{cur.brokername}_{cur.brokerversion}_"
                        f"{cur.classifiername}_{cur.classifierparams}" )
            classifiers[ keycfer ] = cur
        logger.debug( f'Found {len(classifiers)} existing classifiers.' )

        # Create new classifiers as necessary

        addedkeys = set()
        kwargses = []
        for msg in messages:
            keycfer = ( f"{msg['msg']['brokerName']}_{msg['msg']['brokerVersion']}_"
                        f"{msg['msg']['classifierName']}_{msg['msg']['classifierParams']}" )
            if ( keycfer not in classifiers.keys() ) and ( keycfer not in addedkeys ):
                kwargses.append( { 'brokername': msg['msg']['brokerName'],
                                   'brokerversion': msg['msg']['brokerVersion'],
                                   'classifiername': msg['msg']['classifierName'],
                                   'classifierparams': msg['msg']['classifierParams'] } )
                addedkeys.add( keycfer )
        ncferstoadd = len(kwargses)
        logger.debug( f'Adding {ncferstoadd} new classifiers.' )
        if ncferstoadd > 0:
            objs = ( BrokerClassifier( **k ) for k in kwargses )
            batch = list( itertools.islice( objs, len(kwargses) ) )
            newcfers = BrokerClassifier.objects.bulk_create( batch, len(kwargses) )
            for curcfer in newcfers:
                keycfer = ( f"{curcfer.brokername}_{curcfer.brokerversion}_"
                            f"{curcfer.classifiername}_{curcfer.classifierparams}" )
                classifiers[ keycfer ] = curcfer
                # logger.debug( f'key: {keycfer}; brokerName: {curcfer.brokerName}; '
                #                f'brokerVersion: {curcfer.brokerVersion}; classifierName: {curcfer.ClassifierName}; '
                #                f'classifierParams: {curcfer.classifierParams}' )


        # Add the new classifications
        #
        # ROB TODO : think about duplication!  Right now I'm just
        # assuming I won't get any.

        kwargses = []
        for msg in messages:
            if len( msg['msg']['classifications'] ) == 0:
                continue
            keymess = ( f"{msg['msgoffset']}_{msg['topic']}_{msg['msg']['alertId']}" )
            keycfer = ( f"{msg['msg']['brokerName']}_{msg['msg']['brokerVersion']}_"
                        f"{msg['msg']['classifierName']}_{msg['msg']['classifierParams']}" )
            for cfication in msg['msg']['classifications']:
                kwargs = { 'dbmessage': messageobjects[keymess],
                           'classifier_id': classifiers[keycfer].classifier_id,
                           'classid': cfication['classId'],
                           'probability': cfication['probability'] }
                kwargses.append( kwargs )
                # logger.debug( f"Adding {kwargs}" )
        logger.debug( f'Adding {len(kwargses)} new classifications.' )
        objs = ( BrokerClassification( **k ) for k in kwargses )
        batch = list( itertools.islice( objs, len(kwargses) ) )
        newcfications = BrokerClassification.objects.bulk_create( batch, len(kwargses) )

        # return newcfications
        return { "addedmsgs": len(addedmsgs),
                 "addedclassifiers": ncferstoadd,
                 "addedclassifications": len(newcfications),
                 "firstbrokermessage_id": None if len(addedmsgs)==0 else addedmsgs[0].brokermessage_id }



class BrokerClassifier(models.Model):
    """Model for a classifier producing an ELAsTiCC broker classification."""

    classifier_id = models.BigAutoField(primary_key=True, db_index=True)

    brokername = models.CharField(max_length=100)
    brokerversion = models.TextField(null=True)     # state changes logically not part of the classifier
    classifiername = models.CharField(max_length=200)
    classifierparams = models.TextField(null=True)   # change in classifier code / parameters

    modified = models.DateTimeField(auto_now=True)

    class Meta:
        # This may be overdone, since this table won't ever be that long (hundreds or so)
        indexes = [
            models.Index(fields=["brokername"]),
            models.Index(fields=["brokername", "brokerversion"]),
            models.Index(fields=["brokername", "classifiername"]),
            models.Index(fields=["brokername", "brokerversion", "classifiername", "classifierparams"]),
        ]

class BrokerClassification(psqlextra.models.PostgresPartitionedModel):
    """Model for a classification from an ELAsTiCC broker."""

    class PartitioningMeta:
        method = psqlextra.types.PostgresPartitioningMethod.LIST
        key = [ 'classifier_id' ]

    class Meta:
        constraints = [
            models.UniqueConstraint(
                name="unique_constarint_elasticc2_brokerclassification_partitionkey",
                fields=( 'classifier_id', 'classification_id' )
            ),
        ]

    classification_id = models.BigAutoField(primary_key=True)
    dbmessage = models.ForeignKey( BrokerMessage, db_column='brokermessage_id', on_delete=models.CASCADE, null=True )
    # I really want to make a foreign key here, but I haven't figured
    # out how to get Django to succesfully create a unique
    # constraint and a partition on a foreign key.  Either I get try to
    # partition on "classifier_id" and I get an error from Django saying
    # that that field doesn't exist, or I try to partition on
    # "classifier" and I get an error from Postgres saying that that
    # field doesn't exist.  (ORM considered harmful.)
    # classifier = models.ForeignKey( BrokerClassifier, db_column='classifier_id',
    #                                   on_delete=models.CASCADE, null=True )
    classifier_id = models.BigIntegerField()
    # These next three can be determined by looking back at the linked dbMessage
    # alert_id = models.BigIntegerField()
    # diaobject_id = models.BigIntegerField()
    # diasource_id = models.ForeignKey( DiaSource, on_delete=models.PROTECT, null=True )

    classid = models.IntegerField( db_index=True )
    probability = Float32Field()

    # JSON blob of additional information from the broker?
    # Here or in a separate table?
    # (As is, the schema doesn't define such a thing.)

    # Don't need this, timestamps are all in the brokermessage table
    # And, this table will have many rows, so we want to keep it skinny
    # modified = models.DateTimeField(auto_now=True)


# This table is intended to be wiped out all the time.  When broker
# messages are added, the list of diaSourceId will be added to this
# table.  The nightly job that copies sources and objects from PPDBDia*
# to Dia* will look at this table to figure out what it needs to
# consider, to avoid having to look at the brobdingnagnian
# classification table.

class BrokerSourceIds(models.Model):
    """Temporary (not formally) table for keeping sources we heard about from brokers"""

    diasource_id = models.BigIntegerField( primary_key=True, unique=True, db_index=True )

    @classmethod
    def add_batch( cls, sources ):
        if len(sources) == 0:
            return
        objs = [ BrokerSourceIds( i ) for i in sources ]
        addedsources = BrokerSourceIds.objects.bulk_create( objs, len(objs), ignore_conflicts=True )



class CassBrokerMessage(DjangoCassandraModel):
    diasource_id = columns.BigInt( primary_key=True )
    classifier_id = columns.BigInt( primary_key=True )
    id = columns.UUID( primary_key=True, default=uuid.uuid4 )

    topicname = columns.Text()
    streammessage_id = columns.BigInt()
    alert_id = columns.BigInt()
    msghdrtimestamp = columns.DateTime()
    elasticcpublishtimestamp = columns.DateTime()
    brokeringesttimestamp = columns.DateTime()
    descingesttimestamp = columns.DateTime( default=datetime.datetime.utcnow )
    classid = columns.List( columns.Integer() )
    probability = columns.List( columns.Float() )

    class Meta:
        get_pk_field = 'id'

    @staticmethod
    def load_batch( messages, logger=_logger ):
        """Load an array of broker classification messages.

        This doesn't actually do any batching operation, because there's
        no bulk_create in the Django Cassandra interface, and because I
        don't understand Cassandra well enough to know how to do this --
        I've read that batching can be a bad idea.  I'm worried about
        the repeated network overhead, but we'll see how it goes.

        """

        cfers = {}
        sourceids = []
        for i, msgmeta in enumerate(messages):
            msg = msgmeta['msg']
            if len( msg['classifications'] ) == 0:
                logger.debug( 'Message with no classifications' )
                continue
            keycfer = f"{msg['brokerName']}_{msg['brokerVersion']}_{msg['classifierName']}_{msg['classifierParams']}"
            if keycfer not in cfers.keys():
                cfers[ keycfer ] = { 'brokername': msg['brokerName'],
                                     'brokerversion': msg['brokerVersion'],
                                     'classifiername': msg['classifierName'],
                                     'classifierparams': msg['classifierParams'],
                                     'classifier_id': None }
            sourceids.append( msg['diaSourceId'] )

        # Create any classifiers that don't already exist; this
        # is one place where we do get efficiency by calling
        # this batch method.
        cferconds = models.Q()
        logger.debug( f"Looking for pre-existing classifiers" )
        cferconds = models.Q()
        i = 0
        for cferkey, cfer in cfers.items():
            newcond = ( models.Q( brokername = cfer['brokername'] ) &
                        models.Q( brokerversion = cfer['brokerversion'] ) &
                        models.Q( classifiername = cfer['classifiername'] ) &
                        models.Q( classifierparams = cfer['classifierparams'] ) )
            cferconds |= newcond
        curcfers = BrokerClassifier.objects.filter( cferconds )
        numknown = 0
        for cur in curcfers:
            keycfer = f"{cur.brokername}_{cur.brokeversion}_{cur.classifiername}_{cur.classifierparms}"
            cfers[ keycfer ][ 'classifier_id' ] = cur.classifier_id
            numknown += 1
        logger.debug( f'Found {numknown} existing classifiers that match the ones in this batch.' )

        # Create new classifiers as necessary

        kwargses = []
        ncferstoadd = 0
        for keycfer, cfer in cfers.items():
            if cfer[ 'classifier_id' ] is None:
                kwargses.append( { 'brokername': cfer['brokername'],
                                   'brokerversion': cfer['brokerversion'],
                                   'classifiername': cfer['classifiername'],
                                   'classifierparams': cfer['classifierparams'] } )
                ncferstoadd += 1
        ncferstoadd = len(kwargses)
        logger.debug( f'Adding {ncferstoadd} new classifiers.' )
        if ncferstoadd > 0:
            objs = ( BrokerClassifier( **k ) for k in kwargses )
            batch = list( itertools.islice( objs, len(kwargses) ) )
            newcfers = BrokerClassifier.objects.bulk_create( batch, len(kwargses) )
            for newcfer in newcfers:
                keycfer = ( f'{newcfer.brokername}_{newcfer.brokerversion}_'
                            f'{newcfer.classifiername}_{newcfer.classifierparams}' )
                cfers[keycfer]['classifier_id'] = newcfer.classifier_id

        for i, msgmeta in enumerate( messages ):
            msg = msgmeta['msg']
            if len( msg['classifications' ] ) == 0:
                    continue
            keycfer = f"{msg['brokerName']}_{msg['brokerVersion']}_{msg['classifierName']}_{msg['classifierParams']}"
            classes = []
            probs = []
            for cification in msg['classifications']:
                classes.append( cification['classId'] )
                probs.append( cification['probability'] )
            cassmsg = CassBrokerMessage(
                diasource_id=msg['diaSourceId'],
                classifier_id=cfers[ keycfer ][ 'classifier_id' ],
                topicname=msgmeta['topic'],
                streammessage_id=msgmeta['msgoffset'],
                alert_id=msg['alertId'],
                msghdrtimestamp=msgmeta['timestamp'],
                elasticcpublishtimestamp=msg['elasticcPublishTimestamp'],
                brokeringesttimestamp=msg['brokerIngestTimestamp'],
                classid=classes,
                probability=probs
            )
            cassmsg.save()

        # Update the log of new broker source ids
        BrokerSourceIds.add_batch( sourceids )

        logger.debug( f"Classifiers in the messages lost loaded: {cfers.keys()}" )


        # return newcfications
        return { "addedmsgs": len(messages),
                 "addedclassifiers": ncferstoadd,
                 "addedclassifications": None,
                 "firstbrokermessage_id": None }




# This is a thing I use as a "don't run twice at once" lock

class ImportPPDBRunning(models.Model):
    running = models.BooleanField( default=False )
