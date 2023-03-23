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
from django.db import models

# NOTE FOR ROB
#
# Schema are mostly the same as for elasticc.
# However, I'm not inheriting, because I think that would
# break things horribly.  All the refernces to other models
# in what I inherit is probably going to be in the elasticc
# namespace, not in the elasticc2 namespace, which will cause
# no end of confusion.
#
# So, lots of copied code.


# Support for the Q3c indexing scheme, index names up to 63 characters
# (django limits to 30, postgres has more), and the Creatable base class
# that defines the "create" and "load_or_create" methods for bulk
# upserting.  Classes that derive from this must define _create_kws and
# _pk.  See elasticc/models.py for examples.
from elasticc.models import q3c_ang2ipix, LongNameBTreeIndex, Createable
import tom_targets.models

_logger = logging.getLogger(__name__)
_logout = logging.StreamHandler( sys.stderr )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' )
_logout.setFormatter( _formatter )
_logger.propagate = False
_logger.addHandler( _logout )
_logger.setLevel( logging.INFO )
# _logger.setLevel( logging.DEBUG )

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
class DiaObject(Createable):
    diaObjectId = models.BigIntegerField( primary_key=True, unique=True, db_index=True )
    simVersion = models.TextField( null=True )
    ra = models.FloatField( )
    decl = models.FloatField( )
    mwebv = models.FloatField( null=True )
    mwebv_err = models.FloatField( null=True )
    z_final = models.FloatField( null=True )
    z_final_err = models.FloatField( null=True )
    hostgal_ellipticity = models.FloatField( null=True )
    hostgal_sqradius = models.FloatField( null=True )
    hostgal_zspec = models.FloatField( null=True )
    hostgal_zspec_err = models.FloatField( null=True )
    hostgal_zphot = models.FloatField( null=True )
    hostgal_zphot_err = models.FloatField( null=True )
    hostgal_zphot_q000 = models.FloatField( null=True)
    hostgal_zphot_q010 = models.FloatField( null=True )
    hostgal_zphot_q020 = models.FloatField( null=True )
    hostgal_zphot_q030 = models.FloatField( null=True )
    hostgal_zphot_q040 = models.FloatField( null=True )
    hostgal_zphot_q050 = models.FloatField( null=True )
    hostgal_zphot_q060 = models.FloatField( null=True )
    hostgal_zphot_q070 = models.FloatField( null=True )
    hostgal_zphot_q080 = models.FloatField( null=True )
    hostgal_zphot_q090 = models.FloatField( null=True )
    hostgal_zphot_q100 = models.FloatField( null=True )
    hostgal_zphot_p50 = models.FloatField( null=True )
    hostgal_mag_u = models.FloatField( null=True )
    hostgal_mag_g = models.FloatField( null=True )
    hostgal_mag_r = models.FloatField( null=True )
    hostgal_mag_i = models.FloatField( null=True )
    hostgal_mag_z = models.FloatField( null=True )
    hostgal_mag_Y = models.FloatField( null=True )
    hostgal_ra = models.FloatField( null=True )
    hostgal_dec = models.FloatField( null=True )
    hostgal_snsep = models.FloatField( null=True )
    hostgal_magerr_u = models.FloatField( null=True )
    hostgal_magerr_g = models.FloatField( null=True )
    hostgal_magerr_r = models.FloatField( null=True )
    hostgal_magerr_i = models.FloatField( null=True )
    hostgal_magerr_z = models.FloatField( null=True )
    hostgal_magerr_Y = models.FloatField( null=True )
    hostgal2_ellipticity = models.FloatField( null=True )
    hostgal2_sqradius = models.FloatField( null=True )
    hostgal2_zspec = models.FloatField( null=True )
    hostgal2_zspec_err = models.FloatField( null=True )
    hostgal2_zphot = models.FloatField( null=True )
    hostgal2_zphot_err = models.FloatField( null=True )
    hostgal2_zphot_q000 = models.FloatField( null=True )
    hostgal2_zphot_q010 = models.FloatField( null=True )
    hostgal2_zphot_q020 = models.FloatField( null=True )
    hostgal2_zphot_q030 = models.FloatField( null=True )
    hostgal2_zphot_q040 = models.FloatField( null=True )
    hostgal2_zphot_q050 = models.FloatField( null=True )
    hostgal2_zphot_q060 = models.FloatField( null=True )
    hostgal2_zphot_q070 = models.FloatField( null=True )
    hostgal2_zphot_q080 = models.FloatField( null=True )
    hostgal2_zphot_q090 = models.FloatField( null=True )
    hostgal2_zphot_q100 = models.FloatField( null=True )
    hostgal2_zphot_p50 = models.FloatField( null=True )
    hostgal2_mag_u = models.FloatField( null=True )
    hostgal2_mag_g = models.FloatField( null=True )
    hostgal2_mag_r = models.FloatField( null=True )
    hostgal2_mag_i = models.FloatField( null=True )
    hostgal2_mag_z = models.FloatField( null=True )
    hostgal2_mag_Y = models.FloatField( null=True )
    hostgal2_ra = models.FloatField( null=True )
    hostgal2_dec = models.FloatField( null=True )
    hostgal2_snsep = models.FloatField( null=True )
    hostgal2_magerr_u = models.FloatField( null=True )
    hostgal2_magerr_g = models.FloatField( null=True )
    hostgal2_magerr_r = models.FloatField( null=True )
    hostgal2_magerr_i = models.FloatField( null=True )
    hostgal2_magerr_z = models.FloatField( null=True )
    hostgal2_magerr_Y = models.FloatField( null=True )

    class Meta:
        indexes = [
            LongNameBTreeIndex( q3c_ang2ipix( 'ra', 'decl' ),
                                name='idx_%(app_label)s_%(class)s_q3c' ),
        ]

    _pk = 'diaObjectId'
    _create_kws = [ 'diaObjectId', 'simVersion', 'ra', 'decl', 'mwebv', 'mwebv_err', 'z_final', 'z_final_err' ]
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
        _create_kws.append( f'hostgal{_gal}_zphot_p50' )
        for _phot in [ 'q000', 'q010', 'q020', 'q030', 'q040', 'q050', 'q060', 'q070', 'q080', 'q090', 'q100' ]:
            _create_kws.append( f'hostgal{_gal}_zphot_{_phot}' )
        for _band in [ 'u', 'g', 'r', 'i', 'z', 'Y' ]:
            for _err in [ '', 'err' ]:
                _create_kws.append( f'hostgal{_gal}_mag{_err}_{_band}' )

    _dict_kws = _create_kws


# This class is also a simulation of the PPDB.  In actual LSST, we'll
# copy down all sources and forced sources for an object once that
# object is flagged by one of the brokers as something we're interested
# in.  (And, then, at some regular period, we'll have to get updates.)
# For now, rather than having a second table to simulate this copying,
# the simulation of that copying will just be: look at all sources from
# before the current simulated time for objects that has a TOM target.
class DiaSource(Createable):
    diaSourceId = models.BigIntegerField( primary_key=True, unique=True, db_index=True )
    ccdVisitId = models.BigIntegerField( )
    diaObject = models.ForeignKey( DiaObject, db_column='diaObjectId', on_delete=models.CASCADE, null=True )
    # I'm not using a foreign key for parentDiaSource to allow things to be loaded out of order
    parentDiaSourceId = models.BigIntegerField( null=True )
    midPointTai = models.FloatField( db_index=True )
    filterName = models.TextField()
    ra = models.FloatField( )
    decl = models.FloatField( )
    psFlux = models.FloatField()
    psFluxErr = models.FloatField()
    snr = models.FloatField( )
    nobs = models.FloatField( null=True )

    class Meta:
        indexes = [
            LongNameBTreeIndex( q3c_ang2ipix( 'ra', 'decl' ),
                                name='idx_%(app_label)s_%(class)s_q3c' ),
        ]
    
    _pk = 'diaSourceId'
    _create_kws = [ 'diaSourceId', 'ccdVisitId', 'diaObject', 'parentDiaSourceId',
                    'midPointTai', 'filterName', 'ra', 'decl', 'psFlux', 'psFluxErr', 'snr', 'nobs' ]
    _dict_kws = [ 'diaObjectId' if i == 'diaObject' else i for i in _create_kws ]
    _irritating_django_id_map = { 'diaObjectId': 'diaObject_id' }

    
# Same status as DiaSource (see comment above)
class DiaForcedSource(Createable):
    diaForcedSourceId = models.BigIntegerField( primary_key=True, unique=True, db_index=True )
    ccdVisitId = models.BigIntegerField( )
    diaObject = models.ForeignKey( DiaObject, db_column='diaObjectId', on_delete=models.CASCADE )
    midPointTai = models.FloatField( db_index=True )
    filterName = models.TextField()
    psFlux = models.FloatField()
    psFluxErr = models.FloatField()
    totFlux = models.FloatField()
    totFluxErr = models.FloatField()

    _pk = 'diaForcedSourceId'
    _create_kws = [ 'diaForcedSourceId', 'ccdVisitId', 'diaObject',
                    'midPointTai', 'filterName', 'psFlux', 'psFluxErr', 'totFlux', 'totFluxErr' ]
    _dict_kws = [ 'diaObjectId' if i == 'diaObject' else i for i in _create_kws ]
    _irritating_django_id_map = { 'diaObjectId': 'diaObject_id' }

# Alerts that will be sent out as a simulation of LSST alerts.
# All alerts to be sent are stored here.  If they have actually been
# sent, then alertSentTimestamp will be non-NULL.
#
# Find the current simulation time by finding the maximum midPointTai of
# all diaSource objects for alerts that have been sent.  (I should
# probably cache that somewhere, perhaps with a materialized view that I
# then update daily (or more often?))
class DiaAlert(Createable):
    alertId = models.BigIntegerField( primary_key=True, unique=True, db_index=True )
    alertSentTimestamp = models.DateTimeField( null=True, db_index=True )
    diaSource = models.ForeignKey( DiaSource, db_column='diaSourceId', on_delete=models.CASCADE, null=True )
    diaObject = models.ForeignKey( DiaObject, db_column='diaObjectId', on_delete=models.CASCADE, null=True )
    # cutoutDifference
    # cutoutTemplate

    _pk = 'alertId'
    _create_kws = [ 'alertId', 'diaSource', 'diaObject' ]
    _dict_kws = [ 'alertId', 'diaSourceId', 'diaObjectId', 'alertSentTimestamp' ]
    _irritating_django_id_map = { 'diaObjectId': 'diaObject_id',
                                  'diaSourceId': 'diaSource_id' }

    def reconstruct( self ):
        """Reconstruct the dictionary that represents this alert.
        
        It's not just a matter of dumping fields, as it also has to decide if the alert
        should include previous photometry and previous forced photometry, and then
        has to pull all that from the database.
        """
        alert = { "alertId": self.alertId,
                  "diaSource": {},
                  "prvDiaSources": [],
                  "prvDiaForcedSources": [],
                  "prvDiaNondetectionLimits": [],
                  "diaObject": {},
                  "cutoutDifference": None,
                  "cutoutTemplate": None
                 }

        sourcefields = [ "diaSourceId", "ccdVisitId", "parentDiaSourceId", "midPointTai",
                        "filterName", "ra", "decl", "psFlux", "psFluxErr", "snr", "nobs" ]
        
        for field in sourcefields:
            alert["diaSource"][field] = getattr( self.diaSource, field )
        # Special handling since django insists on naming the object field
        # differently from the database column I told it to use
        alert["diaSource"]["diaObjectId"] = self.diaSource.diaObject_id

        objectfields = [ "diaObjectId", "simVersion", "ra", "decl", "mwebv", "mwebv_err",
                         "z_final", "z_final_err" ]
        for suffix in [ "", "2" ]:
            for hgfield in [ "ellipticity", "sqradius", "zspec", "zspec_err", "zphot", "zphot_err",
                             "zphot_q000", "zphot_q010", "zphot_q020", "zphot_q030", "zphot_q040",
                             "zphot_q050", "zphot_q060", "zphot_q070", "zphot_q080", "zphot_q090",
                             "zphot_q100", "zphot_p50",
                             "mag_u", "mag_g", "mag_r", "mag_i", "mag_z", "mag_Y",
                             "ra", "dec", "snsep",
                             "magerr_u", "magerr_g", "magerr_r", "magerr_i", "magerr_z", "magerr_Y" ]:
                objectfields.append( f"hostgal{suffix}_{hgfield}" )
        for field in objectfields:
            alert["diaObject"][field] = getattr( self.diaObject, field )
        
        objsources = DiaSource.objects.filter( diaObject_id=self.diaSource.diaObject_id ).order_by( "midPointTai" )
        for prevsource in objsources:
            if prevsource.diaSourceId == self.diaSource.diaSourceId: break
            newprevsource = {}
            for field in sourcefields:
                newprevsource[field] = getattr( prevsource, field )
            newprevsource["diaObjectId"] = self.diaSource.diaObject_id
            alert["prvDiaSources"].append( newprevsource )

        # If this source is the same night as the original detection, then
        # there will be no forced source information
        # A THING.  I had *thought* that the forced source table was a superset
        # of the source table for elasticc, but it turns out practically speaking
        # that that's not the case; investigation required.  
        if self.diaSource.midPointTai - objsources[0].midPointTai > 0.5:
            objforced = DiaForcedSource.objects.filter( diaObject_id=self.diaSource.diaObject_id,
                                                        midPointTai__gte=objsources[0].midPointTai-30.,
                                                        midPointTai__lt=self.diaSource.midPointTai )
            # _logger.warn( f"Found {len(objforced)} previous" )
            for forced in objforced:
                newforced = {}
                for field in [ "diaForcedSourceId", "ccdVisitId", "midPointTai",
                               "filterName", "psFlux", "psFluxErr", "totFlux", "totFluxErr" ]:
                    newforced[field] = getattr( forced, field )
                newforced["diaObjectId"] = forced.diaObject_id
                alert["prvDiaForcedSources"].append( newforced )
        # else:
        #     _logger.warn( "Not adding previous" )
                

        return alert
                  
    
# ======================================================================
# Truth tables.  Of course, LSST PPDB won't really have these, but
# we have them for our simulation.

class DiaTruth(models.Model):
    # I can't use a foreign key constraint here because there will be truth entries for
    # sources for which there was no alert, and as such which will not be in the
    # DiaSource table.  But, DiaSource will be unique, so make it the primary key.
    diaSourceId = models.BigIntegerField( primary_key=True )
    diaObjectId = models.BigIntegerField( null=True, db_index=True )
    mjd = models.FloatField( null=True )
    detect = models.BooleanField( null=True )
    gentype = models.IntegerField( null=True )
    genmag = models.FloatField( null=True )

    # I'm not making DiaTruth a subclass of Creatable here because the data coming
    #   in doesn't have the right keywords, and because I need to do some custom
    #   checks for existence of stuff in other tables.

    def to_dict( self ):
        return { 'diaSourceId': self.diaSourceId,
                 'diaObjectId': self.diaObjectId,
                 'mjd': self.mjd,
                 'detect': self.detect,
                 'gentype': self.gentype,
                 'genmag': self.genmag }
    
    @staticmethod
    def create( data ):
        try:
            source = DiaSource.objects.get( diaSourceId=data['SourceID'] )
            if source.diaObject_id != data['SNID']:
                raise ValueError( f"SNID {data['SNID']} doesn't match "
                                  f"diaSource diaObject_id {source.diaObject_id} "
                                  f"for diaSource {source.diaSourceId}" )
            if math.fabs( float( data['MJD'] - source.midPointTai ) > 0.01 ):
                raise ValueError( f"MJD {data['MJD']} doesn't match "
                                  f"diaSource midPointTai {source.midPointTai} "
                                  f"for diaSource {source.diaSoruceId}" )
        except DiaSource.DoesNotExist:
            if data['DETECT']:
                raise ValueError( f'No SourceID {data["SourceID"]} for a DETECT=true truth entry' )
        curtruth = DiaTruth(
            diaSourceId = int( data['SourceID'] ),
            diaObjectId = int( data['SNID'] ),
            detect = bool( data['DETECT'] ),
            mjd = float( data['MJD'] ),
            gentype = int( data['TRUE_GENTYPE'] ),
            genmag = float( data['TRUE_GENMAG'] )
        )
        curtruth.save()
        return curtruth
    
    @staticmethod
    def load_or_create( data ):
        try:
            curtruth = DiaTruth.objects.get( diaSourceId=data['SourceID'] )
            # VERIFY THAT STUFF MATCHES?????
            return curtruth
        except DiaTruth.DoesNotExist:
            return DiaTruth.create( data )

    @staticmethod
    def bulk_load_or_create( data ):
        """Pass a list of dicts."""
        dsids = [ i['SourceID'] for i in data ]
        curobjs = list( DiaTruth.objects.filter( diaSourceId__in=dsids ) )
        exists = set( [ i.diaSourceId for i in curobjs ] )
        sources = set( DiaSource.objects.values_list( 'diaSourceId', flat=True ).filter( diaSourceId__in=dsids ) )
        newobjs = set()
        missingsources = set()
        for newdata in data:
            if newdata['SourceID'] in exists:
                continue
            if newdata['SourceID'] not in sources and newdata['DETECT']:
                missingsources.add( newdata['SourceID'] )
                continue
            # ROB : you don't verify that the diaSourceId exists in the source table!
            newobjs.add( DiaTruth( diaSourceId = int( newdata['SourceID'] ),
                                   diaObjectId = int( newdata['SNID'] ),
                                   detect = bool( newdata['DETECT'] ),
                                   mjd = float( newdata['MJD'] ),
                                   gentype = int( newdata['TRUE_GENTYPE'] ),
                                   genmag = float( newdata['TRUE_GENMAG'] ) ) )
        if len(newobjs) > 0:
            addedobjs = DiaTruth.objects.bulk_create( newobjs )
            curobjs.extend( addedobjs )
        return curobjs, missingsources


class DiaObjectTruth(Createable):
    diaObject = models.OneToOneField( DiaObject, db_column='diaObjectId',
                                      on_delete=models.CASCADE, null=False, primary_key=True )
    libid = models.IntegerField( )
    sim_searcheff_mask = models.IntegerField( )
    gentype = models.IntegerField( db_index=True )
    sim_template_index = models.IntegerField( db_index=True )
    zcmb = models.FloatField( db_index=True )
    zhelio = models.FloatField( db_index=True )
    zcmb_smear = models.FloatField( )
    ra = models.FloatField( )
    dec = models.FloatField( )
    mwebv = models.FloatField( )
    galnmatch = models.IntegerField( )
    galid = models.BigIntegerField( null=True )
    galzphot = models.FloatField( null=True )
    galzphoterr = models.FloatField( null=True )
    galsnsep = models.FloatField( null=True )
    galsnddlr = models.FloatField( null=True )
    rv = models.FloatField( )
    av = models.FloatField( )
    mu = models.FloatField( )
    lensdmu = models.FloatField( )
    peakmjd = models.FloatField( db_index=True ) 
    mjd_detect_first = models.FloatField( db_index=True )
    mjd_detect_last = models.FloatField( db_index=True )
    dtseason_peak = models.FloatField( )
    peakmag_u = models.FloatField( )
    peakmag_g = models.FloatField( )
    peakmag_r = models.FloatField( )
    peakmag_i = models.FloatField( )
    peakmag_z = models.FloatField( )
    peakmag_Y = models.FloatField( )
    snrmax = models.FloatField( )
    snrmax2 = models.FloatField( )
    snrmax3 = models.FloatField( )
    nobs = models.IntegerField( )
    nobs_saturate = models.IntegerField( )

    # django insists on making the object field diaObject_id even though
    # I told it to make the column diaObjectId.  This is because it is
    # under the standard ORM presumption that it is enough... that it
    # can completely obscure the SQL and nobody will ever want to go
    # directly to the SQL.  That is, of course, wrong-- of *course*
    # we're going to want to use SQL directly to access the datbase (and
    # so we get back into all of my deeply mixed and vaguely hostile
    # feelings about ORMs).  As a result we have to do a bunch of
    # confusing stuff to convert between what django wants to call the
    # key and what we want to call it in the database.
    #
    # So.  I HOPE I've done this right.  I *think* where I use the _pk
    # field, it's what *django* thinks is the primary key in terms of
    # object fields, *not* the name of the primary key column in
    # the database.  But this is a land mine.
    _pk = 'diaObject_id'
    _create_kws = [ 'diaObject_id', 'libid', 'sim_searcheff_mask', 'gentype', 'sim_template_index',
                    'zcmb', 'zhelio', 'zcmb_smear', 'ra', 'dec', 'mwebv', 'galnmatch', 'galid', 'galzphot',
                    'galzphoterr', 'galsnsep', 'galsnddlr', 'rv', 'av', 'mu', 'lensdmu', 'peakmjd',
                    'mjd_detect_first', 'mjd_detect_last', 'dtseason_peak', 'peakmag_u', 'peakmag_g',
                    'peakmag_r', 'peakmag_i', 'peakmag_z', 'peakmag_Y', 'snrmax', 'snrmax2', 'snrmax3',
                    'nobs', 'nobs_saturate' ]
    _dict_kws = _create_kws
    
    # This is a little bit ugly.  For my own dubious reasons, I wanted
    # to be able to pass in things with diaObjectId that weren't
    # actually in the database (to save myself some pain on the other
    # end).  So, filter those out here before calling the Createable's
    # bulk_load_or_create
    @classmethod
    def bulk_load_or_create( cls, data ):
        """Pass an array of dicts."""
        pks = [ i['diaObjectId'] for i in data ]
        diaobjs = list( DiaObject.objects.filter( pk__in=pks ) )
        objids = set( [ i.diaObjectId for i in diaobjs ] )
        datatoload = [ i for i in data if i['diaObjectId'] in objids ]
        for datum in datatoload:
            datum['diaObject_id'] = datum['diaObjectId']
            del datum['diaObjectId']
        if len(datatoload) > 0:
            return super().bulk_load_or_create( datatoload )
        else:
            return []
    

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
    classId = models.IntegerField( db_index=True )
    gentype = models.IntegerField( db_index=True, null=True, unique=True )
    description = models.TextField()

class ClassIdOfGentype(models.Model):
    id = models.AutoField( primary_key=True )
    gentype = models.IntegerField( db_index=True )
    classId = models.IntegerField( db_index=True )
    exactmatch = models.BooleanField()
    categorymatch = models.BooleanField()
    description = models.TextField()
    
    
# ======================================================================
# ======================================================================
# ======================================================================
# Local information.
#
# Eventually, this will have the local versions of diaObject and
# diaSource.  For now, though, we're going to just look in those tables
# for objects that have a corresponding TOM target (which I will call a
# "known object"), and for sources and forced sources of known objects
# that are prior to the current simulated date.


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
# how we're really going to want ot do it, because we want to have
# pre-existing structure, and not have to search a heterogeneous JSON
# dictionary for all of that (which I think is effectively what the
# target extra data is).  Does this subvert use of the TOM target UI and
# API interfaces, though?  Thought required.
class DiaObjectOfTarget(models.Model):
    diaObject = models.ForeignKey( DiaObject, db_column='diaObjectId', on_delete=models.CASCADE, null=False )
    tomtarget = models.ForeignKey( tom_targets.models.Target, db_column="tomtarget_id",
                                   on_delete=models.CASCADE, null=False )

    @classmethod
    def maybe_new_elasticc_targets( cls, objids, ras, decs ):
        """Given a list of objects (with coordinates), add new TOM targets for objects that don't already exist
        """
        # django weirdness : even though I told it to make the database column
        # diaObjectId, the field that django gets is diaObject_id.
        preexisting = cls.objects.filter( diaObject_id__in=objids )
        preexistingids = [ o.diaObject_id for o in preexisting ]
        newobjs = [ ( objids[i], ras[i], decs[i] )
                    for i in range(len(objids))
                    if objids[i] not in preexistingids ]

        # NOTE : I could use Django's bulk_create() here in order to make
        # the database queries more efficient.  However, that would bypass
        # any hooks that tom_targets has added to its save() method,
        # which scares me.
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

        newlinks = []
        for targ, newobj in zip( newtargs, newobjs ):
            newlinks.append( cls( diaObject_id=newobj[0],
                                  tomtarget_id=targ.id ) )
        if len(newlinks) > 0:
            addedlinks = cls.objects.bulk_create( newlinks )
                

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

    brokerMessageId = models.BigAutoField(primary_key=True)
    streamMessageId = models.BigIntegerField(null=True)
    topicName = models.CharField(max_length=200, null=True)

    alertId = models.BigIntegerField()
    diaSourceId = models.BigIntegerField()
    # diaSource = models.ForeignKey( DiaSource, on_delete=models.PROTECT, null=True )
    
    # timestamps as datetime.datetime (DateTimeField)
    msgHdrTimestamp = models.DateTimeField(null=True)
    descIngestTimestamp = models.DateTimeField(auto_now_add=True)  # auto-generated
    elasticcPublishTimestamp = models.DateTimeField(null=True)
    brokerIngestTimestamp = models.DateTimeField(null=True)

    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index( fields=[ 'topicName', 'streamMessageId' ] ),
            models.Index( fields=[ 'alertId' ] ),
            models.Index( fields=[ 'diaSourceId' ] ),
        ]


    def to_dict( self ):
        resp = {
            'brokerMessageId': self.brokerMessageId,
            'streamMessageId': self.streamMessageId,
            'alertId': self.alertId,
            'diaSourceId': self.diaSourceId,
            'msgHdrTimestamp': self.msgHdrTimestamp.isoformat(),
            'descIngestTimestamp': self.descIngestTimestamp.isoformat(),
            'elasticcPublishTimestamp': int( self.elasticcPublishTimestamp.timestamp() * 1e3 ),
            'brokerIngestTimestamp': int( self.brokerIngestTimestamp.timestamp() * 1e3 ),
            'brokerName': "<unknown>",
            'brokerVersion': "<unknown>",
            'classifications': []
            }
        clsfctions = BrokerClassification.objects.all().filter( dbMessage=self )
        first = True
        for classification in clsfctions:
            clsifer = classification.classifier
            if first:
                resp['brokerName'] = clsifer.brokerName
                resp['brokerVersion'] = clsifer.brokerVersion
                first = False
            else:
                if ( ( clsifer.brokerName != resp['brokerName'] ) or
                     ( clsifer.brokerVersion != resp['brokerVersion'] ) ):
                    raise ValueError( "Mismatching brokerName and brokerVersion in the database! "
                                      "This shouldn't happen!" )
            resp['classifications'].append( { 'classifierName': clsifer.classifierName,
                                              'classifierParams': clsifer.classifierParams,
                                              'classId': classification.classId,
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
        # value of brokerMessageId.  According to that page, this is only true for
        # Postgres... which is what I'm using...
        
        logger.debug( f'In BrokerMessage.load_batch, received {len(messages)} messages.' );

        messageobjects = {}
        kwargses = []
        sourceids = []
        utc = pytz.timezone( "UTC" )
        for msg in messages:
            # logger.debug( f"Gonna try to load {msg}" ) 
            timestamp = msg['timestamp']
            if len( msg['msg']['classifications'] ) == 0:
                logger.debug( "Message with no classifications" )
                continue
            keymess = ( f"{msg['msgoffset']}_{msg['topic']}_{msg['msg']['alertId']}" )
            # logger.debug( f'kemess = {keymess}' )
            if keymess not in messageobjects.keys():
                # logger.debug( f"[msg['msg']['elasticcPublishTimestamp'] = {msg['msg']['elasticcPublishTimestamp']}; "
                #               f"timestamp = {timestamp}" )
                msghdrtimestamp = timestamp
                kwargs = { 'streamMessageId': msg['msgoffset'],
                           'topicName': msg['topic'],
                           'alertId': msg['msg']['alertId'],
                           'diaSourceId': msg['msg']['diaSourceId'],
                           'msgHdrTimestamp': msghdrtimestamp,
                           'descIngestTimestamp': datetime.datetime.now(),
                           'elasticcPublishTimestamp': msg['msg']['elasticcPublishTimestamp'],
                           'brokerIngestTimestamp': msg['msg']['brokerIngestTimestamp'],
                           # 'elasticcPublishTimestamp': datetime.datetime.fromtimestamp(
                           #     msg['msg']['elasticcPublisTimestamp'] / 1000000 ),
                           # 'brokerIngestTimestamp': datetime.datetime.fromtimestamp(
                           #     msg['msg']['brokerIngestTimestamp'] / 1000000 )
                }
                kwargses.append( kwargs )
                sourceids.append( msg['msg']['diaSourceId'] )
            else:
                logger.error( f'Key {keymess} showed up more than once in a message batch!' )
        logger.debug( f'Bulk creating {len(kwargses)} messages.' )
        if len(kwargses) > 0:
            # This is byzantine, but I'm copying django documentation here
            objs = ( BrokerMessage( **k ) for k in kwargses )
            batch = list( itertools.islice( objs, len(kwargses) ) )
            if batch is None:
                raise RunTimeError( "Something bad has happened." )
            addedmsgs = BrokerMessage.objects.bulk_create( batch, len(kwargses) )
            for addedmsg in addedmsgs:
                keymess = f"{addedmsg.streamMessageId}_{addedmsg.topicName}_{addedmsg.alertId}"
                messageobjects[ keymess ] = addedmsg
        else:
            addedmsgs = []

        # Add TOM targets for any objects that we just found out about in this message
        # For real LSST, this will be a more involved process, as it's going to involve
        # asking the PPDB about the diaSourceId.

        objids = []
        ras = []
        decs = []
        newsources = DiaSource.objects.filter( diaSourceId__in=sourceids )
        for newsource in newsources:
            if newsource.diaObject_id not in objids:
                objids.append( newsource.diaObject_id )
                ras.append( newsource.diaObject.ra )
                decs.append( newsource.diaObject.decl )
        DiaObjectOfTarget.maybe_new_elasticc_targets( objids, ras, decs )
            
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
            for cfication in msg['msg']['classifications']:
                sigstr = ( f"{msg['msg']['brokerName']}_{msg['msg']['brokerVersion']}_"
                           f"{cfication['classifierName']}_{cfication['classifierParams']}" )
                if sigstr not in condcache:
                    newcond = ( models.Q( brokerName = msg['msg']['brokerName'] ) &
                                models.Q( brokerVersion = msg['msg']['brokerVersion'] ) &
                                models.Q( classifierName = cfication['classifierName'] ) &
                                models.Q( classifierParams = cfication['classifierParams'] ) )
                    cferconds |= newcond
                condcache.add( sigstr )
            curcfers = BrokerClassifier.objects.filter( cferconds )
            for cur in curcfers:
                keycfer = ( f"{cur.brokerName}_{cur.brokerVersion}_"
                            f"{cur.classifierName}_{cur.classifierParams}" )
                classifiers[ keycfer ] = cur
        logger.debug( f'Found {len(classifiers)} existing classifiers.' )
                
        # Create new classifiers as necessary

        addedkeys = set()
        kwargses = []
        for msg in messages:
            for cfication in msg['msg']['classifications']:
                keycfer = ( f"{msg['msg']['brokerName']}_{msg['msg']['brokerVersion']}_"
                            f"{cfication['classifierName']}_{cfication['classifierParams']}" )
                if ( keycfer not in classifiers.keys() ) and ( keycfer not in addedkeys ):
                    kwargses.append( { 'brokerName': msg['msg']['brokerName'],
                                       'brokerVersion': msg['msg']['brokerVersion'],
                                       'classifierName': cfication['classifierName'],
                                       'classifierParams': cfication['classifierParams'] } )
                    addedkeys.add( keycfer )
        ncferstoadd = len(kwargses)
        logger.debug( f'Adding {ncferstoadd} new classifiers.' )
        if ncferstoadd > 0:
            objs = ( BrokerClassifier( **k ) for k in kwargses )
            batch = list( itertools.islice( objs, len(kwargses) ) )
            newcfers = BrokerClassifier.objects.bulk_create( batch, len(kwargses) )
            for curcfer in newcfers:
                keycfer = ( f"{curcfer.brokerName}_{curcfer.brokerVersion}_"
                            f"{curcfer.classifierName}_{curcfer.classifierParams}" )
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
            for cfication in msg['msg']['classifications']:
                keycfer = ( f"{msg['msg']['brokerName']}_{msg['msg']['brokerVersion']}_"
                            f"{cfication['classifierName']}_{cfication['classifierParams']}" )
                kwargs = { 'dbMessage': messageobjects[keymess],
                           'classifierId': classifiers[keycfer].classifierId,
                           'classId': cfication['classId'],
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
                 "firstbrokerMessageId": None if len(addedmsgs)==0 else addedmsgs[0].brokerMessageId }
        
class BrokerClassifier(models.Model):
    """Model for a classifier producing an ELAsTiCC broker classification."""

    classifierId = models.BigAutoField(primary_key=True, db_index=True)

    brokerName = models.CharField(max_length=100)
    brokerVersion = models.TextField(null=True)     # state changes logically not part of the classifier
    classifierName = models.CharField(max_length=200)
    classifierParams = models.TextField(null=True)   # change in classifier code / parameters
    
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["brokerName"]),
            models.Index(fields=["brokerName", "brokerVersion"]),
            models.Index(fields=["brokerName", "classifierName"]),
            models.Index(fields=["brokerName", "brokerVersion", "classifierName", "classifierParams"]),
        ]

class BrokerClassification(psqlextra.models.PostgresPartitionedModel):
    """Model for a classification from an ELAsTiCC broker."""

    class PartitioningMeta:
        method = psqlextra.types.PostgresPartitioningMethod.LIST
        key = [ 'classifierId' ]
        
    class Meta:
        constraints = [
            models.UniqueConstraint(
                name="unique_constarint_elasticc2_brokerclassification_partitionkey",
                fields=( 'classifierId', 'classificationId' )
            ),
        ]
    
    classificationId = models.BigAutoField(primary_key=True)
    dbMessage = models.ForeignKey( BrokerMessage, db_column='brokerMessageId', on_delete=models.CASCADE, null=True )
    # I really want to make a foreign key here, but I haven't figured
    # out how to get Django to succesfully create a unique
    # constraint and a partition on a foreign key.  Either I get try to
    # partition on "classifierId" and I get an error from Django saying
    # that that field doesn't exist, or I try to partition on
    # "classifier" and I get an error from Postgres saying that that
    # field doesn't exist.  (ORM considered harmful.)
    # classifier = models.ForeignKey( BrokerClassifier, db_column='classifierId',
    #                                   on_delete=models.CASCADE, null=True )
    classifierId = models.BigIntegerField()
    # These next three can be determined by looking back at the linked dbMessage
    # alertId = models.BigIntegerField()
    # diaObjectId = models.BigIntegerField()
    # diaSource = models.ForeignKey( DiaSource, on_delete=models.PROTECT, null=True )

    classId = models.IntegerField( db_index=True )
    probability = models.FloatField()

    # JSON blob of additional information from the broker?
    # Here or in a separate table?
    # (As is, the schema doesn't define such a thing.)
    
    modified = models.DateTimeField(auto_now=True)
