import sys
import math
import time
import io
import datetime
import pytz
import logging
import itertools
import collections
import django.db
from django.db import models
from django.utils.functional import cached_property
import django.contrib.postgres.indexes as indexes
import psqlextra.types
import psqlextra.models
import psycopg2.extras
import pandas

_logger = logging.getLogger(__name__)
_logout = logging.StreamHandler( sys.stderr )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' )
_logout.setFormatter( _formatter )
_logger.propagate = False
_logger.addHandler( _logout )
_logger.setLevel( logging.INFO )
# _logger.setLevel( logging.DEBUG )

# Support the Q3c Indexing scheme

class q3c_ang2ipix(models.Func):
    function = "q3c_ang2ipix"

# A hack so that I can have index names that go up to
#   the 63 characters postgres allows, instead of the
#   30 that django allows
class LongNameBTreeIndex(indexes.BTreeIndex):
    @cached_property
    def max_name_length(self):
        return 63 - len(models.Index.suffix) + len(self.suffix)

# A parent class for my "create" and "load_or_create" methods
# Classes that derive from this must define _create_kws and _pk
class Createable(models.Model):
    class Meta:
        abstract = True

    def to_dict( self ):
        selfdict = {}
        for key in self._dict_kws:
            if hasattr( self, '_irritating_django_id_map') and ( key in self._irritating_django_id_map ):
                self_key = self._irritating_django_id_map[key]
            else:
                self_key = key
            selfdict[key] = getattr( self, self_key )
        return selfdict
                
    @classmethod
    def create( cls, data ):
        kwargs = {}
        for kw in cls._create_kws:
            kwargs[kw] = data[kw] if kw in data else None
        curobj = cls( **kwargs )
        curobj.save()
        return curobj

    @classmethod
    def load_or_create( cls, data ):
        try:
            curobj = cls.objects.get( pk=data[cls._pk] )
            # VERIFY THAT STUFF MATCHES????
            return curobj
        except cls.DoesNotExist:
            return cls.create( data )

    @classmethod
    def which_exist( cls, pks ):
        """Pass a list of primary key, get a list of the ones that already exist."""
        q = cls.objects.filter( pk__in=pks )
        return [ getattr(i, i._pk) for i in q ]

    # This version uses postgres COPY and tries to be faster than mucking
    # about with ORM constructs.
    @classmethod
    def bulk_insert_onlynew( cls, data ):
        """Insert a bunch of data into the database.  Ignores records that conflict with things present.

        data — An array of dicts.  Key in the dicst MUST match columns in the target table.

        Returns the number of rows actually inserted (which may be less than len(data)).
        """
        conn = None
        origautocommit = None
        gratuitous = None
        cursor = None
        try:
            # Jump through hoops to get access to the psycopg2
            #   connection from django.  We need this to
            #   turn off autocommit so we can use a temp table.
            gratuitous = django.db.connection.cursor()
            conn = gratuitous.connection
            origautocommit = conn.autocommit
            conn.autocommit = False
            cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )
            # Yeah.... if anybody ever creates a django application named "bulk" and then
            #   has a model "upsert", we're about to totally screw that up.
            cursor.execute( "DROP TABLE IF EXISTS bulk_upsert" )
            cursor.execute( f"CREATE TEMP TABLE bulk_upsert (LIKE {cls._meta.db_table})" )
            # NOTE: I have a little bit of worry here that pandas is going to destroy
            # datatypes-- in particular, that it will convert my int64s to either int32 or
            # float our double, thereby losing precision.  I've checked it, and it seems
            # to be doing the right thing.  But I have had issues in the past with
            # pandas silently converting data to doubles.
            df = pandas.DataFrame( data )
            strio = io.StringIO()
            df.to_csv( strio, index=False, header=False, sep='\t', na_rep='\\N' )
            strio.seek(0)
            # Have to quote the column names because many have mixed case.
            columns = [ f'"{c}"' for c in df.columns.values ]
            cursor.copy_from( strio, "bulk_upsert", columns=columns, size=1048576 )
            q = f"INSERT INTO {cls._meta.db_table} SELECT * FROM bulk_upsert ON CONFLICT DO NOTHING"
            cursor.execute( q )
            ninserted = cursor.rowcount
            # I don't think I should have to do this; shouldn't it happen automatically
            #   with conn.commit()?  But it didn't seem to.  Maybe it only happens
            #   with conn.close(), but I don't want to do that because it screws
            #   with django.  (I'm probably doing naughty things by even digging to
            #   get conn, but hey, I need it for efficiency.)
            cursor.execute( "DROP TABLE bulk_upsert" )
            conn.commit()
            return ninserted
        except Exception as e:
            if conn is not None:
                conn.rollback()
            raise e
        finally:
            if cursor is not None:
                cursor.close()
                cursor = None
            if gratuitous is not None:
                gratuitous.close()
                gratuitous = None
            if origautocommit is not None and conn is not None:
                conn.autocommit = origautocommit
                origautocommit = None
                conn = None
        
    # NOTE -- this version returns all the objects that were
    #   either loaded or created.  I've got other classes that
    #   have their own "bulk_load_or_create" that don't return
    #   the objects, and that use the database unique checks
    #   to avoid duplication.  I should think about merging
    #   them.
    # This one only detects existing objects based on the
    #   primary key, which is *not enough* for some of the
    #   other things I'm missing.
    @classmethod
    def bulk_load_or_create( cls, data ):
        """Pass an array of dicts."""
        pks = [ i[cls._pk] for i in data ]
        curobjs = list( cls.objects.filter( pk__in=pks ) )
        exists = set( [ getattr(i, i._pk) for i in curobjs ] )
        newobjs = set()
        for newdata in data:
            if newdata[cls._pk] in exists:
                continue
            kwargs = {}
            for kw in cls._create_kws:
                kwargs[kw] = newdata[kw] if kw in newdata else None
            newobjs.add( cls( **kwargs ) )
        if len(newobjs) > 0:
            addedobjs = cls.objects.bulk_create( newobjs )
            curobjs.extend( addedobjs )
        return curobjs
        
# ======================================================================-
# This class is to hold permissions that don't logically belong
# to a single model below.

class ElasticcPermissions(models.Model):
    class Meta:
        managed = False    # No database table to go with this
        default_permissions=()
        permissions = (
            ( 'elasticc_admin', 'Elasticc Admin User' ),
            ( 'elasticc_broker', 'Can Write BrokerMessage Objects' ),
        )


# ======================================================================-
# I want these tables to correspond to the avro schema.  I considered
# writing code to generate the code below (or even to have code below
# that programmatically generated the database fields, though that very
# idea (WRT django in general) seems rather scary; something like
# database table definitions should be stable so they don't easily
# support generating the fields on the fly).


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
        if self.diaSource.midPointTai - objsources[0].midPointTai > 0.5:
            objforced = DiaForcedSource.objects.filter( diaObject_id=self.diaSource.diaObject_id,
                                                        midPointTai__gte=objsources[0].midPointTai-30.,
                                                        midPointTai__lt=self.diaSource.midPointTai )
            for forced in objforced:
                newforced = {}
                for field in [ "diaForcedSourceId", "ccdVisitId", "midPointTai",
                               "filterName", "psFlux", "psFluxErr", "totFlux", "totFluxErr" ]:
                    newforced[field] = getattr( forced, field )
                newforced["diaObjectId"] = forced.diaObject_id
                alert["prvDiaForcedSources"].append( newforced )

        return alert

# Perhaps I should be using django ManyToMany here?
# I do this manually because it mapps directly to
# SQL, so if somebody hits the table with SQL
# directly rather than via django, they'll know
# what to do, and I'll know what the structure
# is.
#
# BUT ALSO : I ended up not loading these next two
# tables becasue the amount of information is HUGE.
# We can, at least in principle, regenerate which
# sources and forced sources were in the alert
# because we know the algorithm:
#   * Each alert is assocaited with a source
#   * If that source is the first detection, there
#     are no previous sources
#   * Otherwise, all previous sources (i.e. detections) are included
#   * If that source is on the same night as the
#     first detection, there is no forced photometry
#   * Otherwise, all forced photometry with a date
#     of (first detection) - 30 days or later is included
# class DiaAlertPrvSource(models.Model):
#     id = models.BigAutoField( primary_key=True )
#     diaAlert = models.ForeignKey( DiaAlert, db_column='diaAlertId', on_delete=models.CASCADE, null=True )
#     diaSource = models.ForeignKey( DiaSource, db_column='diaSourceId', on_delete=models.CASCADE, null=True )

#     # I don't know why, but I was getting a django migraiton error with this in
#     # class Meta:
#     #     unique_together = ( 'diaAlert', 'diaSource' )
    
#     @classmethod
#     def bulk_load_or_create( cls, data ):
#         objs = []
#         for newdata in data:
#             objs.append( cls(**newdata) )
#         cls.objects.bulk_create( objs, ignore_conflicts=True )
        
# class DiaAlertPrvForcedSource(models.Model):
#     id = models.BigAutoField( primary_key=True )
#     diaAlert = models.ForeignKey( DiaAlert, db_column='diaAlertId', on_delete=models.CASCADE, null=True )
#     diaForcedSource = models.ForeignKey( DiaForcedSource, db_column='diaSourceId',
#                                          on_delete=models.CASCADE, null=True )

#     # I don't know why, but I was getting a django migraiton error with this in
#     # class Meta:
#     #     unique_together = ( 'diaAlert', 'diaForcedSource' )

#     # This is distressingly similar to DiaAlertPrvSoruce.bulk_load_or_create
#     @classmethod
#     def bulk_load_or_create( cls, data ):
#         # searchkeys = [ f"{i['diaAlert_id']} {i['diaForcedSource_id']}" for i in data ]
#         # queryset = cls.objects.annotate( srch=models.functions.Concat( 'diaAlert_id',
#         #                                                                models.Value(' '),
#         #                                                                'diaForcedSource_id',
#         #                                                                output_field=models.TextField()) )
#         # curobjs = list( queryset.filter( srch__in=searchkeys ) )
#         # exists = set( [ f"{c.diaAlert_id} {c.diaForcedSource_id}" for c in curobjs ] )
#         # newobjs = []
#         # for newdata in data:
#         #     if f"{newdata['diaAlert_id']} {newdata['diaForcedSource_id']}" in exists:
#         #         continue
#         #     newobjs.append( cls(**newdata) )
#         # if len(newobjs) > 0:
#         #     addedobjs = cls.objects.bulk_create( newobjs )
#         #     curobjs.extend( addedobjs )
#         # return curobjs
#         objs = []
#         for newdata in data:
#             objs.append( cls(**newdata) )
#         cls.objects.bulk_create( objs, ignore_conflicts=True )
    
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
    # directly to the SQL.  It is, of course, wrong-- of *course* we're
    # going to want to use SQL directly to access the datbase.  As a
    # result we have to do a bunch of confusing stuff to convert between
    # what django wants to call the key and what we want to call it in
    # the database.
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

class GentypeOfClassId(models.Model):
    id = models.AutoField( primary_key=True )
    classId = models.IntegerField( db_index=True )
    gentype = models.IntegerField( db_index=True, null=True )
    description = models.TextField()

class ClassIdOfGentype(models.Model):
    id = models.AutoField( primary_key=True )
    gentype = models.IntegerField( db_index=True )
    classId = models.IntegerField( db_index=True )
    exactmatch = models.BooleanField()
    categorymatch = models.BooleanField()
    description = models.TextField()
    

# ======================================================================
# The Broker* don't correspond as directly to the avro alerts
#
# The avro alert schema is elasticc.v0_9.brokerClassification.avsc
#
# For each one of those, we generate an BrokerMessage in the database.
# From the alert directly, we set:
#   alertId --> links back to DiaAlert **
#   diaSourceId --> links back to DiaSource table **
#   elasticcPublishTimestamp
#   brokerIngestTimestamp
#
# ** These won't be actual foreign keys, so that we can ingest things
#    the broker sends us with IDs that don't match what we have.  But,
#    this is what to use on a JOIN.  To think about: return an error
#    if the alertId or diaSourceId is unknown?  Or should we accept
#    that things might come out of order?  For Elasticc, they shouldn't;
#    we should have all the alertID and sourceId loaded first.
#
# We set ourselves:
#   brokerMessageId (just the primary key, auto updated)
#   descIngestTimestamp
#   modified
#   streamMessageId (pulled from the .offset() method of the message we get from kafka consumer)...
#                      ...may not be terribly meaningful because of partitions!  Figure this out)
#   topicName (pulled from the .topic() method of the message we get from the kafka consumer)
#
# In addition, in the avro alert, there is further stuff.  When adding a
# message to the database, We will dig into the classifications array
# and:
#  * See if an BrokerClassifier already exists based on
#       * brokerName, brokerVersion from the alert
#       * classifierName, classifierParams from the classifications array
#    If it does, then, yay.
#    If not, then create that object.
# * Create an BrokerClassification entry which:
#       * links back to the BrokerClassifier with classifier
#       * links back to the broker alert with dbMessage
#       

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

        messages is an array of dicts with keys topic, msgoffset, and msg.
        topic is string, msgoffset is an integer, and msg is a dict that should
        match the elasticc.v0_9.brokerClassification.avsc schema

        Returns information about numbers of things actually added.
        """

        # It's a hard problem to decide if a message already
        # exists.  The things that are in the BrokerMessage object
        # could (by chance) be duplicated for different brokers.
        # I need to think about this, but for now I'm going to assume
        # that all messages coming in are new messages.  I *think*
        # at worst that this will lead to useless message objects
        # in the database that we can ignore.  I think.

        # NOTE: See https://docs.djangoproject.com/en/3.0/ref/models/querysets/#bulk-create
        # the caveats on bulk-create.  I'm assuming that addedmsgs will have the right
        # value of brokerMessageId.  According to that page, this is only true for
        # Postgres... which is what I'm using...
        
        logger.debug( f'In BrokerMessage.load_batch, received {len(messages)} messages.' );

        messageobjects = {}
        kwargses = []
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
                name="unique_constarint_brokerclassification_partitionkey",
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
