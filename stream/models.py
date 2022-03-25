import sys
import math
# from django.contrib.gis.geos import Point
# from django.contrib.gis.db import models as gis_models
from django.db import models
from django.utils.functional import cached_property
import django.contrib.postgres.indexes as indexes

# Create your models here.

# RKNOP 2022-02-02 I'm moving off of PostGIS, to use Q3C for cone and
# polygon searches, as that's more standard in astronomy.  (TODO: need
# to set up analyze and clustering maintenance.)

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

# ======================================================================-
    
# class Target(models.Model):
#     name = models.CharField(max_length=200)
#     right_ascension = models.FloatField(null=True, blank=True)
#     declination = models.FloatField(null=True, blank=True)
#     created = models.DateTimeField(auto_now_add=True)
#     modified = models.DateTimeField(auto_now=True)

#     class Meta:
#         indexes = [
#             LongNameBTreeIndex( q3c_ang2ipix( 'right_ascension', 'declination' ),
#                                 name='idx_%(app_label)s_%(class)s_q3_radec' ),
#         ]

# class Topic(models.Model):
#     name = models.CharField(max_length=50)

#     def __str__(self):
#         return self.name


# class Event(models.Model):
#     identifier = models.CharField(max_length=200)
#     # localization = gis_models.PolygonField(null=True, blank=True)  # TODO: figure out correct model field
#     created = models.DateTimeField(auto_now_add=True)
#     modified = models.DateTimeField(auto_now=True)


# class EventAttributes(models.Model):
#     event = models.ForeignKey(Event, on_delete=models.CASCADE)
#     attributes = models.JSONField(default=dict)
#     tag = models.CharField(max_length=200)
#     sequence_number = models.IntegerField()
#     created = models.DateTimeField(auto_now_add=True)
#     modified = models.DateTimeField(auto_now=True)

# class Alert(models.Model):
#     # target_id = models.ForeignKey(Target, on_delete=models.CASCADE)
#     topic = models.ForeignKey(Topic, on_delete=models.PROTECT)
#     events = models.ManyToManyField(Event)
#     identifier = models.CharField(max_length=200)
#     timestamp = models.DateTimeField(null=True, blank=True)
#     # coordinates = gis_models.PointField(null=True, blank=True)
#     ra = models.FloatField(null=True)
#     decl = models.FloatField(null=True)
#     parsed_message = models.JSONField(default=dict)
#     raw_message = models.JSONField(default=dict)
#     parsed = models.BooleanField(default=False)
#     created = models.DateTimeField(auto_now_add=True)
#     modified = models.DateTimeField(auto_now=True)

#     class Meta:
#         indexes = [
#             models.Index(fields=['timestamp'], name='timestamp_idx'),
#             LongNameBTreeIndex( q3c_ang2ipix( 'ra', 'decl' ), name='idx_%(app_label)s_(class)s_q3c_radec' )
#         ]


# ======================================================================
# I want these tables to correspond to the avro schema.  I considered
#   writing code to generate the code below (or even to have
#   code below that programmatically generated the database fields,
#   though that very idea seems to make a lot of people grouchy, and
#   I can see how something like database table definitions should be
#   stable so they don't easily support generating the fields on the fly).

class ElasticcDiaObject(Createable):
    diaObjectId = models.BigIntegerField( primary_key=True, unique=True, db_index=True )
    ra = models.FloatField( )
    decl = models.FloatField( )
    mwebv = models.FloatField( null=True )
    mwebv_err = models.FloatField( null=True )
    z_final = models.FloatField( null=True )
    z_final_err = models.FloatField( null=True )
    hostgal_ellipticity = models.FloatField( null=True )
    hostgal_sqradius = models.FloatField( null=True )
    hostgal_z = models.FloatField( null=True )
    hostgal_z_err = models.FloatField( null=True )
    hostgal_zphot_q10 = models.FloatField( null=True )
    hostgal_zphot_q20 = models.FloatField( null=True )
    hostgal_zphot_q30 = models.FloatField( null=True )
    hostgal_zphot_q40 = models.FloatField( null=True )
    hostgal_zphot_q50 = models.FloatField( null=True )
    hostgal_zphot_q60 = models.FloatField( null=True )
    hostgal_zphot_q70 = models.FloatField( null=True )
    hostgal_zphot_q80 = models.FloatField( null=True )
    hostgal_zphot_q90 = models.FloatField( null=True )
    hostgal_zphot_q99 = models.FloatField( null=True )
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
    hostgal2_z = models.FloatField( null=True )
    hostgal2_z_err = models.FloatField( null=True )
    hostgal2_zphot_q10 = models.FloatField( null=True )
    hostgal2_zphot_q20 = models.FloatField( null=True )
    hostgal2_zphot_q30 = models.FloatField( null=True )
    hostgal2_zphot_q40 = models.FloatField( null=True )
    hostgal2_zphot_q50 = models.FloatField( null=True )
    hostgal2_zphot_q60 = models.FloatField( null=True )
    hostgal2_zphot_q70 = models.FloatField( null=True )
    hostgal2_zphot_q80 = models.FloatField( null=True )
    hostgal2_zphot_q90 = models.FloatField( null=True )
    hostgal2_zphot_q99 = models.FloatField( null=True )
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
    _create_kws = [ 'diaObjectId', 'ra', 'decl', 'mwebv', 'mwebv_err', 'z_final', 'z_final_err' ]
    for _gal in [ "", "2" ]:
        _create_kws.append( f'hostgal{_gal}_z' )
        _create_kws.append( f'hostgal{_gal}_z_err' )
        _create_kws.append( f'hostgal{_gal}_ra' )
        _create_kws.append( f'hostgal{_gal}_dec' )
        _create_kws.append( f'hostgal{_gal}_snsep' )
        _create_kws.append( f'hostgal{_gal}_ellipticity' )
        _create_kws.append( f'hostgal{_gal}_sqradius' )
        for _phot in [ 'q10', 'q20', 'q30', 'q40', 'q50', 'q60', 'q70', 'q80', 'q90', 'q99' ]:
            _create_kws.append( f'hostgal{_gal}_zphot_{_phot}' )
        for _band in [ 'u', 'g', 'r', 'i', 'z', 'Y' ]:
            for _err in [ '', 'err' ]:
                _create_kws.append( f'hostgal{_gal}_mag{_err}_{_band}' )
    

class ElasticcDiaSource(Createable):
    diaSourceId = models.BigIntegerField( primary_key=True, unique=True, db_index=True )
    ccdVisitId = models.BigIntegerField( )
    diaObject = models.ForeignKey( ElasticcDiaObject, on_delete=models.CASCADE, null=True )
    # I'm not using a foreign key for parentDiaSource to allow things to be loaded out of order
    parentDiaSourceId = models.BigIntegerField( null=True )
    midPointTai = models.FloatField()
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
    
class ElasticcDiaForcedSource(Createable):
    diaForcedSourceId = models.BigIntegerField( primary_key=True, unique=True, db_index=True )
    ccdVisitId = models.BigIntegerField( )
    diaObject = models.ForeignKey( ElasticcDiaObject, on_delete=models.CASCADE )
    midPointTai = models.FloatField()
    filterName = models.TextField()
    psFlux = models.FloatField()
    psFluxErr = models.FloatField()
    totFlux = models.FloatField()
    totFluxErr = models.FloatField()

    _pk = 'diaForcedSourceId'
    _create_kws = [ 'diaForcedSourceId', 'ccdVisitId', 'diaObject',
                    'midPointTai', 'filterName', 'psFlux', 'psFluxErr', 'totFlux', 'totFluxErr' ]
    
class ElasticcDiaAlert(models.Model):
    alertId = models.BigIntegerField( primary_key=True, unique=True, db_index=True )
    diaSource = models.ForeignKey( ElasticcDiaSource, on_delete=models.CASCADE, null=True )
    diaObject = models.ForeignKey( ElasticcDiaObject, on_delete=models.CASCADE, null=True )
    # cutoutDifference
    # cutoutTemplate

# Perhaps I should be using django ManyToMany here?
# I do this manually because it mapps directly to
# SQL, so if somebody hits the table with SQL
# directly rather than via django, they'll know
# what to do, and I'll know what the structure
# is.
class ElasticcDiaAlertPrvSource(models.Model):
    id = models.BigAutoField( primary_key=True )
    diaAlert = models.ForeignKey( ElasticcDiaAlert, on_delete=models.CASCADE, null=True )
    diaSource = models.ForeignKey( ElasticcDiaSource, on_delete=models.CASCADE, null=True )

class ElasticcDiaAlertPrvForcedSource(models.Model):
    id = models.BigAutoField( primary_key=True )
    diaAlert = models.ForeignKey( ElasticcDiaAlert, on_delete=models.CASCADE, null=True )
    diaForcedSource = models.ForeignKey( ElasticcDiaForcedSource, on_delete=models.CASCADE, null=True )
    
class ElasticcDiaTruth(models.Model):
    # I can't use a foreign key constraint here because there will be truth entries for
    # sources for which there was no alert, and as such which will not be in the
    # ElasticcDiaSource table.
    diaSourceId = models.BigIntegerField( null=True )
    diaObjectId = models.BigIntegerField( null=True )
    detect = models.BooleanField( null=True )
    true_gentype = models.IntegerField( null=True )
    true_genmag = models.FloatField( null=True )

    class Meta:
        indexes = [
            models.Index( fields=['diaSourceId'] ),
            models.Index( fields=['diaObjectId'] )
        ]

    @staticmethod
    def create( data ):
        try:
            source = ElasticcDiaSource.objects.get( diaSourceId=data['SourceID'] )
            if source.diaObject_id != data['SNID']:
                raise ValueError( f"SNID {data['SNID']} doesn't match "
                                  f"diaSource diaObject_id {source.diaObject_id} "
                                  f"for diaSource {source.diaSourceId}" )
            if math.fabs( float( data['MJD'] - source.midPointTai ) > 0.01 ):
                raise ValueError( f"MJD {data['MJD']} doesn't match "
                                  f"diaSource midPointTai {source.midPointTai} "
                                  f"for diaSource {source.diaSoruceId}" )
        except ElasticcDiaSource.DoesNotExist:
            if data['DETECT']:
                raise ValueError( f'No SourceID {data["SourceID"]} for a DETECT=true truth entry' )
        curtruth = ElasticcDiaTruth(
            diaSourceId = int( data['SourceID'] ),
            diaObjectId = int( data['SNID'] ),
            detect = bool( data['DETECT'] ),
            true_gentype = int( data['TRUE_GENTYPE'] ),
            true_genmag = float( data['TRUE_GENMAG'] )
        )
        curtruth.save()
        return curtruth
    
    @staticmethod
    def load_or_create( data ):
        try:
            curtruth = ElasticcDiaTruth.objects.get( diaSourceId=data['SourceID'] )
            # VERIFY THAT STUFF MATCHES?????
            return curtruth
        except ElasticcDiaTruth.DoesNotExist:
            return ElasticcDiaTruth.create( data )

# ======================================================================
# The ElasticcBroker* don't correspond as directly to the avro alerts
#
# The avro alert schema is elasticc.v0_9.brokerClassification.avsc
#
# For each one of those, we generate an ElasticcBrokerMessage in the database.
# From the alert directly, we set:
#   alertId --> links back to ROB YOU NEED TO CREATE THIS **
#   diaSourceId --> links back to ElasticcDiaSource table **
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
#   dbMessageIndex (just the primary key, auto updated)
#   descIngestTimestamp
#   modified
# I'm not fully sure what to do with:
#   streamMessageId
#   topicName
# --> these seem to assume that the message came from an kafka stream
#     Probably I should make them nullable in case they didn't come from there.
#     or, another table to track kafka stream topic etc.  ROB THINK.
#
# In addition, in the avro alert, there is further stuff.  We will dig
# into the classifications array and:
#  * See if an ElasticcBrokerClassifier already exists based on
#       * brokerName, brokerVersion from the alert
#       * classifierName, classifierParams from the classifications array
#    If it does, then, yay.
#    If not, then create that object.
# * Create an ElasticcBrokerClassification entry which:
#       * links back to the ElasticcBrokerClassifier with dbClassifier
#       * links back to the broker alert with dbMessage
#       

class ElasticcBrokerMessage(models.Model):
    """Model for the message attributes of an ELAsTiCC broker alert."""

    dbMessageIndex = models.BigAutoField(primary_key=True)
    streamMessageId = models.BigIntegerField(null=True)
    topicName = models.CharField(max_length=200)

    alertId = models.BigIntegerField()
    diaSourceId = models.BigIntegerField()
    # diaSource = models.ForeignKey( ElasticcDiaSource, on_delete=models.PROTECT, null=True )
    
    # timestamps as datetime.datetime (DateTimeField)
    descIngestTimestamp = models.DateTimeField(auto_now_add=True)  # auto-generated
    elasticcPublishTimestamp = models.DateTimeField(null=True)
    brokerIngestTimestamp = models.DateTimeField(null=True)

    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            # models.Index(fields=['dbMessageIndex', 'topicName']),
            models.Index( fields=[ 'dbMessageIndex' ] ),
            models.Index( fields=[ 'alertId' ] ),
            models.Index( fields=[ 'diaSourceId' ] ),
        ]


class ElasticcBrokerClassifier(models.Model):
    """Model for a classifier producing an ELAsTiCC broker classification."""

    dbClassifierIndex = models.BigAutoField(primary_key=True, db_index=True)

    brokerName = models.CharField(max_length=100)
    brokerVersion = models.TextField(null=True)     # state changes logically not part of the classifier
    classiferName = models.CharField(max_length=200)
    classifierParams = models.TextField(null=True)   # change in classifier code / parameters
    
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["brokerName", "classiferName"]),
        ]


class ElasticcBrokerClassification(models.Model):
    """Model for a classification from an ELAsTiCC broker."""

    dbClassificationIndex = models.BigAutoField(primary_key=True)
    dbMessage = models.ForeignKey( ElasticcBrokerMessage, on_delete=models.PROTECT, null=True )
    dbClassifier = models.ForeignKey( ElasticcBrokerClassifier, on_delete=models.PROTECT, null=True )

    # These next three can be determined by looking back at the linked dbMessage
    # alertId = models.BigIntegerField()
    # diaObjectId = models.BigIntegerField()
    # diaSource = models.ForeignKey( ElasticcDiaSource, on_delete=models.PROTECT, null=True )

    classId = models.IntegerField()
    probability = models.FloatField()

    # JSON blob of additional information from the broker?
    # Here or in a separate table?
    # (As is, the schema doesn't define such a thing.)
    
    modified = models.DateTimeField(auto_now=True)

    # class Meta:
    #     indexes = [
    #         models.Index(
    #             fields=['dbClassificationIndex']
    #         ),
    #     ]

# ======================================================================
        
# class RknopTest(models.Model):
#     number = models.IntegerField( primary_key=True )
#     description = models.TextField()
#     ra = models.FloatField( null=True )
#     decl = models.FloatField( null=True )
    
    

