import sys
import math
# from django.contrib.gis.geos import Point
# from django.contrib.gis.db import models as gis_models
from django.db import models

# Create your models here.

# RKNOP 2022-02-02 I'm moving off of PostGIS, to use Q3C for cone and
# polygon searches, as that's more standard in astronomy.  To use this,
# you have to manually create a migration that adds the Q3C index for ra
# and dec fields.  (TODO: need to set up analyze and clustering
# maintenance.)
#
# To do this, first you have to run
#   python manage.py makemigrations stream --name <name> --empty
# Then edit the file created (something like stream/migrations/0002_<name>.py);
# see 0002_q3c_indices.py for an example.

class Target(models.Model):
    name = models.CharField(max_length=200)
    right_ascension = models.FloatField(null=True, blank=True)
    declination = models.FloatField(null=True, blank=True)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)


class Topic(models.Model):
    name = models.CharField(max_length=50)

    def __str__(self):
        return self.name


class Event(models.Model):
    identifier = models.CharField(max_length=200)
    # localization = gis_models.PolygonField(null=True, blank=True)  # TODO: figure out correct model field
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)


class EventAttributes(models.Model):
    event = models.ForeignKey(Event, on_delete=models.CASCADE)
    attributes = models.JSONField(default=dict)
    tag = models.CharField(max_length=200)
    sequence_number = models.IntegerField()
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

class Alert(models.Model):
    # target_id = models.ForeignKey(Target, on_delete=models.CASCADE)
    topic = models.ForeignKey(Topic, on_delete=models.PROTECT)
    events = models.ManyToManyField(Event)
    identifier = models.CharField(max_length=200)
    timestamp = models.DateTimeField(null=True, blank=True)
    # coordinates = gis_models.PointField(null=True, blank=True)
    ra = models.FloatField(null=True)
    decl = models.FloatField(null=True)
    parsed_message = models.JSONField(default=dict)
    raw_message = models.JSONField(default=dict)
    parsed = models.BooleanField(default=False)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['timestamp'], name='timestamp_idx'),
        ]


# ======================================================================
# I want these tables to correspond to the avro schema.  I considered
#   writing code to generate the code below (or even to have
#   code below that programmatically generated the database fields,
#   though that very idea seems to make a lot of people grouchy, and
#   I can see how something like database table definitions should be
#   stable so they don't easily support generating the fields on the fly).
#   However, the majority of the fields in the alerts were null,
#   and it didn't make sense to include them all, so I just did it by
#   hand (well, using some emacs keyboard macros).

class ElasticcDiaObject(models.Model):
    diaObjectId = models.BigIntegerField( primary_key=True, db_index=True )
    ra = models.FloatField( null=True )
    decl = models.FloatField( null=True )
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
    hostgal_zphot_pz50 = models.FloatField( null=True )
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
    hostgal2_zphot_pz50 = models.FloatField( null=True )
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
    
    @staticmethod
    def create( data ):
        kws = [ 'diaObjectId', 'ra', 'decl' ]
        for gal in [ "", "2" ]:
            kws.append( f'hostgal{gal}_z' )
            kws.append( f'hostgal{gal}_z_err' )
            kws.append( f'hostgal{gal}_ra' )
            kws.append( f'hostgal{gal}_dec' )
            kws.append( f'hostgal{gal}_snsep' )
            kws.append( f'hostgal{gal}_ellipticity' )
            kws.append( f'hostgal{gal}_sqradius' )
            for phot in [ 'q10', 'q20', 'q30', 'q40', 'q50', 'q60', 'q70', 'q80', 'q90', 'pz50' ]:
                kws.append( f'hostgal{gal}_zphot_{phot}' )
            for band in [ 'u', 'g', 'r', 'i', 'z', 'Y' ]:
                for err in [ '', 'err' ]:
                    kws.append( f'hostgal{gal}_mag{err}_{band}' )
        kwargs = {}
        for kw in kws:
            if kw in data:
                kwargs[kw] = data[kw]
            else:
                kwargs[kw] = None
        curobj = ElasticcDiaObject( **kwargs )
        curobj.save()
        return curobj
    
    @staticmethod
    def load_or_create( data ):
        try:
            curobj = ElasticcDiaObject.objects.get( pk=data['diaObjectId'] )
            # VERIFY THAT STUFF MATCHES????
            return curobj
        except ElasticcDiaObject.DoesNotExist:
            return ElasticcDiaObject.create( data )
    
class ElasticcDiaSource(models.Model):
    diaSourceId = models.BigIntegerField( primary_key=True, db_index=True )
    ccdVisitId = models.BigIntegerField( null=True )
    diaObject = models.ForeignKey( ElasticcDiaObject, on_delete=models.CASCADE, null=True )
    # I'm not using a foreign key for parentDiaSource to allow things to be loaded out of order
    parentDiaSourceId = models.BigIntegerField( null=True )
    midPointTai = models.FloatField()
    filterName = models.TextField()
    ra = models.FloatField( null=True )
    decl = models.FloatField( null=True )
    psFlux = models.FloatField()
    psFluxErr = models.FloatField()
    snr = models.FloatField( null=True )
    nobs = models.FloatField( null=True )

    @staticmethod
    def create( data ):
        kws = [ 'diaSourceId', 'ccdVisitId', 'diaObject', 'parentDiaSourceId',
                'modPointTai', 'filterName', 'ra', 'decl', 'psFlux', 'psFluxErr', 'snr', 'nobs' ]
        kwargs = {}
        for kw in kws:
            if kw in data:
                kwargs[kw] = data[kw]
            else:
                kwargs[kw] = None
        cursrc = ElasticcDiaSource( **kwargs )
        cursrc.save()
        return cursrc
    
    @staticmethod
    def load_or_create( data ):
        try:
            cursrc = ElasticcDiaSource.objects.get( pk=data['diaSourceId'] )
            # VERIFY THAT STUFF MATCHES????
            return cursrc
        except ElasticcDiaSource.DoesNotExist:
            return ElasticcDiaSource.create( data )


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

class ElasticcBrokerMessage(models.Model):
    """Model for the message attributes of an ELAsTiCC broker alert."""

    dbMessageIndex = models.BigAutoField(primary_key=True)
    streamMessageId = models.BigIntegerField(null=True)
    topicName = models.CharField(max_length=200)

    # timestamps as datetime.datetime (DateTimeField)
    descIngestTimestamp = models.DateTimeField(auto_now_add=True)  # auto-generated
    elasticcPublishTimestamp = models.DateTimeField(null=True)
    brokerIngestTimestamp = models.DateTimeField(null=True)
    brokerPublishTimestamp = models.DateTimeField(null=True)

    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['dbMessageIndex', 'topicName']),
        ]


class ElasticcBrokerClassifier(models.Model):
    """Model for a classifier producing an ELAsTiCC broker classification."""

    dbClassifierIndex = models.BigAutoField(primary_key=True)

    brokerName = models.CharField(max_length=100)
    brokerVersion = models.TextField(null=True)     # state changes logically not part of the classifier
    classiferName = models.CharField(max_length=200)
    classifierVersion = models.TextField(null=True)   # change in classifier code / parameters
    
    modified = models.DateTimeField(auto_now=True)

    models.UniqueConstraint(
        fields=['brokerName', 'classiferName'], name='unique_broker_classifier'
    )

    class Meta:
        indexes = [
            models.Index(fields=["dbClassifierIndex", "brokerName", "classiferName"]),
        ]


class ElasticcBrokerClassification(models.Model):
    """Model for a classification from an ELAsTiCC broker."""

    dbClassificationIndex = models.BigAutoField(primary_key=True)
    dbMessage = models.ForeignKey(
        ElasticcBrokerMessage, on_delete=models.PROTECT, null=True
    )
    dbClassifier = models.ForeignKey(
        ElasticcBrokerClassifier, on_delete=models.PROTECT, null=True
    )

    alertId = models.BigIntegerField()
    # diaObjectId = models.BigIntegerField()
    diaSource = models.ForeignKey( ElasticcDiaSource, on_delete=models.PROTECT, null=True )

    classId = models.IntegerField()
    probability = models.FloatField()

    # JSON blob of additional information from the broker?
    # Here or in a separate table?
    
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(
                fields=['dbClassificationIndex']
            ),
        ]

# ======================================================================
        
class RknopTest(models.Model):
    number = models.IntegerField( primary_key=True )
    description = models.TextField()
    ra = models.FloatField( null=True )
    decl = models.FloatField( null=True )
    
    

