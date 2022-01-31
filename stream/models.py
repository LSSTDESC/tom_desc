import sys
import math
from django.contrib.gis.geos import Point
from django.contrib.gis.db import models as gis_models
from django.db import models

# Create your models here.


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
    coordinates = gis_models.PointField(null=True, blank=True)
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
    # Alert has ra, decl
    radec = gis_models.PointField( spatial_index=True )

    @staticmethod
    def create( data ):
        # There's almost certainly a Django helper method/class to do this automatically....
        # (Or I could build a **kwargs)
        curobj = ElasticcDiaObject(
            diaObjectId = data['diaObjectId'],
            radec = Point( data['ra'], data['decl'] ),
        )
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
    # Alert has field diaObjectId
    diaObject = models.ForeignKey( ElasticcDiaObject, on_delete=models.CASCADE, null=True )
    midPointTai = models.FloatField()
    filterName = models.TextField()
    # I believe that the Point field defaults to Longitude / Latitude in degrees, which is like RA/Dec
    # The alerts have fields ra, decl
    radec = gis_models.PointField( spatial_index=True )
    # ra = models.FloatField()
    # decl = models.FloatField()
    apFlux = models.FloatField()
    apFluxErr = models.FloatField()
    nobs = models.FloatField( null=True )
    mwebv = models.FloatField( null=True )
    mwebv_err = models.FloatField( null=True )
    z_final = models.FloatField( null=True )
    z_final_err = models.FloatField( null=True )
    hostgal_z = models.FloatField( null=True )
    hostgal_z_err = models.FloatField( null=True )
    hostgal_snsep = models.FloatField( null=True )
    hostgal_mag_u = models.FloatField( null=True )
    hostgal_mag_g = models.FloatField( null=True )
    hostgal_mag_r = models.FloatField( null=True )
    hostgal_mag_i = models.FloatField( null=True )
    hostgal_mag_z = models.FloatField( null=True )
    hostgal_mag_Y = models.FloatField( null=True )
    hostgal_magerr_u = models.FloatField( null=True )
    hostgal_magerr_g = models.FloatField( null=True )
    hostgal_magerr_r = models.FloatField( null=True )
    hostgal_magerr_i = models.FloatField( null=True )
    hostgal_magerr_z = models.FloatField( null=True )
    hostgal_magerr_Y = models.FloatField( null=True )
    
    @staticmethod
    def create( data ):
        cursrc = ElasticcDiaSource(
            diaSourceId = data['diaSourceId'],
            diaObject = data['diaObject'],
            midPointTai = data['midPointTai'],
            filterName = data['filterName'],
            radec = Point( data['ra'], data['decl'] ),
            apFlux = data['apFlux'],
            apFluxErr = data['apFluxErr'],
            nobs = data['nobs'],
            mwebv = data['mwebv'],
            mwebv_err = data['mwebv_err'],
            z_final = data['z_final'],
            z_final_err = data['z_final_err'],
            hostgal_z = data['hostgal_z'],
            # hostgal_z_err = data['hostgal_z_err'],   # Isn't currently in alert schema
            hostgal_snsep = data['hostgal_snsep'],
            hostgal_mag_u = data['hostgal_mag_u'],
            hostgal_mag_g = data['hostgal_mag_g'],
            hostgal_mag_r = data['hostgal_mag_r'],
            hostgal_mag_i = data['hostgal_mag_i'],
            hostgal_mag_z = data['hostgal_mag_z'],
            hostgal_mag_Y = data['hostgal_mag_Y'],
            hostgal_magerr_u = data['hostgal_magerr_u'],
            hostgal_magerr_g = data['hostgal_magerr_g'],
            hostgal_magerr_r = data['hostgal_magerr_r'],
            hostgal_magerr_i = data['hostgal_magerr_i'],
            hostgal_magerr_z = data['hostgal_magerr_z'],
            hostgal_magerr_Y = data['hostgal_magerr_Y'],
        )
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

    @staticmethod
    def create( data ):
        try:
            source = ElasticcDiaSource.objects.get( diaSourceId=data['SourceID'] )
            if source.diaObject_id != data['SNID']:
                raise ValueError( f"SNID {data['SNID']} doesn't match diaSource diaObject_id {source.diaObject_id}" )
            if math.fabs( float( data['MJD'] - source.midPointTai ) > 0.01 ):
                raise ValueError( f"MJD {data['MJD']} doesn't match diaSource midPointTai {source.midPointTai}" )
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
            curtruth = ElasticcDiaTruth.objects.get( diaSource_id=data['SourceID'] )
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

    

