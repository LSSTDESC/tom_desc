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
    classiferName = models.CharField(max_length=200)

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
    dbMessageIndex = models.ForeignKey(
        ElasticcBrokerMessage, on_delete=models.PROTECT, null=True
    )
    dbClassifierIndex = models.ForeignKey(
        ElasticcBrokerClassifier, on_delete=models.PROTECT, null=True
    )

    alertId = models.BigIntegerField()
    diaObjectId = models.BigIntegerField()

    classId = models.IntegerField()
    probability = models.FloatField()

    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(
                fields=['dbClassificationIndex', 'dbClassifierIndex', 'dbMessageIndex']
            ),
        ]

# ======================================================================
        
class RknopTest(models.Model):
    number = models.IntegerField( primary_key=True )
    description = models.TextField()

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
    raErr = models.FloatField( null=True )
    declErr = models.FloatField( null=True )
    ra_decl_Cov = models.FloatField( null=True )
    radecTai = models.FloatField( )
    pmRa = models.FloatField( null=True )
    pmDecl = models.FloatField( null=True )
    parallax = models.FloatField( null=True )
    pmRaErr = models.FloatField( null=True )
    pmDeclErr = models.FloatField( null=True )
    parallaxErr = models.FloatField( null=True )
    pmRa_pmDecl_Cov = models.FloatField( null=True )
    pmRa_parallax_Cov = models.FloatField( null=True )
    pmDecl_parallax_Cov = models.FloatField( null=True )
    pmParallaxLnL = models.FloatField( null=True )
    pmParallaxChi2 = models.FloatField( null=True )
    pmParallaxNdata = models.IntegerField( null=True )

    @staticmethod
    def create( data ):
        # There's almost certainly a Django helper method/class to do this automatically....
        # (Or I could build a **kwargs)
        curobj = ElasticcDiaObject(
            diaObjectId = data['diaObjectId'],
            radec = Point( data['ra'], data['decl'] ),
            raErr = data['raErr'],
            declErr = data['declErr'],
            ra_decl_Cov = data['ra_decl_Cov'],
            radecTai = data['radecTai'],
            pmRa = data['pmRa'],
            pmDecl = data['pmDecl'],
            parallax = data['parallax'],
            pmRaErr = data['pmRaErr'],
            pmDeclErr = data['pmDeclErr'],
            parallaxErr = data['parallaxErr'],
            pmRa_pmDecl_Cov = data['pmRa_pmDecl_Cov'],
            pmRa_parallax_Cov = data['pmRa_parallax_Cov'],
            pmDecl_parallax_Cov = data['pmDecl_parallax_Cov'],
            pmParallaxLnL = data['pmParallaxLnL'],
            pmParallaxChi2 = data['pmParallaxChi2'],
            pmParallaxNdata = data['pmParallaxNdata'] )
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
    
class ElasticcSSObject(models.Model):
    ssObjectId = models.BigIntegerField( primary_key=True, db_index=True )
    discoveriySubmissionDate = models.FloatField( null=True )
    firstObservationDate = models.FloatField( null=True )
    arc = models.FloatField( null=True )
    numObjs = models.IntegerField( null=True )
    flags = models.BigIntegerField()

class ElasticcDiaSource(models.Model):
    diaSourceId = models.BigIntegerField( primary_key=True, db_index=True )
    ccdVisitId = models.BigIntegerField()
    # Alert has field diaObjectId
    diaObject = models.ForeignKey( ElasticcDiaObject, on_delete=models.CASCADE, null=True )
    # Alert has field ssObjectId
    ssObject = models.ForeignKey( ElasticcSSObject, on_delete=models.CASCADE, null=True )
    # Alert has field parentDiaSourceId
    parentDiaSource = models.ForeignKey( "self", on_delete=models.CASCADE, null=True )
    midPointTai = models.FloatField()
    filterName = models.TextField()
    programId = models.IntegerField()
    # I believe that the Point field defaults to Longitude / Latitude in degrees, which is like RA/Dec
    # The alerts have fields ra, decl
    radec = gis_models.PointField( spatial_index=True )
    # ra = models.FloatField()
    # decl = models.FloatField()
    raErr = models.FloatField( null=True )
    declErr = models.FloatField( null=True )
    ra_decl_cov = models.FloatField( null=True )
    x = models.FloatField()
    y = models.FloatField()
    xErr = models.FloatField( null=True )
    yErr = models.FloatField( null=True )
    x_y_Cov = models.FloatField( null=True )
    apFlux = models.FloatField()
    apFluxErr = models.FloatField()
    snr = models.FloatField()
    psFlux = models.FloatField()
    psFluxErr = models.FloatField()
    flags = models.BigIntegerField()
    nobs = models.FloatField( null=True )
    mwebv = models.FloatField( null=True )
    mwebv_err = models.FloatField( null=True )
    z_final = models.FloatField( null=True )
    z_final_err = models.FloatField( null=True )
    hostgal_ellipticity = models.FloatField( null=True )
    hostgal_sqradius = models.FloatField( null=True )
    hostgal_z = models.FloatField( null=True )
    hostgal_mag_u = models.FloatField( null=True )
    hostgal_mag_g = models.FloatField( null=True )
    hostgal_mag_r = models.FloatField( null=True )
    hostgal_mag_i = models.FloatField( null=True )
    hostgal_mag_z = models.FloatField( null=True )
    hostgal_mag_Y = models.FloatField( null=True )
    hostgal_radec = gis_models.PointField()
    # hostgal_ra = models.FloatField( null=True )
    # hostgal_dec = models.FloatField( null=True )
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
    hostgal2_mag_u = models.FloatField( null=True )
    hostgal2_mag_g = models.FloatField( null=True )
    hostgal2_mag_r = models.FloatField( null=True )
    hostgal2_mag_i = models.FloatField( null=True )
    hostgal2_mag_z = models.FloatField( null=True )
    hostgal2_mag_Y = models.FloatField( null=True )
    hostgal2_radec = gis_models.PointField()
    # hostgal2_ra = models.FloatField( null=True )
    # hostgal2_dec = models.FloatField( null=True )
    hostgal2_snsep = models.FloatField( null=True )
    hostgal2_magerr_u = models.FloatField( null=True )
    hostgal2_magerr_g = models.FloatField( null=True )
    hostgal2_magerr_r = models.FloatField( null=True )
    hostgal2_magerr_i = models.FloatField( null=True )
    hostgal2_magerr_z = models.FloatField( null=True )
    hostgal2_magerr_Y = models.FloatField( null=True )

class ElasticcAlert(models.Model):
    alertId = models.BigIntegerField( primary_key=True, db_index=True )
    diaSource = models.ForeignKey( ElasticcDiaSource, on_delete=models.CASCADE )
    # prvDiaSources
    # prvDiaForcedSources
    # prvDiaNondetectionLimits
    diaObject = models.ForeignKey( ElasticcDiaObject, on_delete=models.CASCADE )
    ssObject = models.ForeignKey( ElasticcSSObject, on_delete=models.CASCADE )
    # cutoutDifference
    # cutoutTemplate
