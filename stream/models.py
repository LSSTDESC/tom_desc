import sys
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
    
# class ElasticcSSObject(models.Model):
#     ssObjectId = models.BigIntegerField( primary_key=True, db_index=True )
#     discoverySubmissionDate = models.FloatField( null=True )
#     firstObservationDate = models.FloatField( null=True )
#     arc = models.FloatField( null=True )
#     numObs = models.IntegerField( null=True )
#     flags = models.BigIntegerField()

#     @staticmethod
#     def create( data ):
#         curssobj = ElasticcSSObject(
#             ssObjectId = data['ssObjectId'],
#             discoverySubmissionDate = data['discoverySubmissionDate'],
#             firstObservationDate = data['firstObservationDate'],
#             arc = data['arc'],
#             numObs = data['numObs'],
#             flags = data['flags']
#         )
#         curssobj.save()
#         return curssobj

#     @staticmethod
#     def load_or_create( data ):
#         try:
#             curssobj = ElasticcSSObject.objects.get( pk=data['ssObjectId'] )
#             # VERIFY THAT STUFF MATCHES????
#             return curssobj
#         except ElasticcSSObject.DoesNotExist:
#             return ElasticcSSObject.create( data )
    
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
    hostgal_snsep = models.FloatField( null=True )

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
            hostgal_snsep = data['hostgal_snsep'],
        )
        cursrc.save()
        return cursrc
    
    @staticmethod
    def load_or_create( data ):
        try:
            cursrc = ElasticcDiaSource.objects.get( pk=data['ssObjectId'] )
            # VERIFY THAT STUFF MATCHES????
            return cursrc
        except ElasticcDiaSource.DoesNotExist:
            return ElasticcDiaSource.create( data )

# class ElasticcAlert(models.Model):
#     alertId = models.BigIntegerField( db_index=True )
#     diaSource = models.ForeignKey( ElasticcDiaSource, on_delete=models.CASCADE )
#     # prvDiaSources
#     # prvDiaForcedSources
#     # prvDiaNondetectionLimits
#     diaObject = models.ForeignKey( ElasticcDiaObject, on_delete=models.CASCADE )
#     ssObject = models.ForeignKey( ElasticcSSObject, on_delete=models.CASCADE )
#     # cutoutDifference
#     # cutoutTemplate

#     @staticmethod
#     def create( data ):
#         curalert = ElasticcAlert(
#             alertId = data['alertId'],
#             diaSource = data['diaSource'],
#             diaObject = data['diaObject'],
#             ssObject = data['ssObject']
#         )
#         curalert.save()
#         return curalert

#     @staticmethod
#     def load_or_create( data ):
#         # Alas, alertId is not unique, it seems.  I am going to
#         #  consider an alert to be the same thing if all of alertId,
#         #  diaSource.diaSourceId, diaObject.diaObjectId, and
#         #  ssObject.ssObject.Id are the same.  I don't know if this
#         #  is really the riht thing to do

#         them = ElasticcAlert.objects.filter( alertId=data['alertId'],
#                                              diaSource__diaSourceId=data['diaSource'].diaSourceId,
#                                              diaObject__diaObjectId=data['diaObject'].diaObjectId,
#                                              ssObject__ssObjectId=data['ssObject'].ssObjectId )
#         if len(them) == 0:
#             return ElasticcAlert.create( data )
#         if len(them) > 1:
#             sys.stderr.write( f"WARNING: Alert multiply defined: "
#                               f"alertId: {data['alertId']}, "
#                               f"diaSourceId: {data['diaSource'].diaSourceId}, "
#                               f"diaObjectId: {data['diaObject'].diaObjectId}, "
#                               f"ssObjectId: {data['ssObject'].ssObjectId}" )
#         return them[0]
    
