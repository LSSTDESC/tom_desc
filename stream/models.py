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


class ElasticcBrokerMetadata(models.Model):
    """Model for the timestamps associated with an alert from an ELAsTiCC broker."""

    brokerMessageId = models.CharField(primary_key=True, max_length=255)

    # timestamps as datetime.datetime (DateTimeField)
    descIngestTimestamp = models.DateTimeField(auto_now_add=True)  # auto-generated
    elasticcPublishTimestamp = models.DateTimeField(null=True)
    brokerIngestTimestamp = models.DateTimeField(null=True)
    brokerPublishTimestamp = models.DateTimeField(null=True)

    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            # models.Index(fields=['alertId'], name='alertId'),  # is this wanted?
        ]


class ElasticcBrokerClassification(models.Model):
    """Model for an alert conforming to ELAsTiCC brokerClassification schema.

    https://github.com/LSSTDESC/plasticc_alerts/blob/main/Examples/starterkit/plasticc_schema/lsst.v4_1.brokerClassification.avsc
    """

    alertId = models.CharField(max_length=200)
    diaObjectId = models.CharField(max_length=200)
    brokerMessageId = models.ForeignKey(
        ElasticcBrokerMetadata, on_delete=models.PROTECT, null=True
    )

    # using null=True to be forgiving, so the rest of the info still gets stored
    classifierName = models.CharField(max_length=200, null=True)
    classId = models.CharField(max_length=50, null=True)
    probability = models.FloatField(null=True)

    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            # models.Index(fields=['alertId'], name='alertId'),  # is this wanted?
        ]
