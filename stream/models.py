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
