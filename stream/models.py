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


class ElasticcBrokerAlert(models.Model):
    """Model for an alert conforming to ELAsTiCC brokerClassification schema.

    https://github.com/LSSTDESC/plasticc_alerts/blob/main/Examples/starterkit/plasticc_schema/lsst.v4_1.brokerClassification.avsc
    """

    # basic data
    topic = models.ForeignKey(Topic, on_delete=models.PROTECT)
    alertId = models.CharField(max_length=200)
    classifierNames = models.CharField(max_length=255)  # use TextField if need more len

    # classification probabilities
    # assuming each probability should get it's own field
    class_10 = models.FloatField(null=True)             # Bogus
    class_20 = models.FloatField(null=True)             # Real
    class_020 = models.FloatField(null=True)            # Real/Other
    class_120 = models.FloatField(null=True)            # Static
    class_0120 = models.FloatField(null=True)           # Static/Other
    class_1120 = models.FloatField(null=True)           # Non-Recurring
    class_01120 = models.FloatField(null=True)          # Non-Recurring/Other
    class_11120 = models.FloatField(null=True)          # SN-like
    class_011120 = models.FloatField(null=True)         # SN-like/Other
    class_111120 = models.FloatField(null=True)         # Ia
    # etc for all classes

    # timestamps as datetime.datetime (DateTimeField)
    desc_ingest_timestamp = models.DateTimeField(auto_now_add=True)  # auto-generated
    elasticc_publish_timestamp = models.DateTimeField(null=True)
    broker_ingest_timestamp = models.DateTimeField(null=True)
    broker_publish_timestamp = models.DateTimeField(null=True)

    # other
    message_bytes = models.BinaryField(null=True)
    parsed = models.BooleanField(default=False)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['alertId'], name='alertId'),  # is this wanted?
            # should there also be an index that is unique across brokers?
        ]
