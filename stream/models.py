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
    # We need a way to pass in the timestamps.
    # If others pass this through the raw or parsed message,
    # I (Troy) could just shove ours in there as well, and delete the following line
    metadata = models.JSONField(default=dict)
    timestamp = models.DateTimeField(null=True, blank=True)
    # assuming timestamp is currently being used for survey_publish_timestamp
    # survey_publish_timestamp = models.DateTimeField(null=True, blank=True)
    broker_ingest_timestamp = models.DateTimeField(null=True, blank=True)
    broker_publish_timestamp = models.DateTimeField(null=True, blank=True)
    desc_ingest_timestamp = models.DateTimeField(null=True, blank=True)
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

# class ElasticcAlert(models.Model):
#     # target_id = models.ForeignKey(Target, on_delete=models.CASCADE)
#     topic = models.ForeignKey(Topic, on_delete=models.PROTECT)
#     events = models.ManyToManyField(Event)
#     identifier = models.CharField(max_length=200)
#     timestamp = models.DateTimeField(null=True, blank=True)
#     coordinates = gis_models.PointField(null=True, blank=True)
#     parsed_message = models.JSONField(default=dict)
#     raw_message = models.JSONField(default=dict)
#     parsed = models.BooleanField(default=False)
#     created = models.DateTimeField(auto_now_add=True)
#     modified = models.DateTimeField(auto_now=True)
#
#     class Meta:
#         indexes = [
#             models.Index(fields=['timestamp'], name='timestamp_idx'),
#         ]
