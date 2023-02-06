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

# ======================================================================-
    
# class Target(models.Model):
#     name = models.CharField(max_length=200)
#     right_ascension = models.FloatField(null=True, blank=True)
#     declination = models.FloatField(null=True, blank=True)
#     created = models.DateTimeField(auto_now_add=True)
#     modified = models.DateTimeField(auto_now=True)

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
#             models.Index(fields=['timestamp'], name='timestamp_idx')
#         ]
