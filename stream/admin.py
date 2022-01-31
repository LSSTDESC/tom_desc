from django.contrib import admin
from django.forms.widgets import Textarea
from django.contrib.gis.db import models as gis_models
from .models import Target, Topic
from .models import ElasticcBrokerClassification, ElasticcBrokerClassifier, ElasticcBrokerMessage
from .models import Event, EventAttributes, Alert
from .models import RknopTest
from .models import ElasticcDiaObject, ElasticcDiaSource, ElasticcDiaTruth

admin.site.register( [ Target,
                       Topic,
                       ElasticcBrokerClassification,
                       ElasticcBrokerClassifier,
                       ElasticcBrokerMessage,
                       ElasticcDiaTruth,
                       Event, EventAttributes,
                       Alert,
                       RknopTest,
                      ]
                     )

# It's very annoying that DJango GIS admin by default uses a map of the world for
# Point objects.  I just want numbers.

class PointsAsNumbers(admin.ModelAdmin):
    formfield_overrides = {
        gis_models.PointField: { 'widget': Textarea }
    }

admin.site.register( ElasticcDiaObject, PointsAsNumbers )
admin.site.register( ElasticcDiaSource, PointsAsNumbers )
# # I need to do this here rather than in the list above because ElasticcDiaTruth needs ...Source defined
# admin.site.register( ElasticcDiaTrutH )
