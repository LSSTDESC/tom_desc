from django.contrib import admin
from django.forms.widgets import Textarea
from .models import DiaObject, DiaSource, DiaForcedSource, DiaAlert, DiaTruth
from .models import BrokerMessage, BrokerClassifier, BrokerClassification

admin.site.register( [ DiaObject,
                       DiaSource,
                       DiaForcedSource,
                       DiaAlert,
                       DiaTruth,
                       BrokerMessage,
                       BrokerClassifier,
                       BrokerClassification
                       ]
                     )
