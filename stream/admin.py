from django.contrib import admin
from .models import Target, Topic, ElasticcBrokerClassification, ElasticcBrokerClassifier, ElasticcBrokerMessage, Event, EventAttributes, Alert

admin.site.register([Target, Topic, ElasticcBrokerClassification, ElasticcBrokerClassifier, ElasticcBrokerMessage, Event, EventAttributes, Alert])
