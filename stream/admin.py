from django.contrib import admin
from .models import Target, Topic, ElasticcBrokerClassification, ElasticcBrokerMetadata, Event, EventAttributes, Alert

admin.site.register([Target, Topic, ElasticcBrokerClassification, ElasticcBrokerMetadata, Event, EventAttributes, Alert])
