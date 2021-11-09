from django.contrib import admin
from .models import Target, Topic, ElasticcBrokerAlert, Event, EventAttributes, Alert

admin.site.register([Target, Topic, ElasticcBrokerAlert, Event, EventAttributes, Alert])
