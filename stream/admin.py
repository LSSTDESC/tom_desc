from django.contrib import admin
from .models import Target, Topic, Event, EventAttributes, Alert

admin.site.register([Target, Topic, Event, EventAttributes, Alert])