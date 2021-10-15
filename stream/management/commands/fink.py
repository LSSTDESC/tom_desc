from importlib import import_module
import json
import logging

# from confluent_kafka import Consumer
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from fink_client.consumer import AlertConsumer
from fink_client.configuration import load_credentials


from stream.models import Alert, Topic

from tom_alerts.alerts import get_service_class, GenericAlert
from tom_targets.models import Target, TargetList

logger = logging.getLogger(__name__)

FINK_CONSUMER_CONFIGURATION = settings.FINK_CONSUMER_CONFIGURATION
FINK_TOPICS = settings.FINK_TOPICS

class Command(BaseCommand):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_target_from_generic(self, alert: GenericAlert) -> Target:
        """ Redirect query result to a Target using GenericAlert attributes

        We currently just use the name, position (ra, dec), and type.

        Parameters
        ----------
        alert: dict
            GenericAlert instance

        """
        target, created = Target.objects.get_or_create(
            name=alert.name
        )
        if created:
            target.ra = alert.ra
            target.dec = alert.dec
            target.type = 'SIDEREAL'
        else:
            # you could check if target.ra & alert.ra match etc.
            # and create a new alert is needed
            pass
        return target, created

    def handle(self, *args, **options):

        broker_class = get_service_class('Fink')
        broker = broker_class()
        
        # Instantiate a consumer
        consumer = AlertConsumer(FINK_TOPICS, FINK_CONSUMER_CONFIGURATION)
        
        maxtimeout = 5
        while True:
            finkalert = None

            # Poll the servers
            finktopic, finkalert, key = consumer.poll(maxtimeout)
            if finkalert is not None:
                print(finkalert['objectId'])
                finkalert['i:objectId'] = finkalert['objectId']
                finkalert['i:candid'] = finkalert['candid']
                finkalert['d:rfscore'] = finkalert['rfscore']
                finkalert['i:jd'] = finkalert['candidate']['jd']
                finkalert['i:ra'] = finkalert['candidate']['ra']
                finkalert['i:dec'] = finkalert['candidate']['dec']
                finkalert['i:magpsf'] = finkalert['candidate']['magpsf']
                generic_alert = broker.to_generic_alert(finkalert)
                target, created = self.to_target_from_generic(generic_alert)
                if created:
                    target.save()
                    #tl.targets.add(target)
                    self.stdout.write('Created target: {}'.format(target))
                else:
                    self.stdout.write('Target with the name {} already created - skipping'.format(generic_alert.name))
                
