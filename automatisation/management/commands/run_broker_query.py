from django.core.management.base import BaseCommand

from tom_alerts.models import BrokerQuery
from tom_alerts.alerts import get_service_class, GenericAlert
from tom_targets.models import Target, TargetList

from time import sleep
import sys

class Command(BaseCommand):
    help = 'Runs saved alert queries and saves the results as Targets'

    def add_arguments(self, parser):
        parser.add_argument(
            'query_name',
            help='One or more saved queries to run'
        )
        parser.add_argument(
            'target_list_name',
            help='Name of the TargetList in which targets need to be saved'
        )

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
        try:
            query = BrokerQuery.objects.get(name=options['query_name'])
            broker_class = get_service_class(query.broker)
            broker = broker_class()
            alerts = broker.fetch_alerts(query.parameters)
            ntarget = 0

            # Target list - get or create on-the-fly
            tl = TargetList(name=options['target_list_name'])
            tl.save()

            while True:
                try:
                    generic_alert = broker.to_generic_alert(next(alerts))

                    # Get or create a new target
                    target, created = self.to_target_from_generic(generic_alert)
                    if created:
                        target.save()
                        tl.targets.add(target)
                        self.stdout.write('Created target: {}'.format(target))
                        ntarget += 1
                    else:
                        self.stdout.write('Target with the name {} already created - skipping'.format(generic_alert.name))
                except StopIteration:
                    self.stdout.write('Finished creating {} target(s)'.format(ntarget))
                    sys.exit()
                sleep(1)
        except KeyboardInterrupt:
            self.stdout.write('Exiting...')
