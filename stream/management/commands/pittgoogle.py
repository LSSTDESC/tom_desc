#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Listen to a Pitt-Google Pub/Sub stream and save alerts to the database."""
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from stream.models import Alert, Topic

# I will write a Consumer that you can simply import here
from pittgoogle import Consumer


PITTGOOGLE_CONSUMER_CONFIGURATION = settings.PITTGOOGLE_CONSUMER_CONFIGURATION
PITTGOOGLE_PARSERS = settings.PITTGOOGLE_PARSERS


def get_parser_classes(topic):
    parser_classes = []

    parsers = PITTGOOGLE_PARSERS[topic]
    for parser in parsers:
        # TODO
        pass

    return parser_classes


class Command(BaseCommand):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.consumer = Consumer(PITTGOOGLE_CONSUMER_CONFIGURATION['SUBSCRIPTION_NAME'])

    def handle(self, *args, **options):
        """Pull and process alerts through the `callback` (`parse_and_save`),
        in a background thread, until a stoping condition is reached.
        Block until the processing is complete and the connection has been closed.
        """
        self.consumer.stream_alerts(
            stop_conditions=PITTGOOGLE_CONSUMER_CONFIGURATION['stop_conditions'],
            callback=self.parse_and_save
        )

        # If you'd prefer to iterate over the alerts, it would look like this:
        # alert_list = self.consumer.stream_alerts(stop_conditions=stop_conditions)
        # for alert in alert_list:
        #     self.parse_and_save(alert, self.consumer.topic_name)


    def parse_and_save(alert, topic_name):
        topic, _ = Topic.objects.get_or_create(name=topic_name)
        alert = Alert.objects.create(topic=topic, raw_message=alert.data)

        for parser_class in get_parser_classes(topic.name):
            with transaction.atomic():
                # Get the parser class, instantiate it, parse the alert, and save it
                parser = parser_class(alert)
                alert.parsed = parser.parse()
                if alert.parsed is True:
                    alert.save()
                    break
