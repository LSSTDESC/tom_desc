#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Listen to a Pitt-Google Pub/Sub stream and save alerts to the database."""
from importlib import import_module
import logging

from django.conf import settings
from django.contrib.auth.models import Group
from django.core.management.base import BaseCommand
# from django.db import transaction

# from stream.models import Alert, Topic

from tom_pittgoogle.consumer_stream_python import ConsumerStreamPython as Consumer


logger = logging.getLogger(__name__)

PITTGOOGLE_CONSUMER_CONFIGURATION = settings.PITTGOOGLE_CONSUMER_CONFIGURATION
PITTGOOGLE_PARSERS = settings.PITTGOOGLE_PARSERS


def get_parser_classes(topic):
    """."""
    parser_classes = []

    try:
        parsers = PITTGOOGLE_PARSERS[topic]
    except KeyError:
        logger.log(msg=f'PITTGOOGLE_PARSERS not found for topic: {topic}.', level=logging.WARNING)
        return parser_classes

    for parser in parsers:
        mod_name, class_name = parser.rsplit('.', 1)
        try:
            mod = import_module(mod_name)
            clazz = getattr(mod, class_name)
        except (ImportError, AttributeError):
            raise ImportError(f'Unable to import parser {parser}')
        parser_classes.append(clazz)

    return parser_classes


class Command(BaseCommand):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def handle(self, *args, **options):
        """Pull and process alerts from a Pitt-Google Pub/Sub stream.

        -   Execute a "streaming pull" in a background thread.
            Asyncronously, pull alerts and process them through the callback,
            `parse_and_save`.

        -   Stop when a stoping condition is reached.

        -   Block until the processing is complete and the connection has been closed.
        """
        kwargs = {
                # "desc_group": Group.objects.get(name='DESC'),
                # "broker": get_service_class('Pitt-Google StreamPython')(),
                **PITTGOOGLE_CONSUMER_CONFIGURATION,  # stopping conditions
        }

        consumer = Consumer(PITTGOOGLE_CONSUMER_CONFIGURATION['subscription_name'])
        _ = consumer.stream_alerts(
            user_callback=self.parse_and_save,
            **kwargs
        )

    @staticmethod
    def parse_and_save(alert_dict, **kwargs):
        """Parse the alert and save to the database."""
        # TODO
        return
