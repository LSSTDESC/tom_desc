#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Listen to a Pitt-Google Pub/Sub stream and save alerts to the database."""
from importlib import import_module
import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from stream.models import Alert, Topic

from tom_pittgoogle.consumer_stream_python import ConsumerStreamPython as Consumer


logger = logging.getLogger(__name__)

PITTGOOGLE_CONSUMER_CONFIGURATION = settings.PITTGOOGLE_CONSUMER_CONFIGURATION
PITTGOOGLE_PARSERS = settings.PITTGOOGLE_PARSERS


def get_parser_class(topic):
    """Import and return the parser for the given `topic`."""
    try:
        parser = PITTGOOGLE_PARSERS[topic]
    except KeyError:
        logger.log(
            msg=f'PITTGOOGLE_PARSERS not found, topic: {topic}.', level=logging.WARNING
        )
        return None

    mod_name, class_name = parser.rsplit('.', 1)
    try:
        mod = import_module(mod_name)
        clazz = getattr(mod, class_name)
    except (ImportError, AttributeError):
        raise ImportError(f'Unable to import parser {parser}')
    parser_class = clazz

    return parser_class


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
        consumer = Consumer(PITTGOOGLE_CONSUMER_CONFIGURATION['subscription_name'])

        kwargs = {
            # kwargs to be passed to parse_and_save
            # note that in Pub/Sub, the topic and subscription can have different names
            "topic_name": consumer.topic_path.rsplit("/", 1),
            "send_alert_bytes": True,
            "send_metadata": True,
            # kwargs for stopping conditions
            **PITTGOOGLE_CONSUMER_CONFIGURATION,
        }

        # pull and process alerts in a background thread. block until complete.
        _ = consumer.stream_alerts(
            user_callback=self.parse_and_save,
            **kwargs
        )

    @staticmethod
    def parse_and_save(alert_dict, alert_bytes, metadata_dict, **kwargs):
        """Parse the alert and save to the database."""
        topic, _ = Topic.objects.get_or_create(name=kwargs["topic_name"])
        alert = Alert.objects.create(
            topic=topic,
            raw_message=alert_bytes,
            parsed_message=alert_dict,
            metadata=metadata_dict,
        )

        parser_class = get_parser_class(topic.name)
        with transaction.atomic():
            # Get the parser class, instantiate it, parse the alert, and save it
            parser = parser_class(alert)
            alert.parsed = parser.parse()
            if alert.parsed is True:
                alert.save()

        # TODO: catch errors
        success = True

        return success
