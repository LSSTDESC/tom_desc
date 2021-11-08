#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Listen to a Pitt-Google Pub/Sub stream and save alerts to the database."""
from importlib import import_module
import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from stream.models import ElasticcBrokerAlert, Topic

# from tom_pittgoogle.consumer_stream_python import ConsumerStreamPython as Consumer
from stream.management.commands.pittgoogle_consumer import ConsumerStreamPython as Consumer


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

        send_data = "alert_bytes"
        include_metadata = True
        user_callback = self.parse_and_save
        flow_configs = PITTGOOGLE_CONSUMER_CONFIGURATION
        user_kwargs = {"topic_name": consumer.topic_path.split("/")[-1]}

        # pull and process alerts in a background thread. block until complete.
        _ = consumer.stream_alerts(
            send_data=send_data,
            include_metadata=include_metadata,
            user_callback=user_callback,
            flow_configs=flow_configs,
            **user_kwargs
        )

    @staticmethod
    def parse_and_save(message_data, topic_name="elasticc"):
        """Parse the alert and save to the database."""
        alert_bytes = message_data["alert_bytes"]
        metadata_dict = message_data["metadata_dict"]
        topic, _ = Topic.objects.get_or_create(name=topic_name)

        # Instantiate an Alert with topic and timestamps
        try:
            alert = ElasticcBrokerAlert.objects.create(
                topic=topic,
                # timestamps should be datetime.datetime
                elasticc_publish_timestamp=metadata_dict["kafka.timestamp"],
                broker_ingest_timestamp=None,
                broker_publish_timestamp=metadata_dict["publish_time"],
            )
        # catch everything until we know what errors we're looking for
        except Exception as e:
            logger.warn(
                (
                    f"Error instantiating Alert with topic: {topic}, "
                    f"metadata: {metadata_dict}"
                    f"\n{e}"
                )
            )
            return {"ack": False}

        # Parse the alert_bytes and save
        try:
            parser_class = get_parser_class(topic.name)
            with transaction.atomic():
                parser = parser_class(alert)
                alert.parsed = parser.parse(alert_bytes)
                if alert.parsed is True:
                    alert.save()
        # catch everything until we know what errors we're looking for
        except Exception as e:
            logger.warn(f"Error parsing alert bytes.\n{e}")
            return {"ack": False}

        return {"ack": True}
