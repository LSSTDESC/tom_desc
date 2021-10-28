#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Listen to a Pitt-Google Pub/Sub stream and save alerts to the database."""
from importlib import import_module
import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

# from stream.models import Alert, Topic

from tom_pittgoogle.consumer_stream_python import ConsumerStreamPython as Consumer


logger = logging.getLogger(__name__)

PITTGOOGLE_CONSUMER_CONFIGURATION = settings.PITTGOOGLE_CONSUMER_CONFIGURATION


class Command(BaseCommand):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.consumer = Consumer(PITTGOOGLE_CONSUMER_CONFIGURATION['subscription_name'])

    def handle(self, *args, **options):
        """Pull and process alerts from a Pitt-Google Pub/Sub stream.

        -   Execute a "streaming pull" in a background thread.
            Asyncronously, pull alerts and process them through the callback,
            `parse_and_save`.

        -   Stop when a stoping condition is reached.

        -   Block until the processing is complete and the connection has been closed.
        """
        _ = self.consumer.stream_alerts(
            # callback=self.parse_and_save,
            user_filter=self.parse_and_save,
            parameters=PITTGOOGLE_CONSUMER_CONFIGURATION,  # stopping conditions
        )

    @staticmethod
    def parse_and_save(alert, parameters):
        """Parse the alert and save to the database."""
        mylogger = logging.getLogger(__name__)
        mylogger.info("Success! We have pulled an alert.")
        return alert
