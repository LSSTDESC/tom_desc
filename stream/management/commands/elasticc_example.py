#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Load example ELAsTiCC brokerClassification alert and parse to ElasticcBrokerAlert."""
import logging
from pathlib import Path

import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from stream.models import ElasticcBrokerAlert, Topic
from stream.parsers import utils as parser_utils


logger = logging.getLogger(__name__)

elasticc_parser_class_name = settings.ELASTICC_PARSERS["broker-classification"]  # str
ELASTICC_PARSER = parser_utils.import_class(elasticc_parser_class_name)

# example alert conforming to the brokerClassification schema
parent_path = Path(__file__).parent.parent.parent.resolve() / "parsers/elasticc_schema"
ALERT_FILE = parent_path / "brokerClassification_example.avro"
# ALERT_FILE = parent_path / "brokerClassification_schemaless_example.avro"


class Command(BaseCommand):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def handle(self, *args, **options):
        """Listen to broker stream, parse each msg to an `ElasticcBrokerAlert` and save.

        This is an example function that demonstrates the parse-and-save logic
        using an example alert stored on file.

        The exact logic will vary between brokers,
        but should do the following for each message received:

            -   Instantiate an `ElasticcBrokerAlert` object (`alert`) with a `Topic`
                and the `message_bytes` (an Avro serialized bytes object that conforms
                to the ELAsTiCC schema lsst.v4_1.brokerClassification)

            -   Attach timestamps to the `alert` (optional)

            -   Parse the `message_bytes` into the `alert` fields using the
                `ELASTICC_PARSER`

            -   Call `alert.save()` to save the alert to the database.
        """
        # BROKER TODO: fill in your topic_name
        topic_name = "example_broker_topic"
        topic, _ = Topic.objects.get_or_create(name=topic_name)

        # BROKER TODO: replace with your logic to obtain a message from the topic
        msg_payload_avro_bytes, msg_metadata = self.retrieve_msg_from_broker_stream()
        # msg_payload_avro_bytes should be an Avro serialized bytes object
        # that conforms to the ELAsTiCC schema lsst.v4_1.brokerClassification.

        alert = ElasticcBrokerAlert.objects.create(
            topic=topic,
            message_bytes=msg_payload_avro_bytes,
        )

        # BROKER TODO: attach your timestamps
        # This is optional, but must be done before calling alert.save()
        self.attach_timestamps(alert, msg_metadata)

        with transaction.atomic():
            parser = ELASTICC_PARSER(alert)
            alert.parsed = parser.parse()
            if alert.parsed is True:
                # del alert.message_bytes  # reduce storage
                alert.save()

    def attach_timestamps(self, alert, metadata):
        """Attach timestamps to the `alert`."""
        alert.elasticc_publish_timestamp = metadata["elasticc_publish_timestamp"]
        alert.broker_ingest_timestamp = metadata["broker_ingest_timestamp"]
        alert.broker_publish_timestamp = metadata["broker_publish_timestamp"]

    def retrieve_msg_from_broker_stream(self):
        """Pull a message from the stream; this example just loads dummy data."""
        with open(ALERT_FILE, "rb") as f:
            msg_payload_avro_bytes = f.read()

        msg_metadata = self.example_metadata()

        return (msg_payload_avro_bytes, msg_metadata)

    def example_metadata(self):
        """Return metadata that includes timestamps."""
        # this example simply uses the current time for all timestamps
        now = datetime.datetime.now(datetime.timezone.utc)
        example_metadata = {
            "elasticc_publish_timestamp": now,
            "broker_ingest_timestamp": now,
            "broker_publish_timestamp": now,
        }
        return example_metadata
