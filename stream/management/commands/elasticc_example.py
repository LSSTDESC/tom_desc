#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Load example ELAsTiCC brokerClassification alert and parse to ElasticcBrokerAlert."""
import logging
from pathlib import Path

import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
import numpy as np

from stream.parsers import utils as parser_utils


logger = logging.getLogger(__name__)

# BROKER TODO: Fill in your values.
BROKER_NAME = "ExampleBroker"
BROKER_TOPIC = "example_broker_topic"

# import the parser class
elasticc_parser_class_name = settings.ELASTICC_PARSERS["broker-classification"]  # str
ELASTICC_PARSER = parser_utils.import_class(elasticc_parser_class_name)  # class

# example messages conforming to the brokerClassification schema
parent_path = Path(__file__).parent.parent.parent.resolve() / "parsers/elasticc_schema"
EXAMPLE_BROKER_MESSAGES = [
    parent_path / "brokerClassification_example.avro",
    parent_path / "brokerClassification_schemaless_example.avro",
]


class Command(BaseCommand):
    """Process a broker stream."""

    def handle(self, *args, **options):
        """Listen to a broker stream and save each message to the database.

        This is an example function that demonstrates the basic workflow,
        using example alerts that are stored on file.

        The basic workflow is as follows.
        Brokers should substitute their own logic for steps 1, 2, and 4.

            1.  Listen to a broker stream/topic.
                For each message complete the remaining steps.

            2.  Unpack the message payload into an Avro serialized bytes
                object, and the attributes into a dictionary.

            3.  Instantiate an ELASTICC_PARSER, and call its parse_and_save() method.
                This will create database entries for each reported classification,
                along with entries for the message attributes and the classifier.
                See the parser's __init__ docstring for requirements.

            4.  Respond to the parser's success/failure, as desired.
                The parser returns a bool indicating whether all database entries were
                created successfully. False indicates a failure to create one or more
                entries (some entries may have been successful, but at least one was
                not).
        """
        # 1. Listen to the example broker stream.
        for file_path in EXAMPLE_BROKER_MESSAGES:

            # 2. Unpack the message payload and attributes.
            msg_payload_avro_bytes, msg_attrs_dict = self.unpack_message(file_path)

            # 3. Use the parser to save entries to the database.
            parser = ELASTICC_PARSER(msg_payload_avro_bytes, msg_attrs_dict)
            success = parser.parse_and_save()

            # 4. Respond to the parser's success/failure, as desired.
            if success:
                continue
            else:
                logger.warn(f"Error parsing msg. ID: {msg_attrs_dict['messageId']}.")

    def unpack_message(self, path_to_example_message):
        """Return message payload and attributes. This example just loads dummy data."""
        # load the Avro message
        with open(path_to_example_message, "rb") as f:
            msg_payload_avro_bytes = f.read()

        # collect message attributes
        example_message_id = np.random.randint(100000, 1000000)
        example_timestamp = datetime.datetime.now(datetime.timezone.utc)
        msg_attrs_dict = {
            # required info
            "brokerName": BROKER_NAME,
            "brokerTopic": BROKER_TOPIC,
            # optional info
            "messageId": example_message_id,
            "brokerPublishTimestamp": example_timestamp,
        }
        # Two additional timestamps are included in the msg payload,
        # elasticcPublishTimestamp and brokerIngestTimestamp

        return (msg_payload_avro_bytes, msg_attrs_dict)
