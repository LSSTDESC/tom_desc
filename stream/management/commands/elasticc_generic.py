#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Load example ELAsTiCC brokerClassification alert and parse to ElasticcBrokerAlert."""
from importlib import import_module
import logging
from pathlib import Path

import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from stream.models import ElasticcBrokerAlert, Topic


logger = logging.getLogger(__name__)

ELASTICC_PARSERS = settings.ELASTICC_PARSERS

# example alert conforming to the brokerClassification schema
parent_path = Path(__file__).parent.parent.parent.resolve()
ALERT_FILE = parent_path / "parsers/elasticc_schema/brokerClassification_example.avro"

# example topic
ELASTICC_TOPIC = "elasticc"


def get_parser_classes(topic):
    """Import and return the parser for the given `topic`."""
    parser_classes = []

    try:
        parsers = ELASTICC_PARSERS[topic]
    except KeyError:
        logger.log(msg=f'ELASTICC_PARSERS not found for topic: {topic}.', level=logging.WARNING)
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
        """Load the example broker alert from file and parse to ElasticcBrokerAlert."""
        topic, _ = Topic.objects.get_or_create(name=ELASTICC_TOPIC)

        # create fake timestamps
        # assumes these will not be in the Avro packet, but rather in metadata
        now = datetime.datetime.now(datetime.timezone.utc)
        elasticc_publish_timestamp = now
        broker_ingest_timestamp = now
        broker_publish_timestamp = now

        # load example alert
        with open(ALERT_FILE, "rb") as f:
            alert_bytes = f.read()

        # Instantiate an Alert with topic and timestamps
        alert = ElasticcBrokerAlert.objects.create(
            topic=topic,
            elasticc_publish_timestamp=elasticc_publish_timestamp,
            broker_ingest_timestamp=broker_ingest_timestamp,
            broker_publish_timestamp=broker_publish_timestamp,
        )

        # Parse the alert_bytes and save
        for parser_class in get_parser_classes(topic.name):
            with transaction.atomic():
                parser = parser_class(alert)
                alert.parsed = parser.parse(alert_bytes)
                if alert.parsed is True:
                    alert.save()
