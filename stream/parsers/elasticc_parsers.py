#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Parse alerts conforming to ELAsTiCC schema: lsst.v4_1.brokerClassification.avsc."""
import logging
from pathlib import Path

from django.db import IntegrityError
import fastavro

from stream.models import ElasticcBrokerClassification, ElasticcBrokerMessage, ElasticcBrokerClassifier
from stream.parsers import utils as parser_utils
from stream.parsers.base_parser import BaseParser


logger = logging.getLogger(__name__)

# brokerClassification schema
parent_path = Path(__file__).parent.resolve()
avsc_file = parent_path / "elasticc_schema/lsst.v4_1.brokerClassification.avsc"
BROKERCLASSIFICATION_SCHEMA = fastavro.schema.load_schema(avsc_file)


class ElasticcBrokerMessageParser(BaseParser):
    """Parse alerts conforming to ELAsTiCC schema: lsst.v4_1.brokerClassification.avsc.

    Example alert (after casting Avro -> dict):
    {
        "alertId": 123456789,
        "diaObjectId": 135792468,
        "classifications": [
            {
                "classifierName": "RealBogus_v0.1",
                "classId": 10,
                "probability": 0.11,
            },
            {
                "classifierName": "RealBogus_v0.1",
                "classId": 20,
                "probability": 0.89,
            },
            {
                "classifierName": "SuperNNova_v1.3",
                "classId": 11120,
                "probability": 0.82,
            },
            {
                "classifierName": "SuperNNova_v1.3",
                "classId": 111120,
                "probability": 0.11,
            },
        ],
    }
    """

    def __init__(self, msg_payload, msg_attrs, *args, **kwargs):
        """Initialize the parser with a message payload and attributes.

        This overrides BaseParser.__init__, since it must accept different arguments.

        Args:
            msg_payload (bytes):
                Avro serialized bytes object conforming to the ELAsTiCC schema,
                lsst.v4_1.brokerClassification.avsc.

            msg_attrs (dict):
                Attributes associated with the message.
                The keys and value-types should be as follows:
                    'brokerName':               string (required)
                    'brokerTopic':              string (required)
                    'messageId':                int (optional)
                    'elasticcPublishTimestamp': datetime.datetime (optional)
                    'brokerIngestTimestamp':    datetime.datetime (optional)
                    'brokerPublishTimestamp':   datetime.datetime (optional)
        """
        self.msg_payload = msg_payload
        self.msg_attrs = msg_attrs
        # Attribute for the ElasticcBrokerMessage object (unique for given message).
        # save_attrs() will update this value, if it successfully creates the object.
        # save_classification() will use it to associate classifications with the message.
        self.elasticc_broker_attrs = None

    def __repr__(self):
        return 'ELAsTiCC broker message parser'

    def parse_and_save(self):
        """Create database entries for the message.

        Creates one entry in the ElasticcBrokerClassification table for each reported
        classification (possibly multiple entries per alert),
        and one entry in the ElasticcBrokerMessage table (one entry per alert).
        If the message contains a classifier that does not already have an entry in the
        ElasticcBrokerClassifier table, one is created.

        Returns:
            bool:
                True if no errors were encountered (indicates that all database
                entries were created).
                False if one or more errors occurred (indicates that one or more
                of the expected database entries were *not* created).
        """
        # Unpack the payload. If this fails, return immediately.
        msg_dicts, success = self.deserialize_and_validate_payload()  # List[dict], bool
        if not success:
            return success

        # Save attributes to the database.
        attrs_success = self.save_attrs()

        # Save classifications to the database.
        classifications_success = True
        for msg_dict in msg_dicts:
            for classification in msg_dict["classifications"]:
                class_success = self.save_classification(
                    msg_dict["alertId"],
                    msg_dict["diaObjectId"],
                    classification,
                )
                if not class_success:
                    classifications_success = False

        # Report whether all operations were successful.
        return (attrs_success and classifications_success)

    def deserialize_and_validate_payload(self):
        """Deserialze and validate self.msg_payload."""
        success = True

        msg_dicts = parser_utils.avro_to_list_of_dicts(
            self.msg_payload, BROKERCLASSIFICATION_SCHEMA
        )

        # if unable to deserialize and validate, log the attributes
        if msg_dicts is None:
            success = False
            logger.warning(
                f"Unable to deserialize payload. Message attributes: {self.msg_attrs}"
            )

        return (msg_dicts, success)

    def save_attrs(self):
        """Save message attributes to the ElasticcBrokerMessage database table."""
        success = True
        attrs = self.msg_attrs

        # save to the database
        try:
            elasticc_broker_attrs = ElasticcBrokerMessage.objects.create(
                streamMessageId=attrs.get("messageId"),
                topicName=attrs.get("brokerTopic"),
                elasticcPublishTimestamp=attrs.get("elasticcPublishTimestamp"),
                brokerIngestTimestamp=attrs.get("brokerIngestTimestamp"),
                brokerPublishTimestamp=attrs.get("brokerPublishTimestamp"),
            )

        # if there was an error, log it and move on
        except Exception as e:
            success = False
            logger.warning(
                (
                    "Unable to create an ElasticcBrokerMessage entry. "
                    f"Attributes received: {attrs} "
                    f"Error message: {e} "
                )
            )

        # set the class attribute so save_classification() can use it
        else:
            self.elasticc_broker_attrs = elasticc_broker_attrs

        return success

    def save_classification(self, alertId, diaObjectId, classification):
        """Save a classification to the ElasticcBrokerClassification database table."""
        success = True
        classifier, created = ElasticcBrokerClassifier.objects.get_or_create(
            brokerName=self.msg_attrs["brokerName"],
            classiferName=classification["classifierName"],
        )

        # save to the database
        try:
            _ = ElasticcBrokerClassification.objects.create(
                alertId=alertId,
                diaObjectId=diaObjectId,
                dbMessageIndex=self.elasticc_broker_attrs,
                dbClassifierIndex=classifier,
                classId=classification["classId"],
                probability=classification["probability"],
            )

        # if there was an error, just log it and move on
        except Exception as e:
            success = False
            logger.warning(
                (
                    "Unable to create an ElasticcBrokerClassification entry. "
                    f"alertId: {alertId}, diaObjectId: {diaObjectId}, "
                    f"message attributes: {self.msg_attrs}, "
                    f"classification: {classification}. "
                    f"Error message: {e}"
                )
            )

        return success

    def parse(self):
        """Pass. This function is required by BaseParser, but we don't use it here."""
        pass
