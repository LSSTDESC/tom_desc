#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Parse alerts conforming to ELAsTiCC schema: lsst.v4_1.brokerClassification.avsc."""
import logging
from pathlib import Path

from django.db import IntegrityError
import fastavro

from stream.models import ElasticcBrokerClassification, ElasticcBrokerMetadata
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
        "diaObjectId": "Example13579",
        "classifications": [
            {
                "classifierName": "PittGoogle_RealBogus_v0.1",
                "classId": "10",
                "probability": 0.11,
            },
            {
                "classifierName": "PittGoogle_RealBogus_v0.1",
                "classId": "20",
                "probability": 0.89,
            },
            {
                "classifierName": "PittGoogle_SuperNNova_v1.3",
                "classId": "11120",
                "probability": 0.82,
            },
            {
                "classifierName": "PittGoogle_SuperNNova_v1.3",
                "classId": "111120",
                "probability": 0.11,
            },
        ],
    }
    """

    def __init__(self, msg_payload, msg_metadata, *args, **kwargs):
        """Initialize the parser with a message payload and metadata.

        This overrides BaseParser.__init__, since it must accept different arguments.

        Args:
            msg_payload (bytes):
                Avro serialized bytes object conforming to the ELAsTiCC schema,
                lsst.v4_1.brokerClassification.avsc.

            msg_metadata (dict):
                Metadata associated with the message.
                The keys and value-types should be as follows:
                    'brokerName':               string (required)
                    'brokerTopic':              string (required)
                    'messageId':                string (required,
                                                        must be unique within topic)
                    'elasticcPublishTimestamp': datetime.datetime (optional)
                    'brokerIngestTimestamp':    datetime.datetime (optional)
                    'brokerPublishTimestamp':   datetime.datetime (optional)
        """
        self.msg_payload = msg_payload
        self.msg_metadata = msg_metadata
        self.brokerMessageId = self.create_broker_message_id()
        # Attribute for the ElasticcBrokerMetadata object (unique for given message).
        # save_metadata() will update this value, if it successfully creates the object.
        # save_classification() will use it to associate classifications with metadata.
        self.elasticc_broker_metadata = None

    def __repr__(self):
        return 'ELAsTiCC broker message parser'

    def parse_and_save(self):
        """Create database entries for the message classifications and metadata.

        Creates one entry in the ElasticcBrokerClassification table for each reported
        classification (possibly multiple entries per alert),
        and one entry in the ElasticcBrokerMetadata table (one entry per alert).

        Returns:
            bool:
                True if no errors were encountered (indicates that database
                entries for all classifications and the metadata were created).
                False if one or more errors occurred (indicates that one or more
                of the expected database entries were *not* created).
        """
        # Unpack the payload. If this fails, return immediately.
        msg_dicts, success = self.deserialize_and_validate_payload()  # List[dict], bool
        if not success:
            return success

        # Save metadata to the database.
        metadata_success = self.save_metadata()

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
        return (metadata_success and classifications_success)

    def create_broker_message_id(self):
        """Combine and return the messageId, brokerName, and brokerTopic."""
        try:
            broker = self.msg_metadata["brokerName"]
            topic = self.msg_metadata["brokerTopic"]
            msgid = self.msg_metadata["messageId"]

        except KeyError:
            msg = (
                "Unable to create brokerMessageId. The metadata dict does not contain "
                "all of the required keys (messageId, brokerName, and brokerTopic)."
            )
            raise KeyError(msg)

        brokerMessageId = f"{broker}_{topic}_{msgid}".replace(" ", "-")
        return brokerMessageId

    def deserialize_and_validate_payload(self):
        """Deserialze and validate self.msg_payload."""
        success = True

        msg_dicts = parser_utils.avro_to_list_of_dicts(
            self.msg_payload, BROKERCLASSIFICATION_SCHEMA
        )

        # if unable to deserialize and validate, log self.brokerMessageId
        if msg_dicts is None:
            success = False
            logger.warning(
                f"Unable to deserialize payload. brokerMessageId {self.brokerMessageId}"
            )

        return (msg_dicts, success)

    def save_metadata(self):
        """Save self.msg_metadata to the ElasticcBrokerMetadata database table."""
        success = True
        metadata = self.msg_metadata

        # save to the database
        try:
            elasticc_broker_metadata = ElasticcBrokerMetadata.objects.create(
                brokerMessageId=self.brokerMessageId,
                elasticcPublishTimestamp=metadata.get("elasticcPublishTimestamp"),
                brokerIngestTimestamp=metadata.get("brokerIngestTimestamp"),
                brokerPublishTimestamp=metadata.get("brokerPublishTimestamp"),
            )

        # if primary key (brokerMessageId) is not unique, just log the error and move on
        except IntegrityError as e:
            success = False
            logger.warning(
                (
                    "Unable to create an ElasticcBrokerMetadata entry. "
                    f"brokerMessageId {self.brokerMessageId} is not unique."
                    f"\nMetadata received: {metadata}"
                    f"\nError message: {e}"
                )
            )

        # set the class attribute so save_classification() can use it
        else:
            self.elasticc_broker_metadata = elasticc_broker_metadata

        return success

    def save_classification(self, alertId, diaObjectId, classification):
        """Save a classification to the ElasticcBrokerClassification database table."""
        success = True

        # save to the database
        try:
            _ = ElasticcBrokerClassification.objects.create(
                alertId=alertId,
                diaObjectId=diaObjectId,
                brokerMessageId=self.elasticc_broker_metadata,
                classifierName=classification["classifierName"],
                # classId=hex(int(classification["classId"], 10)),
                classId=classification["classId"],
                probability=classification["probability"],
            )

        # if there was an error, just log it and move on
        except Exception as e:
            success = False
            logger.warning(
                (
                    "Unable to create an ElasticcBrokerClassification entry."
                    f"\nalertId: {alertId}, diaObjectId: {diaObjectId}, "
                    f"brokerMessageId: {self.brokerMessageId}, "
                    f"classification: {classification}"
                    f"\nError message: {e}"
                )
            )

        return success

    def parse(self):
        """Pass. This function is required by BaseParser, but we don't use it here."""
        pass
