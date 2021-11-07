#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Parse alerts conforming to ELAsTiCC schema: lsst.v4_1.brokerClassification.avsc."""
import io
import logging
# import re

# from dateutil.parser import parse, parserinfo
from django.core.exceptions import ValidationError
import fastavro

# from stream.models import Event
from stream.parsers.base_parser import BaseParser


logger = logging.getLogger(__name__)

avsc_file = "elasticc_schema/lsst.v4_1.brokerClassification.avsc"
BROKERCLASSIFICATION_SCHEMA = fastavro.schema.load_schema(avsc_file)


class ElasticcBrokerClassificationParser(BaseParser):
    """Parse alerts conforming to ELAsTiCC schema: lsst.v4_1.brokerClassification.avsc.

    Sample alert, after casting Avro -> dict:
    {
        "alertId": 123456789,
        "classifierNames": "RealBogus_v0.1, SuperNNova_v1.3",   # comma-separated string
        "classifications": {                                    # dict with single item
            "classificationDict":                               # list of dicts
                [
                    {"10":      0.1},                           # dict with single item
                    {"20":      0.9},
                    {"11120":   0.8},
                    {"111120":  0.65},
                ]
        }
    }
    """

    def __repr__(self):
        return 'ELAsTiCC brokerClassification Parser'

    def parse(self, avro_bytes):
        """Parse Avro serialized, bytes-like object conforming to ELAsTiCC schema.

        Args:
            avro_bytes (bytes): Avro serialized, bytes-like object

        Returns True if avro_bytes was successfully parsed, else False
        """
        msg_dicts = self.avro_to_dict(avro_bytes)  # list

        if msg_dicts is None:  # unable to parse
            return False

        for msg_dict in msg_dicts:
            # dump the message contents to the Alert object
            try:
                # extract basic data
                self.alert.alertId = msg_dict["alertId"]
                self.alert.classifierNames = msg_dict["classifierNames"]

                # extract classification probabilities
                classDicts = msg_dict["classifications"]["classificationDict"]  # list
                for class_dict in classDicts:
                    for bitmask, value in class_dict.items():
                        setattr(self.alert, f"class_{bitmask}", value)

            except Exception:
                # If we get here, the record matches the schema but for some reason
                # it can't be mapped into the Alert.
                # Indicates a problem in the above code block.
                logger.warn(f"Avro record cannot be mapped into an Alert: {msg_dict}")
                raise

        return True

    def avro_to_dict(self, avro_bytes):
        """Convert Avro serialized bytes data to a dict.

        Args:
            avro_bytes (bytes): Avro serialized, bytes-like object

        Returns:
            A list of dictionaries, or None if bytes_data is not a bytes-like object.
        """
        try:
            with io.BytesIO(avro_bytes) as fin:
                dicts = [r for r in fastavro.reader(fin)]

        except TypeError as e:
            logger.warn(f"Unable to deserialize message {avro_bytes}: {e}")
            return

        # Validate against the schema. May want to turn this off in production.
        try:
            fastavro.validation.validate_many(dicts, BROKERCLASSIFICATION_SCHEMA)
        except ValidationError as e:
            msg = (
                "Message does not conform to brokerClassification schema. "
                f"Message: {dicts}. Schema: {BROKERCLASSIFICATION_SCHEMA}. Error: {e}"
            )
            logger.warn(msg)
            return

        return dicts
