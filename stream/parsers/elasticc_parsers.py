#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Parse alerts conforming to ELAsTiCC schema: lsst.v4_1.brokerClassification.avsc."""
import logging
from pathlib import Path

import fastavro

from stream.parsers import utils as parser_utils
from stream.parsers.base_parser import BaseParser


logger = logging.getLogger(__name__)

# brokerClassification schema
parent_path = Path(__file__).parent.resolve()
avsc_file = parent_path / "elasticc_schema/lsst.v4_1.brokerClassification.avsc"
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

    def parse(self):
        """Parse `self.alert.message_bytes` into the fields of `self.alert`.

        Assumes `self.alert.message_bytes` conforms to the ELAsTiCC
        brokerClassification schema.

        Returns True if `self.alert.message_bytes` was successfully parsed, else False.
        """
        msg_dicts = parser_utils.avro_to_dict(
            self.alert.message_bytes, BROKERCLASSIFICATION_SCHEMA
        )  # list

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
