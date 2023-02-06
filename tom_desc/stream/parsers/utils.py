#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Utility functions used by parsers."""
from importlib import import_module
import io
import logging

import fastavro
from fastavro._validate_common import ValidationError


logger = logging.getLogger(__name__)


def import_class(class_full_name):
    """Import and return the class defined by `class_full_name`.

    Args:
        class_full_name (str):
            Fully qualified name of the class to be imported and returned.
            e.g., 'stream.parsers.elasticc_parsers.ElasticcBrokerMessageParser'

    Returns:
        The imported class.
    """
    try:
        mod_name, class_name = class_full_name.rsplit('.', 1)
    except ValueError as e:
        msg = (
            f"Unable to import. {class_full_name} does not appear to be "
            f"a fully qualified class name. {e}"
        )
        raise ValueError(msg)

    try:
        mod = import_module(mod_name)
        clazz = getattr(mod, class_name)

    except (ImportError, AttributeError):
        raise ImportError(f'Unable to import class {class_full_name}.')

    return clazz


def avro_to_list_of_dicts(avro_bytes, schema):
    """Deserialize `avro_bytes` to a list of dicts, check that it conforms to `schema`.

    Args:
        avro_bytes (bytes):
            Avro serialized, bytes-like object. The schema may or may not be attached
            in the header.
        schema (dict):
            Avro schema to which the `avro_bytes` should conform.

    Returns:
        A list of dictionaries, or None if unable to deserialize.
    """
    # Deserialize avro_bytes
    # first, try assuming the schema is in the header
    try:
        with io.BytesIO(avro_bytes) as fin:
            avro_dicts = [r for r in fastavro.reader(fin)]

    # handle case where the schema is not attached in the header
    except (UnicodeDecodeError, ValueError):
        try:
            with io.BytesIO(avro_bytes) as fin:
                avro_dict = fastavro.schemaless_reader(fin, schema)

        # if we still can't deserialize, log the error and return None
        except (UnicodeDecodeError, StopIteration) as e:
            msg = (
                f"Unable to deserialize bytes {avro_bytes} using schema {schema}. \n{e}"
            )
            logger.warning(msg)
            return

        else:
            # schemaless has a single record. wrap in a list for consistency
            avro_dicts = [avro_dict]

    # handle other errors
    except TypeError as e:
        logger.warning(
            (
                "`avro_bytes` is not a bytes-like object. Unable to deserialize. "
                f"\n`avro_bytes`: {avro_bytes}"
                f"\nError message: {e}"
            )
        )
        return

    # Validate against the schema. If schemaless_reader was used, it's already validated
    else:
        try:
            fastavro.validation.validate_many(avro_dicts, schema)
        except ValidationError as e:
            msg = (
                "Message does not conform to the schema. "
                f"\nMessage: {avro_dicts}. \nSchema: {schema}. \nError: {e}"
            )
            logger.warning(msg)
            return

    return avro_dicts
