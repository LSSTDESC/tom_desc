import logging
import re

from dateutil.parser import parse, parserinfo

from stream.models import Event
from stream.parsers.base_parser import BaseParser


logger = logging.getLogger(__name__)


class PittGoogleBaseParser(BaseParser):
    """Base Parser class for Pitt-Google's streams."""

    def __repr__(self):
        return 'Pitt-Google Base Parser'

    def parse(self):
        try:
            self.parse_metadata()
            self.parse_alert()

        except Exception as e:
            logger.warn(f'Unable to parse alert {self.alert} with parser {self}: {e}')
            return False

        return True

    def parse_metadata(self):
        metadata_dict = self.alert.metadata
        self.alert.identifier = str(metadata_dict["message_id"])
        self.alert.timestamp = metadata_dict["kafka.timestamp"]
        # self.alert.broker_ingest_timestamp = metadata_dict[""]  # TODO
        self.alert.broker_publish_timestamp = metadata_dict["publish_time"]

    def parse_alert(self):
        alert_dict = self.alert.parsed_message
        # self.alert.identifier = f"{alert_dict['candid']}_PittGoogle"
        # self.alert.coordinates = (alert_dict["ra"], alert_dict["dec"])  # TODO
        return

