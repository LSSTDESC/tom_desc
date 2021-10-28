import logging
import re

from dateutil.parser import parse, parserinfo

from stream.models import Event
from stream.parsers.base_parser import BaseParser


logger = logging.getLogger(__name__)


class PittGoogleAlertParser(BaseParser):
    # TODO: write this

    def parse(self):
        pass
