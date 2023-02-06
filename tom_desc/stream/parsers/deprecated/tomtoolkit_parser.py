from datetime import datetime
import logging

from django.contrib.gis.geos import Point

from skip.exceptions import ParseError
from skip.parsers.base_parser import BaseParser


logger = logging.getLogger(__name__)


class TOMToolkitParser(BaseParser):

    def __repr__(self):
        return 'TOM Toolkit Parser'

    def parse_coordinates(self, alert):
        # The TNS message contains sexagesimal RA/Dec in fields 'ra' and 'dec', and degree values in fields 'radeg'
        # and 'decdeg'.
        try:
            ra = alert['ra']
            dec = alert['dec']
            return ra, dec
        except (AttributeError, KeyError):
            # TODO: Alerts of role `utility` appear to have a different format--should be explored further rather than
            # shunted off to the DefaultParser
            raise ParseError('Unable to parse coordinates')

    def parse_alert(self, alert):
        parsed_alert = {}

        try:
            # parsed_alert['alert_identifier'] = ''
            parsed_alert['alert_timestamp'] = datetime.now()
            ra, dec = self.parse_coordinates(alert)
            parsed_alert['coordinates'] = Point(float(ra), float(dec), srid=4035)
        except (AttributeError, KeyError, ParseError) as e:
            logger.log(msg=f'Unable to parse TOM Toolkit alert: {e}', level=logging.WARN)
            return

        parsed_alert['message'] = alert

        return parsed_alert
