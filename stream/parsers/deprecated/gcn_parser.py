import logging
from dateutil.parser import parse

from django.contrib.gis.geos import Point

from skip.exceptions import ParseError
from skip.parsers.base_parser import BaseParser


logger = logging.getLogger(__name__)


class GCNParser(BaseParser):

    def __repr__(self):
        return 'GCN Parser'

    def parse_coordinates(self, alert):
        # A Position2D JSON object comes through like so:
        #   "Position2D":{
        #     "unit":"deg",
        #     "Name1":"RA",
        #     "Name2":"Dec",
        #     "Value2":{
        #        "C1":"285.4246",
        #        "C2":"5.1321"
        #     },
        #     "Error2Radius":"0.0000"
        #   }
        #
        # Though the VOEvent specification implies that RA will always be in the `C1` field and Dec will always
        # be in the `C2` field, it offers no guarantee of this. However, we are making the assumption that it is
        # consistent.
        coordinates = {}

        try:
            coordinates = alert['WhereWhen']['ObsDataLocation']['ObservationLocation']['AstroCoords']['Position2D']
            ra = coordinates['Value2']['C1']
            dec = coordinates['Value2']['C2']
            return ra, dec
        except (AttributeError, KeyError):
            # TODO: Alerts of role `utility` do not have coordinates--should be explored further rather than
            # shunted off to the DefaultParser
            logger.log(msg=f'Unable to parse coordinates: {coordinates}', level=logging.WARN)
            raise ParseError('Unable to parse coordinates')

    def parse_alert(self, alert):
        parsed_alert = {}

        try:
            parsed_alert['role'] = alert['role']
            parsed_alert['alert_identifier'] = alert['ivorn']
            parsed_alert['alert_timestamp'] = parse(alert['Who']['Date'])
            ra, dec = self.parse_coordinates(alert)
            parsed_alert['coordinates'] = Point(float(ra), float(dec), srid=4035)
        except (AttributeError, KeyError, ParseError) as e:
            logger.log(msg=f'Unable to parse GCN alert: {e}', level=logging.WARN)
            return

        parsed_alert['message'] = alert

        return parsed_alert


# TODO: This breaks in the default parser, so make sure it doesn't
