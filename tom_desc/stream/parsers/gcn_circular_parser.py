import logging
import re

from dateutil.parser import parse, parserinfo

from stream.models import Event
from stream.parsers.base_parser import BaseParser


logger = logging.getLogger(__name__)


class GCNCircularParser(BaseParser):
    """
    Sample ``gcn-circular`` alert:

    {
       "header":{
          "title":"GCN CIRCULAR",
          "number":"28609",
          "subject":"IceCube-201007A: No candidate counterparts from the Zwicky Transient Facility",
          "date":"20/10/13 14:05:53 GMT",
          "from":"Simeon Reusch at DESY  <simeon.reusch@desy.de>"
       },
       "body":"Simeon Reusch (DESY), Sven Weimann (Ruhr University Bochum), Robert Stein (DESY) and Anna Franckowiak
               (DESY/Ruhr University Bochum) report,\n\nOn behalf of the Zwicky Transient Facility (ZTF) and Global
               Relay of Observatories Watching Transients Happen (GROWTH) collaborations:\n\nWe serendipitously observed
               the localization region of the neutrino event IC-201007A (Santander et al. GCN 28575) with the Palomar
               48-inch telescope, equipped with the 47 square degree ZTF camera (Bellm et al. 2019, Graham et al. 2019),
               followed by dedicated Target-of-Opportunity observations. We observed in the g- and r-band beginning at
               2020-10-08 02:52:02 UTC, approximately 4.8 hours after event time.  We covered 0.5 sq deg at least twice,
               corresponding to 87.5% of the reported localization region. This estimate accounts for chip gaps. Each
               serendipitous exposure was 30s with a typical depth of 20.5 mag, while ToO exposures were 300s with a
               typical depth of 21.0.\n\nThe images were processed in real-time through the ZTF reduction and image
               subtraction pipelines at IPAC to search for potential counterparts (Masci et al. 2019). AMPEL (Nordin et
               al. 2019; Stein et al. 2020) was used to search the alerts database for candidates. We reject stellar
               sources (Tachibana and Miller 2018) and moving objects, and apply machine learning algorithms (Mahabal et
               al. 2019). We looked for high-significance transient candidates with our pipeline, lying within the 90.0%
               localization of the skymap.\n\nNo counterpart candidates were detected.\n\nZTF and GROWTH are worldwide
               collaborations comprising Caltech, USA; IPAC, USA, WIS, Israel; OKC, Sweden; JSI/UMd, USA; U Washington,
               USA; DESY, Germany; MOST, Taiwan; UW Milwaukee, USA; LANL USA; Tokyo Tech, Japan; IITB, India; IIA,
               India; LJMU, UK; TTU, USA; SDSU, USA and USyd, Australia.\nZTF acknowledges the generous support of the
               NSF under AST MSIP Grant No 1440341.\nGROWTH acknowledges generous support of the NSF under PIRE Grant No
               1545949.\nAlert distribution service provided by DIRAC@UW (Patterson et al. 2019).\nAlert database
               searches are done by AMPEL (Nordin et al. 2019).\nAlert filtering is performed with the AMPEL Follow-up
               Pipeline (Stein et al. 2020).\n\n"
    }
    """

    def __repr__(self):
        return 'GCN Circular Parser'

    def associate_event(self):
        superevent_regex = re.compile(r'S\d{6}[a-z]?')  # matches S######??, where ?? is any number of lowercase alphas
        matches = superevent_regex.findall(self.alert.parsed_message['subject'])
        if len(matches) > 0:
            events = Event.objects.filter(identifier__icontains=matches[0])
            event = events.first() if events.exists() else Event.objects.create(identifier=matches[0])
            event.alert_set.add(self.alert)
            event.save()
            return event

    def is_alert_parsable(self):  # TODO: this should be a common interface method with a generic name
        return all(x.lower() in self.alert.parsed_message['title'].lower() for x in ['GCN', 'CIRCULAR'])

    def parse_date(self):
        self.alert.timestamp = parse(self.alert.parsed_message['date'], parserinfo=parserinfo(yearfirst=True))

    def parse_message(self):
        alert_message = self.alert.raw_message['content']
        try:
            self.alert.parsed_message['body'] = alert_message['body']
            alert_header = alert_message['header']
            for key, value in alert_header.items():
                self.alert.parsed_message[key.lower()] = value.strip()
        except Exception as e:
            logger.warn(f'parse_message failed for {self.alert}: {e}')

    def parse_number(self):
        self.alert.identifier = self.alert.parsed_message['number']

    def parse(self):
        try:
            self.parse_message()

            if not self.is_alert_parsable():
                return False

            self.associate_event()

            self.parse_date()

            self.parse_number()

        except Exception as e:
            logger.warn(f'Unable to parse alert {self.alert} with parser {self}: {e}')
            return False

        return True
