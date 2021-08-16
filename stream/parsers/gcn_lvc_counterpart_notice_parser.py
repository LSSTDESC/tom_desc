from dateutil.parser import parse
from datetime import datetime, timezone
import logging
import re

from django.contrib.gis.geos import Point

from skip.models import Event
from skip.parsers.base_parser import BaseParser


logger = logging.getLogger(__name__)


class GCNLVCCounterpartNoticeParser(BaseParser):
    """
    Sample GCN/LVC Counterpart Notice:

    TITLE:            GCN/LVC COUNTERPART NOTICE
    NOTICE_DATE:      Fri 26 Apr 19 23:13:39 UT
    NOTICE_TYPE:      Other
    CNTRPART_RA:      299.8851d {+19h 59m 32.4s} (J2000),
                      300.0523d {+20h 00m 12.5s} (current),
                      299.4524d {+19h 57m 48.5s} (1950)
    CNTRPART_DEC:     +40.7310d {+40d 43' 51.6"} (J2000),
                      +40.7847d {+40d 47' 04.9"} (current),
                      +40.5932d {+40d 35' 35.4"} (1950)
    CNTRPART_ERROR:   7.6 [arcsec, radius]
    EVENT_TRIG_NUM:   S190426
    EVENT_DATE:       18599 TJD;   116 DOY;   2019/04/26 (yy/mm/dd)
    EVENT_TIME:       55315.00 SOD {15:21:55.00} UT
    OBS_DATE:         18599 TJD;   116 DOY;   19/04/26
    OBS_TIME:         73448.0 SOD {20:24:08.00} UT
    OBS_DUR:          72.7 [sec]
    INTENSITY:        1.00e-11 +/- 2.00e-12 [erg/cm2/sec]
    ENERGY:           0.3-10 [keV]
    TELESCOPE:        Swift-XRT
    SOURSE_SERNUM:    2
    RANK:             2
    WARN_FLAG:        0
    SUBMITTER:        Phil_Evans
    SUN_POSTN:         34.11d {+02h 16m 26s}  +13.66d {+13d 39' 45"}
    SUN_DIST:          84.13 [deg]   Sun_angle= 6.3 [hr] (West of Sun)
    MOON_POSTN:       309.58d {+20h 38m 19s}  -19.92d {-19d 55' 00"}
    MOON_DIST:         61.34 [deg]
    MOON_ILLUM:       50 [%]
    GAL_COORDS:        76.19,  5.74 [deg] galactic lon,lat of the counterpart
    ECL_COORDS:       317.73, 59.32 [deg] ecliptic lon,lat of the counterpart
    COMMENTS:         LVC Counterpart.
    COMMENTS:         This matches a catalogued X-ray source: 1RXH J195932.6+404351
    COMMENTS:         This source has been given a rank of 2
    COMMENTS:         Ranks indicate how likely the object is to be
    COMMENTS:         the GW counterpart. Ranks go from 1-4 with
    COMMENTS:         1 being the most likely and 4 the least.
    COMMENTS:         See http://www.swift.ac.uk/ranks.php for details.
    COMMENTS:         MAY match a known transient, will be checked manually.
    """

    def __repr__(self):
        return 'GCN/LVC Counterpart Notice Parser'

    def associate_event(self):
        events = Event.objects.filter(identifier__icontains=self.alert.parsed_message.get('event_trig_num', ''))
        event = (events.first()
                 if events.exists()
                 else Event.objects.create(identifier=self.alert.parsed_message.get('event_trig_num', '')))
        event.alert_set.add(self.alert)
        event.save()
        return event

    def is_alert_parsable(self):
        return all(
            x.lower() in self.alert.parsed_message['title'].lower() for x in ['GCN', 'LVC', 'COUNTERPART', 'NOTICE']
        )

    def parse_event_trig_num(self):
        """
        Sources are of the format S123456_X1, that is, event trigger number + '_X' + source serial number
        """
        event_trigger_number = self.alert.parsed_message['event_trig_num']
        source_sernum = self.alert.parsed_message['sourse_sernum']
        self.alert.identifier = f'{event_trigger_number}_X{source_sernum}'

    def parse_coordinates(self):
        raw_ra = self.alert.parsed_message['cntrpart_ra'].split(',')[0]
        raw_dec = self.alert.parsed_message['cntrpart_dec'].split(',')[0]
        right_ascension = raw_ra.split('d', 1)[0]
        declination = raw_dec.split('d', 1)[0]
        self.alert.coordinates = Point(float(right_ascension), float(declination), srid=4035)

    def parse_message(self):
        alert_message = self.alert.raw_message['content']
        try:
            last_entry = ''
            for line in alert_message.strip().splitlines():  # Remove leading/trailing newlines
                entry = line.split(':', 1)
                if len(entry) > 1:
                    if entry[0] == 'COMMENTS' and 'comments' in self.alert.parsed_message:
                        self.alert.parsed_message['comments'] += entry[1].lstrip()
                    else:
                        self.alert.parsed_message[entry[0].lower()] = entry[1].strip()
                else:
                    # RA is parsed first, so append to RA if dec hasn't been parsed
                    if last_entry == 'cntrpart_ra':
                        self.alert.parsed_message['cntrpart_ra'] += ' ' + entry[0].strip()
                    elif last_entry == 'cntrpart_dec':
                        self.alert.parsed_message['cntrpart_dec'] += ' ' + entry[0].strip()
                last_entry = entry[0]
        except Exception as e:
            logger.warn(f'parse_message failed for {self.alert}: {e}')

    def parse_obs_timestamp(self):
        raw_datestamp = self.alert.parsed_message['obs_date']
        raw_timestamp = self.alert.parsed_message['obs_time']
        datestamp = re.search(r'\d{2}\/\d{2}\/\d{2}', raw_datestamp)
        parsed_datestamp = parse(datestamp.group(0), yearfirst=True)
        timestamp = re.search(r'\d{2}:\d{2}:\d{2}\.\d{2}', raw_timestamp)
        parsed_timestamp = parse(timestamp.group(0))
        combined_datetime = datetime.combine(parsed_datestamp, parsed_timestamp.time(), tzinfo=timezone.utc)
        self.alert.timestamp = combined_datetime

    def parse(self):
        try:
            self.parse_message()

            if not self.is_alert_parsable():
                return False

            self.associate_event()

            self.parse_event_trig_num()

            self.parse_coordinates()

            self.parse_obs_timestamp()
        except Exception as e:
            logger.warn(f'Unable to parse alert {self.alert} with parser {self}: {e}')
            return False

        return True
