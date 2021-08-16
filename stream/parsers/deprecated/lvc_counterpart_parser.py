from dateutil.parser import parse
from datetime import datetime, timezone
from gzip import decompress
import io
import logging
import re
import requests

from astropy.io import fits
from django.contrib.gis.geos import Point
from django.core.cache import cache
from gracedb_sdk import Client
import healpy as hp
import numpy as np
import voeventparse as vp

from skip.exceptions import ParseError
from skip.parsers.base_parser import BaseParser


logger = logging.getLogger(__name__)


class LVCCounterpartParser(BaseParser):
    superevent_identifier_regex = re.compile(r'S[0-9]{6}[a-z]+')
    counterpart_identifier_regex = re.compile(r'\d?\w+\s\w\d+\.\d(\+|-)\d+')
    comment_warnings_prefix = 'ranks\.php for details.'  # noqa
    comment_warnings_regex = re.compile(r'({prefix}).*$'.format(prefix=comment_warnings_prefix))

    def __init__(self, *args, **kwargs):
        self.gracedb_client = Client()

    def _get_public_superevents(self, refresh_cache=False):
        superevent_ids = cache.get('gracedb_superevents')

        if not superevent_ids or refresh_cache:
            superevent_ids = [se['superevent_id']
                              for se in self.gracedb_client.superevents.search(query='category: Production')]
            cache.set('gracedb_superevents', superevent_ids)

        return superevent_ids

    def __repr__(self):
        return 'LVC Counterpart Parser'

    def _get_confidence_regions(self, superevent):
        confidence_regions = {}

        try:
            buffer = io.BytesIO()
            buffer.write(decompress(requests.get(superevent.files.get()['bayestar.fits.gz'], stream=True).content))
            buffer.seek(0)
            hdul = fits.open(buffer, memmap=False)

            # Get the total number of healpixels in the map
            n_pixels = len(hdul[1].data)
            # Covert that to the nside parameter
            nside = hp.npix2nside(n_pixels)
            # Sort the probabilities so we can do the cumulative sum on them
            probabilities = hdul[1].data['PROB']
            probabilities.sort()
            # Reverse the list so that the largest pixels are first
            probabilities = probabilities[::-1]
            cumulative_probabilities = np.cumsum(probabilities)
            # The number of pixels in the 90 (or 50) percent range is just given by the first set of pixels that add up
            # to 0.9 (0.5)
            index_90 = np.min(np.flatnonzero(cumulative_probabilities >= 0.9))
            index_50 = np.min(np.flatnonzero(cumulative_probabilities >= 0.5))
            # Because the healpixel projection has equal area pixels, the total area is just the heal pixel area * the
            # number of heal pixels
            healpixel_area = hp.nside2pixarea(nside, degrees=True)
            confidence_regions['area_50'] = (index_50 + 1) * healpixel_area
            confidence_regions['area_90'] = (index_90 + 1) * healpixel_area
        except Exception as e:
            logger.error(f'Unable to parse bayestar.fits.gz for confidence regions: {e}')

        return confidence_regions

    # TODO: deal with events for which the last alert was a retraction
    # TODO: deal with events for which the skymap can't be parsed
    def _get_data_from_voevent(self, alert):
        voevent_data = {}

        try:
            event_trigger_number = alert['alert_identifier'].split('_')[0]
            gracedb_superevent = self.gracedb_client.superevents[event_trigger_number]
            latest_voevent = gracedb_superevent.voevents.get()[-1]
            voevent_file = gracedb_superevent.files[latest_voevent['filename']].get()
            voevent = vp.load(voevent_file)

            for param in voevent.What.Param:
                if param.attrib['name'] in ['FAR', 'Instruments']:
                    voevent_data[param.attrib['name']] = param.attrib['value']

            classification_group = voevent.What.find(".//Group[@type='Classification']")
            if classification_group is not None:  # Retractions don't have classifications
                for param in classification_group.findall('Param'):
                    voevent_data[param.attrib['name']] = param.attrib['value']

            properties_group = voevent.What.find(".//Group[@type='Properties']")
            if properties_group is not None:
                for param in properties_group.findall('Param'):
                    voevent_data[param.attrib['name']] = param.attrib['value']

            filename_parts = latest_voevent['filename'].split('.')[0].split('-')
            voevent_data['data_version'] = f'{filename_parts[-1]} {filename_parts[-2]}'

            voevent_data.update(self._get_confidence_regions(gracedb_superevent))
        except requests.exceptions.HTTPError as httpe:
            logger.error(f'Unable to parse VO Event for alert {alert["event_trig_num"]}: {httpe}')

        return voevent_data

    def parse_alert_identifier(self, alert):
        """
        Sources are of the format S123456_X1, that is, event trigger number + '_X' + source serial number
        """
        event_trigger_number = alert['event_trig_num']
        if len(self.superevent_identifier_regex.findall(event_trigger_number)) == 0:
            for superevent_id in self._get_public_superevents():
                if event_trigger_number in superevent_id:
                    event_trigger_number = superevent_id
                    break
            else:
                for superevent_id in self._get_public_superevents(refresh_cache=True):
                    if event_trigger_number in superevent_id:
                        event_trigger_number = superevent_id
                        break
                else:
                    logger.warn(f'Superevent {event_trigger_number} does not match any GraceDB event.')
        source_sernum = alert['sourse_sernum']
        return f'{event_trigger_number}_X{source_sernum}'

    def parse_coordinates(self, alert):
        raw_ra = alert['cntrpart_ra'].split(',')[0]
        raw_dec = alert['cntrpart_dec'].split(',')[0]
        ra = raw_ra.split('d', 1)[0]
        dec = raw_dec.split('d', 1)[0]
        return ra, dec

    def parse_extracted_fields(self, alert):
        extracted_fields = {}

        ci_match = self.counterpart_identifier_regex.search(alert['message']['comments'])
        extracted_fields['counterpart_identifier'] = ci_match[0].strip() if ci_match else ''

        cw_match = self.comment_warnings_regex.search(alert['message']['comments'])
        extracted_fields['comment_warnings'] = (cw_match[0][len(self.comment_warnings_prefix):].strip()
                                                if cw_match else '')

        extracted_fields.update(self._get_data_from_voevent(alert))

        return extracted_fields

    def parse_timestamp(self, alert):
        # TODO: the alert contains three different timestamps, we should determine which we want. This method
        # currently returns the event timestamp, rather than the notice or observation timestamps.
        raw_datestamp = alert['obs_date']
        raw_timestamp = alert['obs_time']
        datestamp = re.search(r'\d{2}\/\d{2}\/\d{2}', raw_datestamp)
        parsed_datestamp = parse(datestamp.group(0), yearfirst=True)
        timestamp = re.search(r'\d{2}:\d{2}:\d{2}\.\d{2}', raw_timestamp)
        parsed_timestamp = parse(timestamp.group(0))
        combined_datetime = datetime.combine(parsed_datestamp, parsed_timestamp.time())
        combined_datetime.replace(tzinfo=timezone.utc)
        return combined_datetime

    def parse_alert(self, alert):
        """
        The GNC/LVC Counterpart notices come through as a text blob. The below code parses the lines into key/value
        pairs and adds them to the JSON that will be saved as a message. A couple of notes:

        - RA/Dec are parsed differently because they come through on three lines, each line providing the coordinate
          using a different epoch.
        - An arbitrary number of comment lines are included, each one preceded by 'COMMENT:'.

        At some point we may consider modifying the fields prior to adding them to the parsed alert, and saving the
        raw message as a key/value pair of the `message` field. This could cause complications elsewhere, however.

        The below is a sample message that can be parsed by this method.

        TITLE:            GCN/LVC COUNTERPART NOTICE
        NOTICE_DATE:      Sat 13 Apr 19 02:48:49 UT
        NOTICE_TYPE:      Other
        CNTRPART_RA:      214.9576d {+14h 19m 49.8s} (J2000),
                          215.1672d {+14h 20m 40.1s} (current),
                          214.4139d {+14h 17m 39.3s} (1950)
        CNTRPART_DEC:     +31.3139d {+31d 18' 50.0"} (J2000),
                          +31.2261d {+31d 13' 33.8"} (current),
                          +31.5428d {+31d 32' 34.1"} (1950)
        CNTRPART_ERROR:   7.2 [arcsec, radius]
        EVENT_TRIG_NUM:   S190412
        EVENT_DATE:       18585 TJD;   102 DOY;   2019/04/12 (yy/mm/dd)
        EVENT_TIME:       19844.00 SOD {05:30:44.00} UT
        OBS_DATE:         18585 TJD;   102 DOY;   19/04/12
        OBS_TIME:         77607.0 SOD {21:33:27.00} UT
        OBS_DUR:          80.2 [sec]
        INTENSITY:        3.50e-12 +/- 1.80e-12 [erg/cm2/sec]
        ENERGY:           0.3-10 [keV]
        TELESCOPE:        Swift-XRT
        SOURSE_SERNUM:    3
        RANK:             4
        WARN_FLAG:        0
        SUBMITTER:        Phil_Evans
        SUN_POSTN:         21.19d {+01h 24m 45s}   +8.90d {+08d 54' 17"}
        SUN_DIST:         137.69 [deg]   Sun_angle= 11.1 [hr] (West of Sun)
        MOON_POSTN:       119.27d {+07h 57m 04s}  +21.28d {+21d 16' 50"}
        MOON_DIST:         83.90 [deg]
        MOON_ILLUM:       54 [%]
        GAL_COORDS:        50.48, 70.30 [deg] galactic lon,lat of the counterpart
        ECL_COORDS:       199.09, 42.19 [deg] ecliptic lon,lat of the counterpart
        COMMENTS:         LVC Counterpart.
        COMMENTS:         This matches a catalogued X-ray source: 1RXS J141947.5+311838
        COMMENTS:         This source has been given a rank of 4
        COMMENTS:         Ranks indicate how likely the object is to be
        COMMENTS:         the GW counterpart. Ranks go from 1-4 with
        COMMENTS:         1 being the most likely and 4 the least.
        COMMENTS:         See http://www.swift.ac.uk/ranks.php for details.
        """
        parsed_alert = {'message': {}, 'extracted_fields': {}}

        try:
            if isinstance(alert, dict):
                if 'content' in alert.keys():  # Alerts sometimes come through with nested content, sometimes without
                    alert = alert['content']
            for line in alert.splitlines():
                entry = line.split(':', 1)
                if len(entry) > 1:
                    if entry[0] == 'COMMENTS' and 'comments' in parsed_alert['message']:
                        parsed_alert['message']['comments'] += entry[1].lstrip()
                    else:
                        parsed_alert['message'][entry[0].lower()] = entry[1].strip()
                else:
                    # RA is parsed first, so append to RA if dec hasn't been parsed
                    # TODO: modify this to just keep track of the most recent key
                    if 'cntrpart_dec' not in parsed_alert['message']:
                        parsed_alert['message']['cntrpart_ra'] += ' ' + entry[0].strip()
                    else:
                        parsed_alert['message']['cntrpart_dec'] += ' ' + entry[0].strip()

            # TODO: how to make this update with new data rather than ignore duplicate alerts?
            parsed_alert['alert_identifier'] = self.parse_alert_identifier(parsed_alert['message'])
            # if Alert.objects.filter(alert_identifier=parsed_alert['alert_identifier']).exists():
            #     logger.warn(msg=f"Alert {parsed_alert['alert_identifier']} already exists.")
            #     return

            ra, dec = self.parse_coordinates(parsed_alert['message'])
            parsed_alert['coordinates'] = Point(float(ra), float(dec), srid=4035)

            timestamp = self.parse_timestamp(parsed_alert['message'])
            parsed_alert['alert_timestamp'] = timestamp

            parsed_alert['extracted_fields'] = self.parse_extracted_fields(parsed_alert)
        except (AttributeError, KeyError, ParseError) as e:
            logger.warn(msg=f'Unable to parse LVC Counterpart alert {alert}: {e}')
            return

        return parsed_alert
