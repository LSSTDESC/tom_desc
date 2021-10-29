"""
An experimental Django management command for consuming alerts from ANTARES.

Contact Nicholas Wolf for support (nic.wolf@noirlab.edu).

"""

import datetime

import astropy.time
from antares_client import StreamingClient
from django.conf import settings
from django.contrib.gis.geos import Point
from django.core.management.base import BaseCommand

from stream.models import Alert, Topic

ANTARES_CONSUMER_CONFIGURATION = settings.ANTARES_CONSUMER_CONFIGURATION


def mjd_to_datetime(mjd: float) -> datetime.datetime:
    """Converts an MJD to a timezone-aware datetime in UTC."""
    return astropy.time.Time([mjd], format="mjd")[0].to_datetime(
        timezone=astropy.time.TimezoneInfo()
    )


class Command(BaseCommand):
    """
    Stream and save alerts from ANTARES.

    This is an experimental Django management command for streaming and storing
    ANTARES alerts in the DESC-expected format.

    We are using the ANTARES client library to stream alerts which is a high
    level interface to the ANTARES output stream. As it receives messages it
    converts them into `antares_client.Locus` objects. This class has roughly
    this structure:

    class Locus:
        locus_id: str
        properties: dict
        ra: float
        dec: float
        alerts: List[Alert]

    class Alert:
        alert_id: str
        properties: dict

    This is a helpful interface for downstream users that want to conveniently
    access ANTARES output but might not be the best for the DESC use case. We
    can think about different ways of parsing (e.g. accessing the underlying
    Kafka message) or sending a different data format to DESC.

    Notes
    ---------

    * Because ANTARES uses Kafka as its message broker, it is important to be
      thoughtful about what you specify as the consumer group. By default, here,
      we mimic the behavior of the ANTARES client library which uses the system's
      hostname if no group is specified.

    """

    def handle(self, *args, **options):
        with StreamingClient(
            topics=["extragalactic_staging", "nuclear_transient_staging"],
            api_key=ANTARES_CONSUMER_CONFIGURATION["key"],
            api_secret=ANTARES_CONSUMER_CONFIGURATION["secret"],
            group=ANTARES_CONSUMER_CONFIGURATION["group"],
        ) as client:
            for topic_name, locus in client.iter():
                print("received {} on {}".format(locus, topic_name))
                topic, _ = Topic.objects.get_or_create(name=topic_name)
                alert = Alert.objects.create(
                    topic=topic,
                    identifier=locus.locus_id,
                    timestamp=mjd_to_datetime(
                        locus.properties["newest_alert_observation_time"]
                    ),
                    coordinates=Point(locus.ra, locus.dec, srid=4035),
                    parsed=True,
                )
                alert.save()
