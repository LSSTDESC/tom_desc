from importlib import import_module
import json
import logging

# from confluent_kafka import Consumer
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from hop import Stream
from hop.auth import Auth
from hop.io import StartPosition


from stream.models import Alert, Topic

logger = logging.getLogger(__name__)

HOPSKOTCH_CONSUMER_CONFIGURATION = settings.HOPSKOTCH_CONSUMER_CONFIGURATION
HOPSKOTCH_TOPICS = settings.HOPSKOTCH_TOPICS
HOPSKOTCH_CONSUMER_POLLING_TIMEOUT = settings.HOPSKOTCH_CONSUMER_POLLING_TIMEOUT
HOPSKOTCH_PARSERS = settings.HOPSKOTCH_PARSERS


def get_parser_classes(topic):
    parser_classes = []

    try:
        parsers = HOPSKOTCH_PARSERS[topic]
    except KeyError:
        logger.log(msg=f'HOPSKOTCH_PARSER not found for topic: {topic}.', level=logging.WARNING)
        return parser_classes

    for parser in parsers:
        mod_name, class_name = parser.rsplit('.', 1)
        try:
            mod = import_module(mod_name)
            clazz = getattr(mod, class_name)
        except (ImportError, AttributeError):
            raise ImportError(f'Unable to import parser {parser}')
        parser_classes.append(clazz)

    return parser_classes


class Command(BaseCommand):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.consumer = Consumer(HOPSKOTCH_CONSUMER_CONFIGURATION)

    def handle(self, *args, **options):

        # self.consumer.subscribe(HOPSKOTCH_TOPICS)

        auth = Auth(settings.HOPSKOTCH_CONSUMER_CONFIGURATION['sasl.username'], settings.HOPSKOTCH_CONSUMER_CONFIGURATION['sasl.password'])

        while True:
            # logger.info(
            #     f'Polling topics {HOPSKOTCH_TOPICS} with timeout of {HOPSKOTCH_CONSUMER_POLLING_TIMEOUT} seconds'
            # )

            with Stream(start_at=StartPosition.EARLIEST,auth=auth).open("kafka://kafka.scimma.org/gcn.circular", "r") as src:
                for kafka_message, metadata in src.read(metadata=True):

                    if kafka_message is None:
                        continue
                    # if kafka_message.error():
                    #     logger.warn(f'Error consuming message: {kafka_message.error()}')
                    #     # continue
                    #     return  # TODO: maybe don't completely stop ingesting if this happens

                    topic_name = metadata.topic
                    topic, _ = Topic.objects.get_or_create(name=topic_name)
                    alert = Alert.objects.create(topic=topic, raw_message=kafka_message.serialize())

                    for parser_class in get_parser_classes(topic.name):
                        with transaction.atomic():
                            # Get the parser class, instantiate it, parse the alert, and save it
                            parser = parser_class(alert)
                            alert.parsed = parser.parse()
                            if alert.parsed is True:
                                alert.save()
                                break

                    # print(kafka_message, metadata.topic)


            # kafka_message = self.consumer.poll(HOPSKOTCH_CONSUMER_POLLING_TIMEOUT)
            # if kafka_message is None:
            #     continue
            # if kafka_message.error():
            #     logger.warn(f'Error consuming message: {kafka_message.error()}')
            #     # continue
            #     return  # TODO: maybe don't completely stop ingesting if this happens

            # topic_name = kafka_message.topic()
            # topic, _ = Topic.objects.get_or_create(name=topic_name)

            # decoded_message = kafka_message.value().decode('utf-8')
            # message = json.loads(decoded_message)

            # # For whatever reason, TNS messages needs to be serialized to JSON twice. This should probably be handled
            # # elsewhere/differently
            # if topic.name == 'tns':
            #     message = json.loads(message)

            # logger.info(f'Processing alert: {message}')

            # alert = Alert.objects.create(topic=topic, raw_message=message)

            # for parser_class in get_parser_classes(topic.name):
            #     with transaction.atomic():
            #         # Get the parser class, instantiate it, parse the alert, and save it
            #         parser = parser_class(alert)
            #         alert.parsed = parser.parse()
            #         if alert.parsed is True:
            #             alert.save()
            #             break

            # if alert.parsed is True:
            #     logger.info(f'saved alert {alert}')
            # else:
            #     logger.warn(f'Unable to parse alert {alert}: {message}')

        # self.consumer.close()
