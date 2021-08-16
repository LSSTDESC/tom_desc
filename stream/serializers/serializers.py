from astropy.coordinates import Angle
from astropy import units

from stream.models import Alert, Event, EventAttributes, Target, Topic
from rest_framework import serializers


class TargetSerializer(serializers.ModelSerializer):
    class Meta:
        model = Target
        fields = ['name', 'right_ascension', 'declination', 'created', 'modified']


class AlertSerializer(serializers.ModelSerializer):
    right_ascension = serializers.SerializerMethodField()
    right_ascension_sexagesimal = serializers.SerializerMethodField()
    declination = serializers.SerializerMethodField()
    declination_sexagesimal = serializers.SerializerMethodField()
    topic = serializers.SerializerMethodField()

    class Meta:
        model = Alert
        fields = ['id',
                  'identifier',
                  'timestamp',
                  'topic',
                  'right_ascension',
                  'declination',
                  'right_ascension_sexagesimal',
                  'declination_sexagesimal',
                  'parsed_message',
                  'raw_message',
                  'created',
                  'modified']

    def get_right_ascension(self, obj):
        if obj.coordinates:
            return obj.coordinates.x

    def get_declination(self, obj):
        if obj.coordinates:
            return obj.coordinates.y

    def get_right_ascension_sexagesimal(self, obj):
        if obj.coordinates:
            a = Angle(obj.coordinates.x, unit=units.degree)
            return a.to_string(unit=units.hour, sep=':')

    def get_declination_sexagesimal(self, obj):
        if obj.coordinates:
            a = Angle(obj.coordinates.y, unit=units.degree)
            return a.to_string(unit=units.degree, sep=':')

    def get_topic(self, obj):
        return Topic.objects.get(pk=obj.topic.id).name


class TopicSerializer(serializers.ModelSerializer):
    class Meta:
        model = Topic
        fields = ['id', 'name']


class EventAttributesSerializer(serializers.ModelSerializer):
    class Meta:
        model = EventAttributes
        fields = [
            'tag',
            'sequence_number',
            'attributes'
        ]


class EventSerializer(serializers.HyperlinkedModelSerializer):
    event_attributes = serializers.SerializerMethodField()
    event_detail = serializers.HyperlinkedIdentityField(view_name='stream:event-detail', read_only=True)

    class Meta:
        model = Event
        fields = [
            'id',
            'identifier',
            'event_detail',
            'event_attributes'
        ]

    def get_event_attributes(self, instance):
        event_attributes = instance.eventattributes_set.all().order_by('-sequence_number')
        return EventAttributesSerializer(event_attributes, many=True).data


class EventDetailSerializer(EventSerializer):
    alerts = serializers.SerializerMethodField()

    class Meta:
        model = Event
        fields = [
            'id',
            'identifier',
            'event_attributes',
            'alerts'
        ]

    def get_alerts(self, instance):
        alerts = instance.alert_set.all().order_by('-timestamp')
        return AlertSerializer(alerts, many=True).data
