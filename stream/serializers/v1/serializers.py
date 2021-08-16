from astropy.coordinates import Angle
from astropy import units

from skip.models import Alert, Topic
from rest_framework import serializers


class AlertSerializer(serializers.ModelSerializer):
    alert_identifier = serializers.CharField(source='identifier')
    alert_timestamp = serializers.CharField(source='timestamp')
    message = serializers.JSONField(source='parsed_message')
    extracted_fields = serializers.SerializerMethodField()
    right_ascension = serializers.SerializerMethodField()
    right_ascension_sexagesimal = serializers.SerializerMethodField()
    declination = serializers.SerializerMethodField()
    declination_sexagesimal = serializers.SerializerMethodField()
    topic = serializers.SerializerMethodField()

    class Meta:
        model = Alert
        fields = [
                  'id',
                  'alert_identifier',
                  'alert_timestamp',
                  'topic',
                  'right_ascension',
                  'declination',
                  'right_ascension_sexagesimal',
                  'declination_sexagesimal',
                  'extracted_fields',
                  'message',
                  'created',
                  'modified']

    def get_extracted_fields(self, obj):
        extracted_fields = {}
        event = obj.events.all()
        if event.count() > 0:
            event_attributes_list = event.first().eventattributes_set.order_by('-sequence_number')
            if event_attributes_list.count() > 0:
                event_attributes = event_attributes_list.first()
                extracted_fields['BBH'] = event_attributes.attributes.get('prob_bbh', '')
                extracted_fields['BNS'] = event_attributes.attributes.get('prob_bns', '')
                extracted_fields['FAR'] = event_attributes.attributes.get('far', '')
                extracted_fields['NSBH'] = event_attributes.attributes.get('prob_nsbh', '')
                extracted_fields['HasNS'] = event_attributes.attributes.get('prob_ns', '')
                extracted_fields['MassGap'] = event_attributes.attributes.get('prob_massgap', '')
                extracted_fields['area_50'] = event_attributes.attributes.get('area_50', '')
                extracted_fields['area_90'] = event_attributes.attributes.get('area_90', '')
                extracted_fields['HasRemnant'] = event_attributes.attributes.get('prob_remnant', '')
                extracted_fields['Instruments'] = ''
                extracted_fields['Terrestrial'] = event_attributes.attributes.get('prob_terres', '')
                version = obj.parsed_message.get('notice_type', '') + ' ' + obj.parsed_message.get('sequence_num', '')
                extracted_fields['data_version'] = version
        return extracted_fields

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
