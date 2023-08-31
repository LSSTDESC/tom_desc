from elasticc2.models import PPDBDiaObject, PPDBDiaSource, PPDBDiaForcedSource
from rest_framework import serializers

class PPDBDiaObjectSerializer( serializers.ModelSerializer ):
    class Meta:
        model = PPDBDiaObject
        fields = '__all__'

class PPDBDiaSourceSerializer( serializers.ModelSerializer ):
    class Meta:
        model = PPDBDiaSource
        fields = '__all__'

class PPDBDiaForcedSourceSerializer( serializers.ModelSerializer ):
    class Meta:
        model = PPDBDiaForcedSource
        fields = '__all__'
        
