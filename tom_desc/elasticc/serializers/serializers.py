from elasticc.models import DiaObject, DiaForcedSource, DiaSource, DiaTruth, DiaAlert
from rest_framework import serializers

class DiaObjectSerializer(serializers.ModelSerializer):
    class Meta:
        model = DiaObject
        fields = '__all__'


class DiaSourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = DiaSource
        fields = '__all__'

class DiaForcedSourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = DiaForcedSource
        fields = '__all__'
        
class DiaTruthSerializer(serializers.ModelSerializer):
    class Meta:
        model = DiaTruth
        exclude = [ 'id' ]

class DiaAlertSerializer(serializers.BaseSerializer):
    def to_representation( self, alert ):
        srcserzer = DiaSourceSerializer( alert.diaSource )
        objserzer = DiaObjectSerializer( alert.diaObject )
        alert = { 'alertId': alert.alertId,
                  'diaSource': srcserzer.data,
                  'diaObject': objserzer.data,
                  'cutoutDifference': None,
                  'cutoutTemplate': None,
                  'prvDiaSources': [],
                  'prvDiaForcedSources': [],
                  'prvDiaNonDetectionLimits': [] }
        # ROB TODO : get the previous sources and previous forced sources
        # (the *Prv* models are not used, have to do it algorithmically)
        # prvsources = DiaAlertPrvSource.objects.all().filter( diaAlert_id=alert['alertId'] )
        # for prvsource in prvsources:
        #     srcserzer = DiaSourceSerializer( prvsource.diaSource )
        #     alert['prvDiaSources'].append( srcserzer.data )
        # prvsources = DiaAlertPrvForcedSource.objects.all().filter( diaAlert_id=alert['alertId'] )
        # for prvsource in prvsources:
        #     srcserzer = DiaSourceSerializer( prvsource.diaSource )
        #     alert['prvDiaForcedSources'].append( srcserzer.data )
        return alert
        
        

    
