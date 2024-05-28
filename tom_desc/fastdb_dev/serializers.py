from rest_framework import serializers
from fastdb_dev.models import LastUpdateTime, ProcessingVersions, HostGalaxy, Snapshots, DiaObject, DiaSource, DiaForcedSource, SnapshotTags
from fastdb_dev.models import DStoPVtoSS, DFStoPVtoSS, BrokerClassifier, BrokerClassification

class DiaObjectSerializer(serializers.ModelSerializer):

    class Meta:
        model = DiaObject
        fields = ['dia_object','validity_start','validity_end','season','fake_id','ra','ra_sigma','decl','decl_sigma','ra_dec_tai','nobs','host_gal_first','host_gal_second','host_gal_third','insert_time']

class DiaSourceSerializer(serializers.ModelSerializer):

    class Meta:
        model = DiaSource
        fields = ['dia_source','ccd_visit_id','dia_object','parent_dia_source_id','season','fake_id','mid_point_tai','filter_name','ra','decl','ps_flux','ps_flux_err','snr','processing_version','broker_count','insert_time']

class DiaForcedSourceSerializer(serializers.ModelSerializer):

    class Meta:
        model = DiaForcedSource
        fields = ['dia_forced_source','ccd_visit_id','dia_object','season','fake_id','mid_point_tai','filter_name','ps_flux','ps_flux_err','processing_version','insert_time']

class DStoPVtoSSSerializer(serializers.ModelSerializer):

    class Meta:
        model = DStoPVtoSS
        fields = ['processing_version','snapshot_name','dia_source','insert_time']

class DFStoPVtoSSSerializer(serializers.ModelSerializer):

    class Meta:
        model = DFStoPVtoSS
        fields = ['processing_version','snapshot_name','dia_forced_source','insert_time']

class SnapshotsSerializer(serializers.ModelSerializer):

    class Meta:
        model = Snapshots
        fields = ['name','insert_time']

class SnapshotTagsSerializer(serializers.ModelSerializer):

    class Meta:
        model = SnapshotTags
        fields = ['name','snapshot_name','insert_time']

class ReturnDiaSourceDataSerializer(serializers.ModelSerializer):

    ds_pv_ss = DStoPVtoSSSerializer(many=True)
    
    class Meta:
        
        model = DiaSource
        fields = "__all__"
        #fields = ['dia_source','ccd_visit_id','dia_object','parent_dia_source_id','season','fake_id','mid_point_tai','filter_name','ra','decl','ps_flux','ps_flux_err','snr','processing_version','broker_count','insert_time','ds_pv_ss']
