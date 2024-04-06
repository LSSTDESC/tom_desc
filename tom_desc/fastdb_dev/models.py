django.db import models
from django.contrib.postgres.fields import ArrayField
import django.contrib.postgres.indexes as indexes
from django.utils import timezone
from psqlextra.types import PostgresPartitioningMethod
from psqlextra.models import PostgresPartitionedModel
import uuid

# Support the Q3c Indexing scheme

class q3c_ang2ipix(models.Func):
    function = "q3c_ang2ipix"

class ProcessingVersions(models.Model):

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'processing_versions'

    version = models.TextField(db_comment="Processing version", primary_key=True)
    validity_start = models.DateTimeField(db_comment="Time when validity of this processing version starts")
    validity_end = models.DateTimeField(null=True, blank=True, db_comment="Time when validity of this processing version ends")


class HostGalaxy(models.Model):

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'host_galaxy'

    host_gal_id = models.BigIntegerField(db_comment="nearbyObj from PPDB")
    host_gal_flux = ArrayField(models.FloatField(db_comment="galaxy flux in ugrizy", null=True))
    host_gal_fluxerr = ArrayField(models.FloatField(db_comment="galaxy flux error in ugrizy", null=True))
    processing_version = models.ForeignKey(ProcessingVersions, on_delete=models.RESTRICT, null=True, related_name='hg_proc_version')
    insert_time =  models.DateTimeField(default=timezone.now)

class Snapshots(models.Model):

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'snapshots'

    name = models.TextField(db_comment='Snapshot name', primary_key=True)
    insert_time =  models.DateTimeField(default=timezone.now)


    def __str__(self):
       return self.name
   
class DiaObject(models.Model):

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'dia_object'
        indexes = [
            models.Index(fields=["dia_object"]),
            models.Index(fields=["season"]),
            models.Index(fields=["dia_object","season"]),
        ]


    dia_object = models.BigIntegerField(primary_key=True, db_comment="diaObjectId from PPDB")
    dia_object_iau_name = models.TextField(db_comment="IAU Name", null=True)
    validity_start = models.DateTimeField(default=timezone.now, db_comment="Time when validity of this diaObject starts")
    validity_end = models.DateTimeField(null=True, blank=True, db_comment="Time when validity of this diaObject ends")
    season = models.IntegerField(db_comment='Season when this object appears')
    fake_id = models.IntegerField(db_comment='ID to indicate fake SN, fake = integer, real = 0', default=0)
    ra = models.FloatField(db_comment="RA of object at time radecTai")
    ra_sigma = models.FloatField(db_comment="Uncertainty in RA of object at time radecTai")
    decl = models.FloatField(db_comment="DEC of object at time radecTai")
    decl_sigma = models.FloatField(db_comment="Uncertainty in DEC of object at time radecTai")
    ra_dec_tai =  models.FloatField(db_comment="Time at which object was at position ra/dec")
    nobs = models.IntegerField(db_comment='Number of observations of this object')
    host_gal_first = models.ForeignKey(HostGalaxy, on_delete=models.CASCADE, null=True, related_name='hgal1')
    host_gal_second = models.ForeignKey(HostGalaxy, on_delete=models.CASCADE, null=True, related_name='hgal2')
    host_gal_third = models.ForeignKey(HostGalaxy, on_delete=models.CASCADE, null=True, related_name='hgal3')
    insert_time =  models.DateTimeField(default=timezone.now)

class SnapshotTags(models.Model):

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'snapshot_tags'
        constraints = [models.UniqueConstraint(fields=['name','snapshot_name'], name='unique_ss_tag')]

    
    name =  models.TextField(db_comment='Tag name')
    snapshot_name = models.ForeignKey(Snapshots, on_delete=models.RESTRICT, db_column='snapshot_name', related_name='tag_snap_name')
    insert_time =  models.DateTimeField(default=timezone.now)

class LastUpdateTime(models.Model):

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'last_update_time'

    last_update_time =  models.DateTimeField(null=True)


class DiaSource(PostgresPartitionedModel):

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'dia_source'
        constraints = [models.UniqueConstraint(fields=['processing_version','dia_source'],name='unique_pv_dia_source')]
        indexes = [
            models.Index(fields=["dia_object"]),
            models.Index(fields=["dia_source"]),
        ]

    class PartitioningMeta:
        method = PostgresPartitioningMethod.LIST
        key = ['processing_version']

    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dia_source = models.BigIntegerField(db_comment="diaSourceId from Alert stream")
    ccd_visit_id = models.BigIntegerField(db_comment="ccdVisitId from PPDB", null=True)
    dia_object = models.ForeignKey(DiaObject, on_delete=models.RESTRICT, related_name='dia_obj_s', db_column='dia_object')
    parent_dia_source_id = models.BigIntegerField(null=True, db_comment="parentdiaSourceId from PPDB", db_column='parent_dia_source_id')
    season = models.IntegerField(db_comment='Season when this object appears - filled from DiaObject')
    fake_id = models.IntegerField(db_comment='ID to indicate fake SN, fake = integer, real = 0  - filled from DiaObject', null=True)
    mid_point_tai = models.FloatField(db_comment="midPointTai from PPDB")
    filter_name = models.TextField(db_comment="CcdVisit.filterName from PPDB")
    ra = models.FloatField(db_comment="RA of the center of this source from PPDB" )
    decl = models.FloatField(db_comment="DEC of the center of this source from PPDB" )
    ps_flux = models.FloatField(db_comment="psFlux from PPDB")
    ps_flux_err = models.FloatField(db_comment="psFluxErr from PPDB")
    snr = models.FloatField(db_comment="snr from PPDB" )
    processing_version =  models.TextField(db_comment="Local copy of Processing version key to circumvent Django")
    broker_count = models.IntegerField(db_comment="Number of brokers that alerted on this source")
    valid_flag =  models.IntegerField(db_comment='Valid data flag', default=1)
    insert_time =  models.DateTimeField(default=timezone.now)



class DiaForcedSource(PostgresPartitionedModel):

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'dia_forced_source'
        constraints = [models.UniqueConstraint(fields=['processing_version','dia_forced_source'],name='unique_pv_dia_forced_source')]
        indexes = [
            models.Index(fields=["dia_object"]),
            models.Index(fields=["dia_forced_source"]),
        ]

        
    class PartitioningMeta:
        method = PostgresPartitioningMethod.LIST
        key = ['processing_version']

    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dia_forced_source = models.BigIntegerField(db_comment="diaForcedSourceId from PPDB")
    ccd_visit_id = models.BigIntegerField(db_comment="ccdVisitId from PPDB", null=True)
    dia_object = models.ForeignKey(DiaObject, on_delete=models.RESTRICT, related_name='dia_obj_fs', db_column='dia_object')
    season = models.IntegerField(db_comment='Season when this object appears - filled from DiaObject')
    fake_id = models.IntegerField(db_comment='ID to indicate fake SN, fake = integer, real = 0  - filled from DiaObject', null=True)
    mid_point_tai = models.FloatField(db_comment="midPointTai from PPDB", null=True)
    filter_name = models.TextField(db_comment="CcdVisit.filterName from PPDB")
    ps_flux = models.FloatField(db_comment="psFlux from PPDB")
    ps_flux_err = models.FloatField(db_comment="psFluxErr from PPDB")
    processing_version =  models.TextField(db_comment="Local copy of Processing version key to circumvent Django")
    valid_flag =  models.IntegerField(db_comment='Valid data flag', default=1)
    insert_time =  models.DateTimeField(default=timezone.now)


class DStoPVtoSS(PostgresPartitionedModel):

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'ds_to_pv_to_ss'
        constraints = [models.UniqueConstraint(fields=['processing_version','snapshot_name','dia_source'],name='unique_ds_pv_ss')]

    class PartitioningMeta:
        method = PostgresPartitioningMethod.LIST
        key = ['processing_version']


    processing_version =  models.TextField(db_comment="Local copy of Processing version key to circumvent Django")
    snapshot_name = models.TextField(db_comment="Local copy of snapshot_name key to circumvent Django")
    dia_source = models.BigIntegerField(db_comment="Local copy of dia_source to circumvent Django")
    valid_flag = models.IntegerField(db_comment='Valid data flag', default=1)
    insert_time =  models.DateTimeField(default=timezone.now)
        


class DFStoPVtoSS(PostgresPartitionedModel):

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'dfs_to_pv_to_ss'
        constraints = [models.UniqueConstraint(fields=['processing_version','snapshot_name','dia_forced_source'],name='unique_dfs_pv_ss')]

    class PartitioningMeta:
        method = PostgresPartitioningMethod.LIST
        key = ['processing_version']

    processing_version =  models.TextField(db_comment="Local copy of Processing version to circumvent Django")
    snapshot_name = models.TextField(db_comment="Local copy of snapshot_name to circumvent Django")
    dia_forced_source = models.BigIntegerField(db_comment="Local copy of dia_source to circumvent Django")
    valid_flag = models.IntegerField(db_comment='Valid data flag', default=1)
    insert_time =  models.DateTimeField(default=timezone.now)
    
# Broker information

# Brokers send avro alerts (or the equivalent) with schema in:
# https://github.com/LSSTDESC/elasticc/blob/main/alert_schema/elasticc.v0_1.brokerClassification.avsc
#
# Each one of these alerts will be saved to a MongoDB
#
# The classifications array of that message will be saved as a JSON object to BrokerClassifications,
# creating new entries in BrokerClassifier as necessary.

            
class BrokerClassifier(models.Model):
    """Model for a classifier producing a broker classification."""

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'broker_classifier'
        indexes = [
            models.Index(fields=["broker_name"]),
            models.Index(fields=["broker_name", "broker_version"]),
            models.Index(fields=["broker_name", "classifier_name"]),
            models.Index(fields=["broker_name", "broker_version", "classifier_name", "classifier_params"]),
        ]


    classifier_id = models.BigAutoField(primary_key=True, db_index=True)

    broker_name = models.CharField(max_length=100)
    broker_version = models.TextField(null=True)     # state changes logically not part of the classifier
    classifier_name = models.CharField(max_length=200)
    classifier_params = models.TextField(null=True)   # change in classifier code / parameters
    
    insert_time =  models.DateTimeField(default=timezone.now)


class BrokerClassification(PostgresPartitionedModel):
    """Model for a classification from a broker."""

    class PartitioningMeta:
        method = PostgresPartitioningMethod.LIST
        key = [ 'classifier' ]
        
    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'broker_classification'
        constraints = [
            models.UniqueConstraint(
                name="unique_brokerclassification",
                fields=['classifier', 'classification_id', "alert_id"]
            ),
        ]
        indexes = [models.Index(fields=["classification_id"]),
                   models.Index(fields=["alert_id"]),
                   models.Index(fields=["classification_id","alert_id"]),
                   models.Index(fields=["dia_source"]),
                   models.Index(fields=["dia_source","alert_id","classification_id"]),]


    classification_id = models.BigAutoField(primary_key=True)
    alert_id = models.BigIntegerField(null=True)
    dia_source = models.BigIntegerField(db_comment="Local copy of dia_source to circumvent Django")
    topic_name = models.CharField(max_length=200, null=True)
    desc_ingest_timestamp = models.DateTimeField(default=timezone.now)  # auto-generated
    broker_ingest_timestamp = models.DateTimeField(null=True)
    classifier = models.BigIntegerField("Local copy of clasifier to circumvent Django")

    # Create classifications JSON object with class_id and probability aa keys
    
    classifications = models.JSONField(db_index=True, null=True)
    insert_time =  models.DateTimeField(default=timezone.now)

class DBViews(models.Model):

    class Meta:
        app_label = 'fastdb_dev'
        db_table = 'db_views'

    view_name = models.TextField(db_comment="Name for view", null=True)
    view_sql = models.TextField(db_comment="SQL for view", null=True)
    insert_time =  models.DateTimeField(default=timezone.now)

