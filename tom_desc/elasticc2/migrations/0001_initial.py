# Generated by Django 4.1.7 on 2023-03-23 15:30

from django.db import migrations, models
import django.db.models.deletion
import elasticc.models
import psqlextra.backend.migrations.operations.add_default_partition
import psqlextra.backend.migrations.operations.create_partitioned_model
import psqlextra.manager.manager
import psqlextra.models.partitioned
import psqlextra.types


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('tom_targets', '0019_auto_20210811_0018'),
    ]

    operations = [
        psqlextra.backend.migrations.operations.create_partitioned_model.PostgresCreatePartitionedModel(
            name='BrokerClassification',
            fields=[
                ('classificationId', models.BigAutoField(primary_key=True, serialize=False)),
                ('classifierId', models.BigIntegerField()),
                ('classId', models.IntegerField(db_index=True)),
                ('probability', models.FloatField()),
                ('modified', models.DateTimeField(auto_now=True)),
            ],
            partitioning_options={
                'method': psqlextra.types.PostgresPartitioningMethod['LIST'],
                'key': ['classifierId'],
            },
            bases=(psqlextra.models.partitioned.PostgresPartitionedModel,),
            managers=[
                ('objects', psqlextra.manager.manager.PostgresManager()),
            ],
        ),
        psqlextra.backend.migrations.operations.add_default_partition.PostgresAddDefaultPartition(
            model_name='BrokerClassification',
            name='default',
        ),
        migrations.CreateModel(
            name='BrokerClassifier',
            fields=[
                ('classifierId', models.BigAutoField(db_index=True, primary_key=True, serialize=False)),
                ('brokerName', models.CharField(max_length=100)),
                ('brokerVersion', models.TextField(null=True)),
                ('classifierName', models.CharField(max_length=200)),
                ('classifierParams', models.TextField(null=True)),
                ('modified', models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.CreateModel(
            name='BrokerMessage',
            fields=[
                ('brokerMessageId', models.BigAutoField(primary_key=True, serialize=False)),
                ('streamMessageId', models.BigIntegerField(null=True)),
                ('topicName', models.CharField(max_length=200, null=True)),
                ('alertId', models.BigIntegerField()),
                ('diaSourceId', models.BigIntegerField()),
                ('msgHdrTimestamp', models.DateTimeField(null=True)),
                ('descIngestTimestamp', models.DateTimeField(auto_now_add=True)),
                ('elasticcPublishTimestamp', models.DateTimeField(null=True)),
                ('brokerIngestTimestamp', models.DateTimeField(null=True)),
                ('modified', models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.CreateModel(
            name='ClassIdOfGentype',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('gentype', models.IntegerField(db_index=True)),
                ('classId', models.IntegerField(db_index=True)),
                ('exactmatch', models.BooleanField()),
                ('categorymatch', models.BooleanField()),
                ('description', models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name='DiaAlert',
            fields=[
                ('alertId', models.BigIntegerField(db_index=True, primary_key=True, serialize=False, unique=True)),
                ('alertSentTimestamp', models.DateTimeField(db_index=True, null=True)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='DiaForcedSource',
            fields=[
                ('diaForcedSourceId', models.BigIntegerField(db_index=True, primary_key=True, serialize=False, unique=True)),
                ('ccdVisitId', models.BigIntegerField()),
                ('midPointTai', models.FloatField(db_index=True)),
                ('filterName', models.TextField()),
                ('psFlux', models.FloatField()),
                ('psFluxErr', models.FloatField()),
                ('totFlux', models.FloatField()),
                ('totFluxErr', models.FloatField()),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='DiaObject',
            fields=[
                ('diaObjectId', models.BigIntegerField(db_index=True, primary_key=True, serialize=False, unique=True)),
                ('simVersion', models.TextField(null=True)),
                ('ra', models.FloatField()),
                ('decl', models.FloatField()),
                ('mwebv', models.FloatField(null=True)),
                ('mwebv_err', models.FloatField(null=True)),
                ('z_final', models.FloatField(null=True)),
                ('z_final_err', models.FloatField(null=True)),
                ('hostgal_ellipticity', models.FloatField(null=True)),
                ('hostgal_sqradius', models.FloatField(null=True)),
                ('hostgal_zspec', models.FloatField(null=True)),
                ('hostgal_zspec_err', models.FloatField(null=True)),
                ('hostgal_zphot', models.FloatField(null=True)),
                ('hostgal_zphot_err', models.FloatField(null=True)),
                ('hostgal_zphot_q000', models.FloatField(null=True)),
                ('hostgal_zphot_q010', models.FloatField(null=True)),
                ('hostgal_zphot_q020', models.FloatField(null=True)),
                ('hostgal_zphot_q030', models.FloatField(null=True)),
                ('hostgal_zphot_q040', models.FloatField(null=True)),
                ('hostgal_zphot_q050', models.FloatField(null=True)),
                ('hostgal_zphot_q060', models.FloatField(null=True)),
                ('hostgal_zphot_q070', models.FloatField(null=True)),
                ('hostgal_zphot_q080', models.FloatField(null=True)),
                ('hostgal_zphot_q090', models.FloatField(null=True)),
                ('hostgal_zphot_q100', models.FloatField(null=True)),
                ('hostgal_zphot_p50', models.FloatField(null=True)),
                ('hostgal_mag_u', models.FloatField(null=True)),
                ('hostgal_mag_g', models.FloatField(null=True)),
                ('hostgal_mag_r', models.FloatField(null=True)),
                ('hostgal_mag_i', models.FloatField(null=True)),
                ('hostgal_mag_z', models.FloatField(null=True)),
                ('hostgal_mag_Y', models.FloatField(null=True)),
                ('hostgal_ra', models.FloatField(null=True)),
                ('hostgal_dec', models.FloatField(null=True)),
                ('hostgal_snsep', models.FloatField(null=True)),
                ('hostgal_magerr_u', models.FloatField(null=True)),
                ('hostgal_magerr_g', models.FloatField(null=True)),
                ('hostgal_magerr_r', models.FloatField(null=True)),
                ('hostgal_magerr_i', models.FloatField(null=True)),
                ('hostgal_magerr_z', models.FloatField(null=True)),
                ('hostgal_magerr_Y', models.FloatField(null=True)),
                ('hostgal2_ellipticity', models.FloatField(null=True)),
                ('hostgal2_sqradius', models.FloatField(null=True)),
                ('hostgal2_zspec', models.FloatField(null=True)),
                ('hostgal2_zspec_err', models.FloatField(null=True)),
                ('hostgal2_zphot', models.FloatField(null=True)),
                ('hostgal2_zphot_err', models.FloatField(null=True)),
                ('hostgal2_zphot_q000', models.FloatField(null=True)),
                ('hostgal2_zphot_q010', models.FloatField(null=True)),
                ('hostgal2_zphot_q020', models.FloatField(null=True)),
                ('hostgal2_zphot_q030', models.FloatField(null=True)),
                ('hostgal2_zphot_q040', models.FloatField(null=True)),
                ('hostgal2_zphot_q050', models.FloatField(null=True)),
                ('hostgal2_zphot_q060', models.FloatField(null=True)),
                ('hostgal2_zphot_q070', models.FloatField(null=True)),
                ('hostgal2_zphot_q080', models.FloatField(null=True)),
                ('hostgal2_zphot_q090', models.FloatField(null=True)),
                ('hostgal2_zphot_q100', models.FloatField(null=True)),
                ('hostgal2_zphot_p50', models.FloatField(null=True)),
                ('hostgal2_mag_u', models.FloatField(null=True)),
                ('hostgal2_mag_g', models.FloatField(null=True)),
                ('hostgal2_mag_r', models.FloatField(null=True)),
                ('hostgal2_mag_i', models.FloatField(null=True)),
                ('hostgal2_mag_z', models.FloatField(null=True)),
                ('hostgal2_mag_Y', models.FloatField(null=True)),
                ('hostgal2_ra', models.FloatField(null=True)),
                ('hostgal2_dec', models.FloatField(null=True)),
                ('hostgal2_snsep', models.FloatField(null=True)),
                ('hostgal2_magerr_u', models.FloatField(null=True)),
                ('hostgal2_magerr_g', models.FloatField(null=True)),
                ('hostgal2_magerr_r', models.FloatField(null=True)),
                ('hostgal2_magerr_i', models.FloatField(null=True)),
                ('hostgal2_magerr_z', models.FloatField(null=True)),
                ('hostgal2_magerr_Y', models.FloatField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='DiaTruth',
            fields=[
                ('diaSourceId', models.BigIntegerField(primary_key=True, serialize=False)),
                ('diaObjectId', models.BigIntegerField(db_index=True, null=True)),
                ('mjd', models.FloatField(null=True)),
                ('detect', models.BooleanField(null=True)),
                ('gentype', models.IntegerField(null=True)),
                ('genmag', models.FloatField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='GentypeOfClassId',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('classId', models.IntegerField(db_index=True)),
                ('gentype', models.IntegerField(db_index=True, null=True, unique=True)),
                ('description', models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name='DiaObjectTruth',
            fields=[
                ('diaObject', models.OneToOneField(db_column='diaObjectId', on_delete=django.db.models.deletion.CASCADE, primary_key=True, serialize=False, to='elasticc2.diaobject')),
                ('libid', models.IntegerField()),
                ('sim_searcheff_mask', models.IntegerField()),
                ('gentype', models.IntegerField(db_index=True)),
                ('sim_template_index', models.IntegerField(db_index=True)),
                ('zcmb', models.FloatField(db_index=True)),
                ('zhelio', models.FloatField(db_index=True)),
                ('zcmb_smear', models.FloatField()),
                ('ra', models.FloatField()),
                ('dec', models.FloatField()),
                ('mwebv', models.FloatField()),
                ('galnmatch', models.IntegerField()),
                ('galid', models.BigIntegerField(null=True)),
                ('galzphot', models.FloatField(null=True)),
                ('galzphoterr', models.FloatField(null=True)),
                ('galsnsep', models.FloatField(null=True)),
                ('galsnddlr', models.FloatField(null=True)),
                ('rv', models.FloatField()),
                ('av', models.FloatField()),
                ('mu', models.FloatField()),
                ('lensdmu', models.FloatField()),
                ('peakmjd', models.FloatField(db_index=True)),
                ('mjd_detect_first', models.FloatField(db_index=True)),
                ('mjd_detect_last', models.FloatField(db_index=True)),
                ('dtseason_peak', models.FloatField()),
                ('peakmag_u', models.FloatField()),
                ('peakmag_g', models.FloatField()),
                ('peakmag_r', models.FloatField()),
                ('peakmag_i', models.FloatField()),
                ('peakmag_z', models.FloatField()),
                ('peakmag_Y', models.FloatField()),
                ('snrmax', models.FloatField()),
                ('snrmax2', models.FloatField()),
                ('snrmax3', models.FloatField()),
                ('nobs', models.IntegerField()),
                ('nobs_saturate', models.IntegerField()),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='DiaSource',
            fields=[
                ('diaSourceId', models.BigIntegerField(db_index=True, primary_key=True, serialize=False, unique=True)),
                ('ccdVisitId', models.BigIntegerField()),
                ('parentDiaSourceId', models.BigIntegerField(null=True)),
                ('midPointTai', models.FloatField(db_index=True)),
                ('filterName', models.TextField()),
                ('ra', models.FloatField()),
                ('decl', models.FloatField()),
                ('psFlux', models.FloatField()),
                ('psFluxErr', models.FloatField()),
                ('snr', models.FloatField()),
                ('nobs', models.FloatField(null=True)),
                ('diaObject', models.ForeignKey(db_column='diaObjectId', null=True, on_delete=django.db.models.deletion.CASCADE, to='elasticc2.diaobject')),
            ],
        ),
        migrations.CreateModel(
            name='DiaObjectOfTarget',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('diaObject', models.ForeignKey(db_column='diaObjectId', on_delete=django.db.models.deletion.CASCADE, to='elasticc2.diaobject')),
                ('tomtarget', models.ForeignKey(db_column='tomtarget_id', on_delete=django.db.models.deletion.CASCADE, to='tom_targets.target')),
            ],
        ),
        migrations.AddIndex(
            model_name='diaobject',
            index=elasticc.models.LongNameBTreeIndex(elasticc.models.q3c_ang2ipix('ra', 'decl'), name='idx_elasticc2_diaobject_q3c'),
        ),
        migrations.AddField(
            model_name='diaforcedsource',
            name='diaObject',
            field=models.ForeignKey(db_column='diaObjectId', on_delete=django.db.models.deletion.CASCADE, to='elasticc2.diaobject'),
        ),
        migrations.AddField(
            model_name='diaalert',
            name='diaObject',
            field=models.ForeignKey(db_column='diaObjectId', null=True, on_delete=django.db.models.deletion.CASCADE, to='elasticc2.diaobject'),
        ),
        migrations.AddField(
            model_name='diaalert',
            name='diaSource',
            field=models.ForeignKey(db_column='diaSourceId', null=True, on_delete=django.db.models.deletion.CASCADE, to='elasticc2.diasource'),
        ),
        migrations.AddIndex(
            model_name='brokermessage',
            index=models.Index(fields=['topicName', 'streamMessageId'], name='elasticc2_b_topicNa_1b0625_idx'),
        ),
        migrations.AddIndex(
            model_name='brokermessage',
            index=models.Index(fields=['alertId'], name='elasticc2_b_alertId_547cdf_idx'),
        ),
        migrations.AddIndex(
            model_name='brokermessage',
            index=models.Index(fields=['diaSourceId'], name='elasticc2_b_diaSour_d86ee7_idx'),
        ),
        migrations.AddIndex(
            model_name='brokerclassifier',
            index=models.Index(fields=['brokerName'], name='elasticc2_b_brokerN_f8074f_idx'),
        ),
        migrations.AddIndex(
            model_name='brokerclassifier',
            index=models.Index(fields=['brokerName', 'brokerVersion'], name='elasticc2_b_brokerN_eb327f_idx'),
        ),
        migrations.AddIndex(
            model_name='brokerclassifier',
            index=models.Index(fields=['brokerName', 'classifierName'], name='elasticc2_b_brokerN_57d6c5_idx'),
        ),
        migrations.AddIndex(
            model_name='brokerclassifier',
            index=models.Index(fields=['brokerName', 'brokerVersion', 'classifierName', 'classifierParams'], name='elasticc2_b_brokerN_bfc520_idx'),
        ),
        migrations.AddField(
            model_name='brokerclassification',
            name='dbMessage',
            field=models.ForeignKey(db_column='brokerMessageId', null=True, on_delete=django.db.models.deletion.CASCADE, to='elasticc2.brokermessage'),
        ),
        migrations.AddIndex(
            model_name='diasource',
            index=elasticc.models.LongNameBTreeIndex(elasticc.models.q3c_ang2ipix('ra', 'decl'), name='idx_elasticc2_diasource_q3c'),
        ),
        migrations.AddConstraint(
            model_name='brokerclassification',
            constraint=models.UniqueConstraint(fields=('classifierId', 'classificationId'), name='unique_constarint_elasticc2_brokerclassification_partitionkey'),
        ),
    ]
