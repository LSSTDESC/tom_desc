# Generated by Django 4.2.16 on 2024-12-05 21:31

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('fastdb_dev', '0005_dfstopvtoss_update_time_diaforcedsource_update_time_and_more'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='dstopvtoss',
            index=models.Index(fields=['processing_version', 'snapshot_name', 'dia_source'], name='ds_to_pv_to_process_191840_idx'),
        ),
    ]