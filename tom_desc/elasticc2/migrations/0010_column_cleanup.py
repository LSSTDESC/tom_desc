# Generated by Django 4.2 on 2023-04-18 19:36

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc2', '0009_delete_diatruth_remove_diaobject_hostgal2_zphot_p50_and_more'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='diaforcedsource',
            name='ccdvisitid',
        ),
        migrations.RemoveField(
            model_name='diaforcedsource',
            name='totflux',
        ),
        migrations.RemoveField(
            model_name='diaforcedsource',
            name='totfluxerr',
        ),
        migrations.RemoveField(
            model_name='diasource',
            name='ccdvisitid',
        ),
        migrations.RemoveField(
            model_name='diasource',
            name='nobs',
        ),
        migrations.RemoveField(
            model_name='diasource',
            name='parent_diasource_id',
        ),
        migrations.RemoveField(
            model_name='ppdbdiaforcedsource',
            name='ccdvisitid',
        ),
        migrations.RemoveField(
            model_name='ppdbdiaforcedsource',
            name='totflux',
        ),
        migrations.RemoveField(
            model_name='ppdbdiaforcedsource',
            name='totfluxerr',
        ),
        migrations.RemoveField(
            model_name='ppdbdiasource',
            name='ccdvisitid',
        ),
        migrations.RemoveField(
            model_name='ppdbdiasource',
            name='nobs',
        ),
        migrations.RemoveField(
            model_name='ppdbdiasource',
            name='parent_ppdbdiasource_id',
        ),
    ]
