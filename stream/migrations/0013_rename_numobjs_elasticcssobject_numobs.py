# Generated by Django 3.2.11 on 2022-01-11 19:59

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('stream', '0012_rename_discoveriysubmissiondate_elasticcssobject_discoverysubmissiondate'),
    ]

    operations = [
        migrations.RenameField(
            model_name='elasticcssobject',
            old_name='numObjs',
            new_name='numObs',
        ),
    ]