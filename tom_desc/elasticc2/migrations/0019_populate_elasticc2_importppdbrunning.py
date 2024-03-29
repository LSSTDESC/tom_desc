# Generated by Django 4.2.2 on 2023-07-05 16:36

from django.db import migrations

def populate_importppdbrunning( apps, schema_editor ):
    ImportPPDBRunning = apps.get_model( 'elasticc2', 'ImportPPDBRunning' )
    ImportPPDBRunning.objects.all().delete()
    r = ImportPPDBRunning( running=False )
    r.save()

def wipe_importppdbrunning( apps, schema_editor ):
    ImportPPDBRunning = apps.get_model( 'elasticc2', 'ImportPPDBRunning' )
    ImportPPDBRunning.objects.all().delete()


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc2', '0018_brokersourceids_importppdbrunning'),
    ]

    operations = [
        migrations.RunPython( populate_importppdbrunning, wipe_importppdbrunning )
    ]
