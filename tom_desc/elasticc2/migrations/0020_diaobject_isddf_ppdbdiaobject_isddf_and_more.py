# Generated by Django 4.2.3 on 2023-10-03 19:33

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc2', '0019_populate_elasticc2_importppdbrunning'),
    ]

    operations = [
        migrations.AddField(
            model_name='diaobject',
            name='isddf',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='ppdbdiaobject',
            name='isddf',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='trainingdiaobject',
            name='isddf',
            field=models.BooleanField(default=False),
        ),
    ]