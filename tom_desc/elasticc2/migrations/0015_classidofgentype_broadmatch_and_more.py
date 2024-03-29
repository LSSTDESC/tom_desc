# Generated by Django 4.2.2 on 2023-06-08 16:59

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc2', '0014_remove_diaobjecttruth_galnmatch'),
    ]

    operations = [
        migrations.AddField(
            model_name='classidofgentype',
            name='broadmatch',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='classidofgentype',
            name='generalmatch',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='classidofgentype',
            name='categorymatch',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='classidofgentype',
            name='exactmatch',
            field=models.BooleanField(default=False),
        ),
    ]
