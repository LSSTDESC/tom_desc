# Generated by Django 4.0.6 on 2022-09-07 12:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc', '0032_redo_elasticc_sourceclass_view'),
    ]

    operations = [
        migrations.AlterField(
            model_name='diaalert',
            name='alertSentTimestamp',
            field=models.DateTimeField(db_index=True, null=True),
        ),
    ]
