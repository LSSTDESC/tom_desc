# Generated by Django 4.2 on 2023-04-25 15:24

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc2', '0010_column_cleanup'),
    ]

    operations = [
        migrations.RenameField(
            model_name='brokerclassification',
            old_name='dbMessage',
            new_name='dbmessage',
        ),
    ]
