# Generated by Django 4.0.6 on 2022-08-08 15:05

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc', '0023_remove_diaalertprvforcedsource_diaalert_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='brokerclassification',
            name='dbMessage',
            field=models.ForeignKey(db_column='brokerMessageId', null=True, on_delete=django.db.models.deletion.CASCADE, to='elasticc.brokermessage'),
        ),
    ]
