# Generated by Django 4.2.7 on 2024-01-23 20:29

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc2', '0023_diaobjectclassification_diaobjectinfo_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='diaobjectinfo',
            name='diaobject',
            field=models.ForeignKey(db_column='diaobject_id', on_delete=django.db.models.deletion.CASCADE, to='elasticc2.diaobject'),
        ),
    ]