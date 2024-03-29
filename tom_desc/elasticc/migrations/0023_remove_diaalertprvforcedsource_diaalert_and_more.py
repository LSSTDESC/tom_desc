# Generated by Django 4.0.6 on 2022-08-08 15:03

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc', '0022_ro_users'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='diaalertprvforcedsource',
            name='diaAlert',
        ),
        migrations.RemoveField(
            model_name='diaalertprvforcedsource',
            name='diaForcedSource',
        ),
        migrations.RemoveField(
            model_name='diaalertprvsource',
            name='diaAlert',
        ),
        migrations.RemoveField(
            model_name='diaalertprvsource',
            name='diaSource',
        ),
        migrations.RemoveIndex(
            model_name='brokermessage',
            name='elasticc_br_dbMessa_59550d_idx',
        ),
        migrations.RenameField(
            model_name='brokerclassification',
            old_name='dbClassificationIndex',
            new_name='classificationId',
        ),
        migrations.RenameField(
            model_name='brokerclassifier',
            old_name='dbClassifierIndex',
            new_name='classiferId',
        ),
        migrations.RenameField(
            model_name='brokermessage',
            old_name='dbMessageIndex',
            new_name='brokerMessageId',
        ),
        migrations.RenameField(
            model_name='classificationmap',
            old_name='snana_gentype',
            new_name='gentype',
        ),
        migrations.RenameField(
            model_name='diatruth',
            old_name='true_genmag',
            new_name='genmag',
        ),
        migrations.RenameField(
            model_name='diatruth',
            old_name='true_gentype',
            new_name='gentype',
        ),
        migrations.RemoveField(
            model_name='brokerclassification',
            name='dbClassifier',
        ),
        migrations.AddField(
            model_name='brokerclassification',
            name='classifier',
            field=models.ForeignKey(db_column='classifierId', null=True, on_delete=django.db.models.deletion.CASCADE, to='elasticc.brokerclassifier'),
        ),
        migrations.AlterField(
            model_name='diaalert',
            name='diaObject',
            field=models.ForeignKey(db_column='diaObjectId', null=True, on_delete=django.db.models.deletion.CASCADE, to='elasticc.diaobject'),
        ),
        migrations.AlterField(
            model_name='diaalert',
            name='diaSource',
            field=models.ForeignKey(db_column='diaSourceId', null=True, on_delete=django.db.models.deletion.CASCADE, to='elasticc.diasource'),
        ),
        migrations.AlterField(
            model_name='diaforcedsource',
            name='diaObject',
            field=models.ForeignKey(db_column='diaObjectId', on_delete=django.db.models.deletion.CASCADE, to='elasticc.diaobject'),
        ),
        migrations.AlterField(
            model_name='diaobjecttruth',
            name='diaObject',
            field=models.ForeignKey(db_column='diaObjectId', on_delete=django.db.models.deletion.CASCADE, primary_key=True, serialize=False, to='elasticc.diaobject'),
        ),
        migrations.AlterField(
            model_name='diaobjecttruth',
            name='mjd_detect_first',
            field=models.FloatField(db_index=True),
        ),
        migrations.AlterField(
            model_name='diaobjecttruth',
            name='mjd_detect_last',
            field=models.FloatField(db_index=True),
        ),
        migrations.AlterField(
            model_name='diaobjecttruth',
            name='peakmjd',
            field=models.FloatField(db_index=True),
        ),
        migrations.AlterField(
            model_name='diaobjecttruth',
            name='zcmb',
            field=models.FloatField(db_index=True),
        ),
        migrations.AlterField(
            model_name='diaobjecttruth',
            name='zhelio',
            field=models.FloatField(db_index=True),
        ),
        migrations.AlterField(
            model_name='diasource',
            name='diaObject',
            field=models.ForeignKey(db_column='diaObjectId', null=True, on_delete=django.db.models.deletion.CASCADE, to='elasticc.diaobject'),
        ),
        migrations.DeleteModel(
            name='DiaAlertPrvForcedSource',
        ),
        migrations.DeleteModel(
            name='DiaAlertPrvSource',
        ),
    ]
