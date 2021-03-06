# Generated by Django 3.2.11 on 2022-06-06 16:02

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc', '0009_rename_hostgal_zpec_err_diaobject_hostgal_zspec_err'),
    ]

    operations = [
        migrations.CreateModel(
            name='DiaObjectTruth',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('libid', models.IntegerField()),
                ('sim_searcheff_mask', models.IntegerField()),
                ('gentype', models.IntegerField()),
                ('sim_template_index', models.IntegerField()),
                ('zcmb', models.FloatField()),
                ('zhelio', models.FloatField()),
                ('zcmb_smear', models.FloatField()),
                ('ra', models.FloatField()),
                ('dec', models.FloatField()),
                ('mwebv', models.FloatField()),
                ('galid', models.BigIntegerField()),
                ('galzphot', models.FloatField()),
                ('galzphoterr', models.FloatField()),
                ('galsnep', models.FloatField()),
                ('galsnddlr', models.FloatField()),
                ('rv', models.FloatField()),
                ('av', models.FloatField()),
                ('mu', models.FloatField()),
                ('lensedmu', models.FloatField()),
                ('peakmjd', models.FloatField()),
                ('mjd_detect_first', models.FloatField()),
                ('mjd_detect_last', models.FloatField()),
                ('dtseason_peak', models.FloatField()),
                ('peakmag_u', models.FloatField()),
                ('peakmag_g', models.FloatField()),
                ('peakmag_r', models.FloatField()),
                ('peakmag_i', models.FloatField()),
                ('peakmag_z', models.FloatField()),
                ('peakmag_Y', models.FloatField()),
                ('snrmax', models.FloatField()),
                ('snrmax2', models.FloatField()),
                ('snrmax3', models.FloatField()),
                ('nobs', models.IntegerField()),
                ('nobs_saturate', models.IntegerField()),
                ('diaObjectId', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='elasticc.diaobject')),
            ],
        ),
        migrations.DeleteModel(
            name='DiaObjectTrut',
        ),
    ]
