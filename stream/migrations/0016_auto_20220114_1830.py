# Generated by Django 3.2.11 on 2022-01-14 18:30

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('stream', '0015_auto_20220111_2014'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='declErr',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='parallax',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='parallaxErr',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='pmDecl',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='pmDeclErr',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='pmDecl_parallax_Cov',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='pmParallaxChi2',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='pmParallaxLnL',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='pmParallaxNdata',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='pmRa',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='pmRaErr',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='pmRa_parallax_Cov',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='pmRa_pmDecl_Cov',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='raErr',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='ra_decl_Cov',
        ),
        migrations.RemoveField(
            model_name='elasticcdiaobject',
            name='radecTai',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='ccdVisitId',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='declErr',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='flags',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_ellipticity',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_mag_Y',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_mag_g',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_mag_i',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_mag_r',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_mag_u',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_mag_z',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_magerr_Y',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_magerr_g',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_magerr_i',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_magerr_r',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_magerr_u',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_magerr_z',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_radec',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_snsep',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_sqradius',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal2_z',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_ellipticity',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_mag_Y',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_mag_g',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_mag_i',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_mag_r',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_mag_u',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_mag_z',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_magerr_Y',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_magerr_g',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_magerr_i',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_magerr_r',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_magerr_u',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_magerr_z',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_radec',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='hostgal_sqradius',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='parentDiaSource',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='programId',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='psFlux',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='psFluxErr',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='raErr',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='ra_decl_Cov',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='snr',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='ssObject',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='x',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='xErr',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='x_y_Cov',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='y',
        ),
        migrations.RemoveField(
            model_name='elasticcdiasource',
            name='yErr',
        ),
        migrations.DeleteModel(
            name='ElasticcAlert',
        ),
        migrations.DeleteModel(
            name='ElasticcSSObject',
        ),
    ]