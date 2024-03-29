# Generated by Django 4.0.6 on 2022-08-19 17:34

import math
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc', '0041_view_classifiersourcetotprob'),
    ]

    operations = [
        migrations.RunSQL( f"""
CREATE INDEX elasticc_view_classifications_totprobpersource_cferid ON elasticc_view_classifications_totprobpersource("classifierId")
"""),
        migrations.RunSQL( f"""
CREATE INDEX elasticc_view_classifications_totprobpersource_srcid ON elasticc_view_classifications_totprobpersource("diaSourceId")
"""),
        migrations.RunSQL( f"""
CREATE INDEX elasticc_view_classifications_probmetrix_cferdex ON elasticc_view_classifications_probmetrics("classifierId")
"""),
        migrations.RunSQL( f"""
CREATE INDEX elasticc_view_classifications_probmetrix_clsdex ON elasticc_view_classifications_probmetrics("classId")
"""),
        migrations.RunSQL( f"""
CREATE INDEX elasticc_view_classifications_probmetrix_truedex ON elasticc_view_classifications_probmetrics(trueclassid)
""")
    ]
