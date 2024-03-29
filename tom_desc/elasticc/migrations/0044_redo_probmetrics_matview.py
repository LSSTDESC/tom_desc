# Generated by Django 4.0.6 on 2022-08-19 17:34

import math
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc', '0043_view_dedupedclassifications'),
    ]

    t0 = -30.
    t1 = 100.
    dt = 5.
    nt = math.floor( ( t1 - t0 ) / dt + 0.5 )
    
    p0 = 0.
    p1 = 1.
    dp = 0.05
    np = math.floor( ( p1 - p0 ) / dp + 0.5 )
    
    operations = [
        migrations.RunSQL( "DROP MATERIALIZED VIEW elasticc_view_classifications_probmetrics" ),
        migrations.RunSQL( f"""
CREATE MATERIALIZED VIEW elasticc_view_classifications_probmetrics AS
  SELECT "classifierId","classId","trueClassId",tbin,probbin,COUNT(*) FROM
  ( SELECT "classifierId","classId","trueClassId",
           width_bucket( dt, {t0}, {t1}, {nt} ) AS tbin,
           width_bucket( probability, {p0}, {p1}, {np} ) as probbin
    FROM elasticc_view_dedupedclassifications
  ) subq
  GROUP BY "classifierId","classId","trueClassId",tbin,probbin
""" ),
        migrations.RunSQL( "GRANT SELECT ON elasticc_view_classifications_probmetrics TO postgres_elasticc_admin_ro" ),
        migrations.RunSQL( f"""
CREATE INDEX elasticc_view_classifications_probmetrix_cferdex ON elasticc_view_classifications_probmetrics("classifierId")
"""),
        migrations.RunSQL( f"""
CREATE INDEX elasticc_view_classifications_probmetrix_clsdex ON elasticc_view_classifications_probmetrics("classId")
"""),
        migrations.RunSQL( f"""
CREATE INDEX elasticc_view_classifications_probmetrix_truedex ON elasticc_view_classifications_probmetrics("trueClassId")
""")
    ]
