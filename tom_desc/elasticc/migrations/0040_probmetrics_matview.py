# Generated by Django 4.0.6 on 2022-08-19 17:34

import math
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc', '0039_redo_elasticc_sourceclass_view'),
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
        migrations.RunSQL( f"""
CREATE MATERIALIZED VIEW elasticc_view_classifications_probmetrics AS
  SELECT "classifierId","classId",trueclassid,tbin,probbin,COUNT(*) FROM
  ( SELECT c."classifierId",c."classId",gtoc."classId" AS trueclassid,
           width_bucket( s."midPointTai"-ot.peakmjd, {t0}, {t1}, {nt} ) AS tbin,
           width_bucket( c.probability, {p0}, {p1}, {np} ) as probbin
    FROM elasticc_brokerclassification c
    INNER JOIN elasticc_brokermessage m ON c."brokerMessageId"=m."brokerMessageId"
    INNER JOIN elasticc_diasource s ON m."diaSourceId"=s."diaSourceId"
    INNER JOIN elasticc_diaobjecttruth ot ON s."diaObjectId"=ot."diaObjectId"
    INNER JOIN elasticc_gentypeofclassid gtoc ON ot.gentype=gtoc.gentype
  ) subq
  GROUP BY "classifierId","classId",trueclassid,tbin,probbin
""" ),
        migrations.RunSQL( "GRANT SELECT ON elasticc_view_classifications_probmetrics TO postgres_elasticc_admin_ro" )
    ]
