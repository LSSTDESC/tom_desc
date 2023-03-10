import math
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc', '0042_view_indexes'),
    ]

    operations = [
        migrations.RunSQL( f"""
CREATE MATERIALIZED VIEW elasticc_view_dedupedclassifications AS
  SELECT c."classifierId", c."brokerMessageId", c."classId", gtoc."classId" AS "trueClassId", m."diaSourceId",
         s."midPointTai"-ot.peakmjd AS dt,
         COUNT(probability) AS n, AVG(probability) AS probability, STDDEV(probability) AS sigma
  FROM elasticc_brokerclassification c
  INNER JOIN elasticc_brokermessage m ON c."brokerMessageId"=m."brokerMessageId"
  INNER JOIN elasticc_diasource s ON m."diaSourceId"=s."diaSourceId"
  INNER JOIN elasticc_diaobjecttruth ot ON s."diaObjectId"=ot."diaObjectId"
  INNER JOIN elasticc_gentypeofclassid gtoc ON ot.gentype=gtoc.gentype
  GROUP BY c."classifierId",c."brokerMessageId",c."classId",gtoc."classId",m."diaSourceId",s."midPointTai",ot.peakmjd
"""),
        migrations.RunSQL( "GRANT SELECT ON elasticc_view_dedupedclassifications TO postgres_elasticc_admin_ro" ),
        migrations.RunSQL( 'CREATE INDEX elasticc_view_dedupedcls_cfer ON elasticc_view_dedupedclassifications("classifierId")' ),
        migrations.RunSQL( 'CREATE INDEX elasticc_view_dedupedcls_cls ON elasticc_view_dedupedclassifications("classId")' ),
        migrations.RunSQL( 'CREATE INDEX elasticc_view_dedupedcls_trcls ON elasticc_view_dedupedclassifications("trueClassId")' ),
    ]      
        

