import math
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc2', '0006_maxprobhist_matview_actualhist'),
    ]

    dp0 = -1.025
    dp1 = 1.025
    ddp = 0.05
    ndp = math.floor( ( dp1 - dp0 ) / ddp + 0.5 )

    # These are the timebin indexes; see migration 0005 for translation
    earlyranges = '( 3, 4, 5 )'
    lateranges = '( 7, 8, 9, 10, 11, 12, 13 )'

    operations = [
        migrations.RunSQL( f"""
CREATE MATERIALIZED VIEW elasticc2_view_maxprobdiff AS
  SELECT early.timebin AS earlytimebin, late.timebin  AS latetimebin,
         early."diaObjectId", early."trueClassId", early."classifierId",
         early.maxprob AS earlymaxprob, late.maxprob AS latemaxprob
  FROM elasticc2_view_class_max_right_in_bin early
  INNER JOIN elasticc2_view_class_max_right_in_bin late
    ON early."diaObjectId"=late."diaObjectId" AND early."trueClassId"=late."trueClassId"
       AND early."classifierId"=late."classifierId"
       AND early.timebin IN {earlyranges} AND late.timebin IN {lateranges}
""",
                           "DROP MATERIALIZED VIEW elasticc2_view_maxprobdiff" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc2_view_maxprobdiff( "classifierId" )', "" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc2_view_maxprobdiff( "diaObjectId" )', "" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc2_view_maxprobdiff( "trueClassId" )', "" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc2_view_maxprobdiff( earlytimebin, latetimebin )', "" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc2_view_maxprobdiff( earlymaxprob, latemaxprob )', "" ),
        
        migrations.RunSQL( "GRANT SELECT ON elasticc2_view_maxprobdiff TO postgres_elasticc_admin_ro", "" ),


        migrations.RunSQL( f"""
CREATE MATERIALIZED VIEW elasticc2_view_maxprobdiff_hist AS
  SELECT "classifierId", "trueClassId", earlytimebin, latetimebin,
         probdiffbin, {dp0+ddp/2}+(probdiffbin-1)*{ddp} AS binmeanprobdiff,
         COUNT(*) AS count
  FROM (
    SELECT earlytimebin, latetimebin, "trueClassId", "classifierId",
           width_bucket( latemaxprob-earlymaxprob, {dp0}, {dp1}, {ndp} ) AS probdiffbin
    FROM elasticc2_view_maxprobdiff
  ) subq
  GROUP BY "classifierId", "trueClassId", earlytimebin, latetimebin, probdiffbin
""",
                           "DROP MATERIALIZED VIEW elasticc2_view_maxprobdiff_hist"
                           ),

        migrations.RunSQL( 'CREATE INDEX ON elasticc2_view_maxprobdiff_hist("classifierId")', "" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc2_view_maxprobdiff_hist("trueClassId")', "" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc2_view_maxprobdiff_hist(earlytimebin,latetimebin)', "" ),

        migrations.RunSQL( 'GRANT SELECT ON elasticc2_view_maxprobdiff_hist TO postgres_elasticc_admin_ro', "" ),
        
    ]
