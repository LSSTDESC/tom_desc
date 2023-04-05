import math
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc', '0045_maxprobhist_matview'),

    ]

    p0 = -0.025
    p1 = 1.025
    dp = 0.05
    np = math.floor( ( p1 - p0 ) / dp + 0.5 )

    operations = [
        migrations.RunSQL( f"""
CREATE MATERIALIZED VIEW elasticc_view_class_max_right_hist AS
  SELECT subq.timebin, dtmin, dtmax, maxprobbin, {p0+dp/2}+(maxprobbin-1)*{dp} AS binmeanmaxprob,
         "classifierId", "trueClassId", COUNT(*) AS count
  FROM (
    SELECT timebin, "classifierId", "trueClassId", width_bucket( maxprob, {p0}, {p1}, {np} ) as maxprobbin
    FROM elasticc_view_class_max_right_in_bin
  ) subq
  INNER JOIN elasticc_maxprob_timebins tb ON subq.timebin=tb.timebin
  GROUP BY subq.timebin,dtmin,dtmax,maxprobbin,"classifierId","trueClassId"
""",
                           "DROP MATERIALIZED VIEW elasticc_view_class_max_right_hist"
                          ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc_view_class_max_right_hist("classifierId")', "" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc_view_class_max_right_hist("trueClassId")', "" ),

        migrations.RunSQL( "GRANT SELECT ON elasticc_view_class_max_right_hist TO postgres_elasticc_admin_ro", "" ),
    ]
