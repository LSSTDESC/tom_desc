# Generated by Django 4.1.7 on 2023-04-05 16:55

import math
from django.db import migrations


# ROB TODO : This whole migration is broken.  Don't try to fix
#  it, just make a later one that does what you want

class Migration(migrations.Migration):

    dependencies = [
        ('elasticc2', '0005_maxprobhist_matview'),

    ]

    operations = []
#     p0 = -0.025
#     p1 = 1.025
#     dp = 0.05
#     np = math.floor( ( p1 - p0 ) / dp + 0.5 )

#     operations = [
#         migrations.RunSQL( f"""
# CREATE MATERIALIZED VIEW elasticc2_view_class_max_right_hist AS
#   SELECT subq.timebin, dtmin, dtmax, maxprobbin, {p0+dp/2}+(maxprobbin-1)*{dp} AS binmeanmaxprob,
#          "classifierId", "trueClassId", COUNT(*) AS count
#   FROM (
#     SELECT timebin, "classifierId", "trueClassId", width_bucket( maxprob, {p0}, {p1}, {np} ) as maxprobbin
#     FROM elasticc2_view_class_max_right_in_bin
#   ) subq
#   INNER JOIN elasticc2_maxprob_timebins tb ON subq.timebin=tb.timebin
#   GROUP BY subq.timebin,dtmin,dtmax,maxprobbin,"classifierId","trueClassId"
# """,
#                            "DROP MATERIALIZED VIEW elasticc2_view_class_max_right_hist"
#                           ),
#         migrations.RunSQL( 'CREATE INDEX ON elasticc2_view_class_max_right_hist("classifierId")', "" ),
#         migrations.RunSQL( 'CREATE INDEX ON elasticc2_view_class_max_right_hist("trueClassId")', "" ),

#         migrations.RunSQL( "GRANT SELECT ON elasticc2_view_class_max_right_hist TO postgres_elasticc_admin_ro", "" ),
#     ]
