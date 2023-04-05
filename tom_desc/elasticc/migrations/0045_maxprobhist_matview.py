from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('elasticc', '0044_redo_probmetrics_matview'),
    ]

    operations = [
        migrations.RunSQL( f"""
CREATE TABLE elasticc_maxprob_timebins(
   timebin int,
   dtmin float,
   dtmax float
)
""",
                           "DROP TABLE elasticc_maxprob_timebins" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 0, -999999, -20 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 1, -20, -10 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 2, -10, 0 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 3, 0, 10 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 4, 10, 20 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 5, 20, 30 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 6, 30, 40 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 7, 40, 50 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 8, 50, 60 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 9, 60, 70 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 10, 70, 80 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 11, 80, 90 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 12, 90, 100 )", "" ),
        migrations.RunSQL( "INSERT INTO elasticc_maxprob_timebins VALUES( 13, 100, 999999 )", "" ),


        migrations.RunSQL( "GRANT SELECT ON elasticc_maxprob_timebins TO postgres_elasticc_admin_ro", "" ),

        migrations.RunSQL( f"""
CREATE MATERIALIZED VIEW elasticc_view_class_max_right_in_bin AS
  SELECT timebin, "diaObjectId", "classifierId", "trueClassId", max(probability) AS maxprob
  FROM (
    SELECT tb.timebin, s."diaObjectId", dd."classifierId", dd."trueClassId", dd."probability"
    FROM elasticc_view_dedupedclassifications dd
    INNER JOIN elasticc_maxprob_timebins tb ON dd.dt >= tb.dtmin AND dd.dt < tb.dtmax
    INNER JOIN elasticc_diaSource s ON dd."diaSourceId"=s."diaSourceId"
    WHERE dd."trueClassId"=dd."classId"
  ) subq
  GROUP BY timebin, "diaObjectId", "classifierId", "trueClassId"
""",
                           "DROP MATERIALIZED VIEW elasticc_view_class_max_right_in_bin"
                           ),
        
        migrations.RunSQL( 'CREATE INDEX ON elasticc_view_class_max_right_in_bin (timebin)', "" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc_view_class_max_right_in_bin ("classifierId")', "" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc_view_class_max_right_in_bin ("trueClassId")', "" ),

        migrations.RunSQL( "GRANT SELECT ON elasticc_view_class_max_right_in_bin TO postgres_elasticc_admin_ro", "" ),

        migrations.RunSQL( f"""
CREATE MATERIALIZED VIEW elasticc_view_nobjclassified_in_timebin AS
  SELECT "classifierId", "trueClassId", timebin, COUNT("diaObjectId") as n
  FROM (
    SELECT DISTINCT "classifierId", "diaObjectId", "trueClassId", timebin
    FROM
      ( SELECT dd."classifierId",s."diaObjectId",dd."trueClassId",tb.timebin
        FROM elasticc_view_dedupedclassifications dd
        INNER JOIN elasticc_maxprob_timebins tb ON dd.dt > tb.dtmin AND dd.dt < tb.dtmax
        INNER JOIN elasticc_diasource s ON s."diaSourceId"=dd."diaSourceId"
      ) subsubq
  ) subq
  GROUP BY "classifierId","trueClassId",timebin
""",
                           "DROP MATERIALIZED VIEW elasticc_view_nobjclassified_in_timebin"
                           ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc_view_nobjclassified_in_timebin(timebin)', "" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc_view_nobjclassified_in_timebin("trueClassId")', "" ),
        migrations.RunSQL( 'CREATE INDEX ON elasticc_view_nobjclassified_in_timebin("classifierId")', "" ),
        
        migrations.RunSQL( "GRANT SELECT ON elasticc_view_nobjclassified_in_timebin "
                           "TO postgres_elasticc_admin_ro", "" ),

    ]
