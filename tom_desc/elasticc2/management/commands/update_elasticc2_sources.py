import sys
import re
import pathlib
import datetime
import logging
import random
import psycopg2
import psycopg2.extras
import django.db
from django.core.management.base import BaseCommand, CommandError
from elasticc2.models import PPDBDiaObject, PPDBDiaSource, PPDBDiaForcedSource
from elasticc2.models import DiaObject, DiaSource, DiaForcedSource, BrokerSourceIds, DiaObjectOfTarget

_rundir = pathlib.Path(__file__).parent

_logger = logging.getLogger( __name__ )
_logger.propagate = False
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.INFO )

class Command(BaseCommand):
    help = 'Update DiaObject, DiaSources, and DiaSorcedSources from PPDB with new sources learned from brokers.'

    def add_arguments( self, parser ):
        parser.add_argument( '--doall', action='store_true', default=False,
                             help=( "Search the whole broker message table, not just the "
                                    "brokersourceids table." ) )

    def handle( self, *args, **options ):
        conn = None
        origautocommit = None
        gratuitous = None
        cursor = None
        setrunningflag = False
        tmpsourcetable = None
        newobjs = []
        try:
            # Have to jump through some hoops to get the actual psycopg2
            # connection from django; we need this to turn off autocommit
            # so we can use a temp table
            gratuitous = django.db.connection.cursor()
            conn = gratuitous.connection
            origautocommit = conn.autocommit
            conn.autocommit = False

            # Make sure that one of these commands isn't already running.  It would
            # probably be a mess if multiple were running it once.  (It might
            # actually be OK, but it would be gratuitous load on the database.)
            _logger.info( "Setting elasticc2_importppdbrunning to true" )
            cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )
            cursor.execute( "LOCK TABLE elasticc2_importppdbrunning" )
            cursor.execute( "SELECT running FROM elasticc2_importppdbrunning" )
            rows = cursor.fetchall()
            if ( len(rows) == 0 ) or ( len(rows) > 1 ):
                _logger.error( f"There are {len(rows)} rows in the importppdbrunning table; should only be 1" )
                raise RuntimeError( "importppdbrunning table corrupted" )
            if rows[0]['running']:
                _logger.error( "importppdbrunning table indicates this command is already running" )
                raise RuntimeError( "update_elasticc2_sources already running" )
            cursor.execute( "UPDATE elasticc2_importppdbrunning SET running=true" )
            setrunningflag = True
            conn.commit()

            cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )

                             
            # Figure out which sources we don't know about

            sourcetab = 'elasticc2_brokermessage' if options['doall'] else 'elasticc2_brokersourceids'

            _logger.info( "Finding unknown sources" )
            cursor.execute( "CREATE TEMP TABLE tmp_unknownsources( diasource_id bigint )" )
            cursor.execute( f"INSERT INTO tmp_unknownsources "
                            f"  SELECT bid FROM "
                            f"    ( SELECT DISTINCT ON (b.diasource_id) b.diasource_id AS bid, s.diasource_id AS sid "
                            f"      FROM {sourcetab} b "
                            f"      LEFT JOIN elasticc2_diasource s ON b.diasource_id=s.diasource_id "
                            f"      WHERE s.diasource_id IS NULL ) subq" )

            _logger.info( "Finding updated and unknown objects" )
            cursor.execute( "CREATE TEMP TABLE tmp_updatedobjects( diaobject_id bigint )" )
            cursor.execute( "INSERT INTO tmp_updatedobjects "
                            "  SELECT pid FROM "
                            "  ( SELECT DISTINCT ON (p.diaobject_id) p.diaobject_id AS pid "
                            "    FROM tmp_unknownsources t "
                            "    INNER JOIN elasticc2_ppdbdiasource p ON t.diasource_id=p.diasource_id ) subq" )

            _logger.info( "Finding unknown objects" )
            cursor.execute( "CREATE TEMP TABLE tmp_unknownobjects( diaobject_id bigint )" )
            cursor.execute( "INSERT INTO tmp_unknownobjects "
                            "  SELECT pid FROM "
                            "  ( SELECT t.diaobject_id AS pid "
                            "    FROM tmp_updatedobjects t"
                            "    LEFT JOIN elasticc2_diaobject o ON t.diaobject_id=o.diaobject_id "
                            "    WHERE o.diaobject_id IS NULL ) subq" )
            # cursor.execute( "INSERT INTO tmp_unknownobjects "
            #                 "  SELECT pid FROM "
            #                 "    ( SELECT DISTINCT ON (p.diaobject_id) p.diaobject_id AS pid, o.diaobject_id AS oid"
            #                 "      FROM tmp_unknownsources t "
            #                 "      INNER JOIN elasticc2_ppdbdiasource p ON t.diasource_id=p.diasource_id "
            #                 "      LEFT JOIN elasticc2_diaobject o ON p.diaobject_id=o.diaobject_id "
            #                 "      WHERE o.diaobject_id IS NULL ) subq" )

            _logger.info( "Importing unknown objects" )
            cursor.execute( "INSERT INTO elasticc2_diaobject "
                            "  ( SELECT o.* FROM elasticc2_ppdbdiaobject o "
                            "    INNER JOIN tmp_unknownobjects t "
                            "    ON o.diaobject_id=t.diaobject_id )" )

            _logger.info( "Importing unknown sources" )
            cursor.execute( "INSERT INTO elasticc2_diasource "
                            "  ( SELECT s.* FROM tmp_unknownsources t "
                            "    INNER JOIN elasticc2_ppdbdiasource s "
                            "    ON s.diasource_id=t.diasource_id )" )

            _logger.info( "Finding latest known source for each object" )
            cursor.execute( "CREATE TEMP TABLE tmp_latestsource "
                            "  ( diaobject_id bigint, midpointtai double precision )" )
            cursor.execute( "INSERT INTO tmp_latestsource "
                            "  SELECT diaobject_id, MAX(midpointtai) "
                            "  FROM elasticc2_diasource "
                            "  GROUP BY diaobject_id" )

            # This one takes freaking forever
            _logger.info( "Importing new forced sources" )
            cursor.execute( "INSERT INTO elasticc2_diaforcedsource "
                            "  ( SELECT p.* FROM tmp_latestsource t "
                            "    INNER JOIN elasticc2_ppdbdiaforcedsource p "
                            "      ON t.diaobject_id=p.diaobject_id AND p.midpointtai <= t.midpointtai ) "
                            "ON CONFLICT DO NOTHING" )

            # ...this one will usually be a slow null operation,
            # as the first source is not likely to change as additional sources are added
            _logger.info( "Updating DiaObjectInfo -- first source in each filter" )
            cursor.execute( "INSERT INTO elasticc2_diaobjectinfo(diaobject_id,filtername,"
                            "                                    firstsource_id,firstsourceflux,"
                            "                                    firstsourcefluxerr,firstsourcemjd) "
                            "  SELECT DISTINCT ON (t.diaobject_id,s.filtername) "
                            "     t.diaobject_id,s.filtername,s.diasource_id,s.psflux,s.psfluxerr,s.midpointtai "
                            "  FROM tmp_updatedobjects t "
                            "  INNER JOIN elasticc2_diasource s ON t.diaobject_id=s.diaobject_id "
                            "  ORDER BY t.diaobject_id,s.filtername,s.midpointtai "
                            "ON CONFLICT ON CONSTRAINT diaobjectinfo_unique DO UPDATE "
                            "  SET firstsource_id=excluded.firstsource_id, "
                            "      firstsourceflux=excluded.firstsourceflux, "
                            "      firstsourcefluxerr=excluded.firstsourcefluxerr, "
                            "      firstsourcemjd=excluded.firstsourcemjd" )

            _logger.info( "Updating DiaObjectInfo -- latest source in each filter" )
            cursor.execute( "INSERT INTO elasticc2_diaobjectinfo(diaobject_id,filtername,"
                            "                                    latestsource_id,latestsourceflux,"
                            "                                    latestsourcefluxerr,latestsourcemjd) "
                            "  SELECT DISTINCT ON (t.diaobject_id,s.filtername) "
                            "     t.diaobject_id,s.filtername,s.diasource_id,s.psflux,s.psfluxerr,s.midpointtai "
                            "  FROM tmp_updatedobjects t "
                            "  INNER JOIN elasticc2_diasource s ON t.diaobject_id=s.diaobject_id "
                            "  ORDER BY t.diaobject_id,s.filtername,s.midpointtai DESC "
                            "ON CONFLICT ON CONSTRAINT diaobjectinfo_unique DO UPDATE "
                            "  SET latestsource_id=excluded.latestsource_id, "
                            "      latestsourceflux=excluded.latestsourceflux, "
                            "      latestsourcefluxerr=excluded.latestsourcefluxerr, "
                            "      latestsourcemjd=excluded.latestsourcemjd" )

        except Exception as e:
            _logger.exception( "Exception during transaction, rolling back." )
            conn.rollback()

        finally:
            if cursor is not None:
                cursor.close()
                cursor = None
            # TODO: drop temp tables
            if setrunningflag:
                _logger.info( "Setting elasticc2_importppdbrunning to false" )
                cursor = conn.cursor()
                # Not bothering to lock the table here, since we don't read-then-write
                cursor.execute( "UPDATE elasticc2_importppdbrunning SET running=false" )
                conn.commit()
            if origautocommit is not None and conn is not None:
                conn.autocommit = origautocommit
                origautocommit = None
                conn = None



        _logger.info( "Done." )
