raise RuntimeError( "May be out of date; Rob, review." )

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

    # The brokersourceids table are all the sourceids of broker messages
    # we're received since the last time we wiped that table.  This
    # command looks at that list, and figures out which of the objects
    # and sources on that list aren't already in the DiaObject and
    # DiaSource tables, copying them from the PPDBDia* tables as
    # necessary.  It also adds the appropriate forced photometry from
    # the PPDBDiaForcedSource table (i.e. all of that with a time less
    # than the time of the latest source for an object).

    # Do all of this with SQL rather than Django commands so that long
    # lists of data don't have to be copied from the postgres server to
    # the django server and back.  Since, ultimately, we're copying from
    # one table to another table, there's no need for all that data to
    # be transferred between servers.
    
    def add_arguments( self, parser ):
        pass

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
            
            # The first thing we have to do is make a copy of the
            # BrokerSourceIds table.  We don't want to lock that table
            # for this whole transaction, because this transaction might
            # be a bit long.  And, we need to erase from that table all
            # the IDs we processed.  So, I start by making a copy of
            # that table and then wiping the original inside a
            # transaction.
            brokersourceidsize = BrokerSourceIds.objects.count()
            _logger.info( f"There are {brokersourceidsize} objects in the brokersourceids table" )
            
            tmpsourcetable = f'brokersourceids_tmp_{"".join(random.choices("0123456789abdef", k=8))}'
            _logger.info( f"Copying elasticc2_brokersourceids to {tmpsourcetable}" )
            cursor = conn.cursor()
            cursor.execute( "LOCK TABLE elasticc2_brokersourceids" )
            cursor.execute( f"CREATE TABLE {tmpsourcetable} AS TABLE elasticc2_brokersourceids" )
            cursor.execute( "TRUNCATE TABLE elasticc2_brokersourceids" )
            conn.commit()

            # Now for the long transaction

            cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )
            
            # Get a list of all objects assocated with these sources
            # I'm not sure which of the following two ways of selecting out
            # things from tmpsourcetable is faster; I should investigate
            #  f"    WHERE diasource_id IN ( SELECT diasource_id FROM {tmpsourcetable} ) "
            #  f"    INNER JOIN {tmpsourcetable} t ON s.diaobject_id=t.dia_object_id "

            _logger.info( "Making all_objids temp table" )
            cursor.execute( "CREATE TEMP TABLE all_objids( id bigint, latesttai double precision )" )
            cursor.execute( f"INSERT INTO all_objids "
                            f"  SELECT diaobject_id, MAX(midpointtai) FROM elasticc2_ppdbdiasource s "
                            f"    WHERE diasource_id IN ( SELECT diasource_id FROM {tmpsourcetable} ) "
                            f"GROUP BY diaobject_id" )
            cursor.execute( "CREATE INDEX ON all_objids(id)" );
            # ****
            # cursor.execute( "SELECT COUNT(*) AS count FROM all_objids" )
            # row = cursor.fetchone()
            # _logger.debug( f'all_objids has {row["count"]} rows' )
            # ****
            _logger.info( "Making existing_objids temp table" )
            cursor.execute( "CREATE TEMP TABLE existing_objids( id bigint, latesttai double precision  )" )
            cursor.execute( "INSERT INTO existing_objids "
                            "  SELECT a.id, a.latesttai FROM all_objids a "
                            "  INNER JOIN elasticc2_diaobject o ON o.diaobject_id=a.id" )
            # ****
            # cursor.execute( "SELECT COUNT(*) AS count FROM existing_objids" )
            # row = cursor.fetchone()
            # _logger.debug( f'existing_objids has {row["count"]} rows' )
            # ****
            _logger.info( "Making new_objs temp table" )
            cursor.execute( "CREATE TEMP TABLE new_objs ( LIKE elasticc2_diaobject )" )
            newobjsfields = ','.join( DiaObject._create_kws )
            ppdbobjsfields = ','.join( [ f"o.{i}" for i in PPDBDiaObject._create_kws ] )
            cursor.execute( f"INSERT INTO new_objs({newobjsfields}) "
                            f" SELECT {ppdbobjsfields} FROM elasticc2_ppdbdiaobject o "
                            f" INNER JOIN all_objids a ON o.diaobject_id=a.id "
                            f" WHERE o.diaobject_id NOT IN "
                            f"   ( SELECT id FROM existing_objids )" )
            # ****
            # cursor.execute( "SELECT COUNT(*) AS count FROM new_objs" )
            # row = cursor.fetchone()
            # _logger.debug( f'new_objs has {row["count"]} rows' )
            # ****
            _logger.info( "Inserting into elasticc2_diaobject" )
            cursor.execute( "INSERT INTO elasticc2_diaobject SELECT * FROM new_objs" )

            _logger.info( "Getting list of new object ids for tom targets later" )
            cursor.execute( "SELECT diaobject_id,ra,decl FROM new_objs" )
            newobjs = cursor.fetchall()
            
            # WARNING : I'm doing this slightly wrong.  Really, we shouldn't
            # add forced sources until this detection is at least a day
            # later than the first detection, because forced sources won't exist yet.
            # However, we know that *eventually* we're going to get all the forced sources
            # for any candidate, so just grab them all now and accept that when we first get them,
            # we're getting them "too soon".

            _logger.info( "Creating allsourceids temp table" )
            cursor.execute( "CREATE TEMP TABLE allsourceids( id bigint )" )
            cursor.execute( "INSERT INTO allsourceids "
                            "  SELECT s.diasource_id FROM elasticc2_ppdbdiasource s "
                            "  INNER JOIN all_objids a ON s.diaobject_id=a.id "
                            "  WHERE s.midpointtai <= a.latesttai" )
            cursor.execute( "CREATE INDEX ON allsourceids(id)" )
            # ****
            # cursor.execute( "SELECT COUNT(*) AS count FROM allsourceids" )
            # row = cursor.fetchone()
            # _logger.debug( f'allsourceids has {row["count"]} rows' )
            # ****
            _logger.info( "Creating exsitingsourceids temp table" )
            cursor.execute( "CREATE TEMP TABLE existingsourceids( id bigint )" )
            cursor.execute( "INSERT INTO existingsourceids "
                            "  SELECT a.id FROM allsourceids a "
                            "  INNER JOIN elasticc2_diasource s ON s.diasource_id=a.id" )
            # ****
            # cursor.execute( "SELECT COUNT(*) AS count FROM existingsourceids" )
            # row = cursor.fetchone()
            # _logger.debug( f'existingsourceids has {row["count"]} rows' )
            # ****
            _logger.info( "Creating new_srcs temp table" )
            cursor.execute( "CREATE TEMP TABLE new_srcs ( LIKE elasticc2_diasource )" )
            newsrcfields = ','.join( DiaSource._create_kws )
            ppdbsrcfields = ','.join( [ f"s.{i}" for i in PPDBDiaSource._create_kws ] )
            # _logger.debug( f'ppdbsrcfields = {ppdbsrcfields}' )
            cursor.execute( f"INSERT INTO new_srcs({newsrcfields}) "
                            f" SELECT {ppdbsrcfields} FROM elasticc2_ppdbdiasource s "
                            f" INNER JOIN allsourceids a ON s.diasource_id=a.id "
                            f" WHERE s.diasource_id NOT IN "
                            f"   ( SELECT id FROM existingsourceids )" )
            # ****
            # cursor.execute( "SELECT COUNT(*) AS count FROM new_srcs" )
            # row = cursor.fetchone()
            # _logger.debug( f'new_srcs has {row["count"]} rows' )
            # ****
            _logger.info( "Inserting into elasticc2_diasource" )
            cursor.execute( f"INSERT INTO elasticc2_diasource SELECT * FROM new_srcs" )

            # Forced source is going to be slower because that table is freaking huge

            _logger.info( "Creating allforcedids temp table" )
            cursor.execute( "CREATE TEMP TABLE allforcedids( diaforcedsource_id bigint )" )
            cursor.execute( "INSERT INTO allforcedids "
                            "  SELECT s.diaforcedsource_id FROM elasticc2_ppdbdiaforcedsource s "
                            "  INNER JOIN all_objids a ON s.diaobject_id=a.id "
                            "  WHERE s.midpointtai <= a.latesttai" )
            cursor.execute( "CREATE INDEX ON allforcedids(diaforcedsource_id)" )
            # ****
            # cursor.execute( "SELECT COUNT(*) AS count FROM allforcedids" )
            # row = cursor.fetchone()
            # _logger.debug( f'allforcedids has {row["count"]} rows' )
            # ****
            _logger.info( "Creating existingforcedids table" )
            cursor.execute( "CREATE TEMP TABLE existingforcedids( diaforcedsource_id bigint )" )
            cursor.execute( "INSERT INTO existingforcedids "
                            "  SELECT a.diaforcedsource_id FROM allforcedids a "
                            "  INNER JOIN elasticc2_diaforcedsource s ON s.diaforcedsource_id=a.diaforcedsource_id" )
            # ****
            # cursor.execute( "SELECT COUNT(*) AS count FROM existingforcedids" )
            # row = cursor.fetchone()
            # _logger.debug( f'existingforcedids {row["count"]} rows' )
            # ****
            # This one tends to be incredibly slow
            # ROB : try replacing the WHERE s.diaforcedsource_id NOT IN ( SELECT... ) with:
            #   LEFT JOIN existingforcedids e ON s.diaforcedsource_id=e.diaforcedsource_id
            #   WHERE e.diaforcedsource_id IS NULL
            # --or--
            #   WHERE NOT EXISTS ( SELECT e.diaforcedsource_id FROM existingforcedids e
            #                      WHERE e.diaforcedsource_id=s.diaforcedsource_id )
            # (Something I've read suggests that with postgres, these two are
            # semantically equivalent.)
            _logger.info( "Creating new_forced table" )
            cursor.execute( "CREATE TEMP TABLE new_forced ( LIKE elasticc2_diaforcedsource )" )
            newforcedfields = ','.join( DiaForcedSource._create_kws )
            ppdbforcedfields = ','.join( [ f"s.{i}" for i in PPDBDiaForcedSource._create_kws ] )
            cursor.execute( f"INSERT INTO new_forced({newforcedfields}) "
                            f" SELECT {ppdbforcedfields} FROM elasticc2_ppdbdiaforcedsource s "
                            f" INNER JOIN allforcedids a ON s.diaforcedsource_id=a.diaforcedsource_id "
                            f" WHERE s.diaforcedsource_id NOT IN "
                            f"   ( SELECT diaforcedsource_id FROM existingforcedids )" )
            # ****
            # cursor.execute( "SELECT COUNT(*) AS count FROM new_forced" )
            # row = cursor.fetchone()
            # _logger.debug( f'new_forced {row["count"]} rows' )
            # ****
            # This one tends to be incredibly slow.
            # ROB; consider temporarily disabling indexes.
            #  See : https://fle.github.io/temporarily-disable-all-indexes-of-a-postgresql-table.html
            _logger.info( "Inserting into elasticc2_diaforcedsource" )
            cursor.execute( f"INSERT INTO elasticc2_diaforcedsource SELECT * FROM new_forced" )

            # Turns out that temp tables aren't dropped at the end of a transaction
            _logger.info ( "Dropping temp tables" )
            cursor.execute( "DROP TABLE new_forced" )
            cursor.execute( "DROP TABLE existingforcedids" )
            cursor.execute( "DROP TABLE allforcedids" )
            cursor.execute( "DROP TABLE new_srcs" )
            cursor.execute( "DROP TABLE existingsourceids" )
            cursor.execute( "DROP TABLE allsourceids" )
            cursor.execute( "DROP TABLE new_objs" )
            cursor.execute( "DROP TABLE existing_objids" )
            cursor.execute( "DROP TABLE all_objids" )

            _logger.info( "Committing..." )
            conn.commit()
            _logger.info( "...done committing." )
        except Exception as e:
            _logger.exception( e )
            if conn is not None:
                conn.rollback()
                if tmpsourcetable is not None:
                    _logger.warning( f"Restoring brokersourceids from {tmpsourcetable}" )
                    cursor = conn.cursor()
                    cursor.execute( f"INSERT INTO elasticc2_brokersourceids "
                                    f"SELECT * FROM {tmpsourcetable} "
                                    f"ON CONFLICT DO NOTHING" )
                    conn.commit()
            raise e
        finally:
            if cursor is not None:
                cursor.close()
                cursor = None
            if gratuitous is not None:
                gratuitous.close()
                gratuitous = None
            if tmpsourcetable is not None:
                _logger.info( f"Dropping table {tmpsourcetable}" )
                cursor = conn.cursor()
                cursor.execute( f"DROP TABLE {tmpsourcetable}" )
                conn.commit()
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
        
        # Create TOM targets if necessary

        _logger.info( "Creating new tom targets" )
        if len(newobjs) > 0:
            targobjids = [ row['diaobject_id'] for row in newobjs ]
            targobjras = [ row['ra'] for row in newobjs ]
            targobjdecs = [ row['decl'] for row in newobjs ]
            DiaObjectOfTarget.maybe_new_elasticc_targets( targobjids, targobjras, targobjdecs )

        _logger.info( "Done." )
