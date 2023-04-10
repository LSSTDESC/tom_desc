import sys
import logging
import psycopg2
import psycopg2.extras
import django.db
from django.core.management.base import BaseCommand, CommandError

_logger = logging.getLogger( __name__ )
_logger.propagate = False
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.INFO )

class Command(BaseCommand):
    help = 'Make new partitions for the elasticc2_brokerclassification table'

    def add_arguments( self, parser ):
        pass

    def handle( self, *args, **options ):
        conn = django.db.connection.cursor().connection

        with conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
            _logger.info( "Pulling broker identifications from default partition" )
            cursor.execute( 'SELECT DISTINCT "classifierId" FROM elasticc2_brokerclassification_default '
                            'ORDER BY "classifierId"' )
            whichbrokers = [ row['classifierId'] for row in cursor.fetchall() ]
            _logger.info( f"{len(whichbrokers)} new brokers to move out of the default table." )
        
        for i, brokerid in enumerate(whichbrokers):
            with conn.cursor() as cursor:
                _logger.info( f"**** Working on broker {brokerid} ({i} of {len(whichbrokers)})" )
                try:
                    # I don't think I need this with psycopg2
                    # cursor.execute( "BEGIN TRANSACTION" )
                    # cursor.execute( "LOCK TABLE elasticc2_brokerclassification" )
                    _logger.info( "Detaching default partition" )
                    sql = ( 'ALTER TABLE elasticc2_brokerclassification '
                            'DETACH PARTITION elasticc2_brokerclassification_default' )
                    cursor.execute( sql )
                    _logger.info( "Creating partition for brokerid {brokerid}" )
                    sql = ( f'CREATE TABLE elasticc2_brokerclassification_{brokerid} '
                            f'PARTITION OF elasticc2_brokerclassification '
                            f'FOR VALUES IN ({brokerid})' )
                    cursor.execute( sql )
                    _logger.info( "Populating new partition" )
                    # I have to explicitly list the columns here because of how
                    #  the default partition was created; the columns may not be
                    #  in the same order as they were in the parent table.
                    sql = ( f'INSERT INTO elasticc2_brokerclassification_{brokerid}'
                            f'("classificationId","classId","probability","modified",'
                            f' "brokerMessageId","classifierId")'
                            f'SELECT "classificationId","classId","probability","modified",'
                            f'       "brokerMessageId","classifierId" '
                            f'FROM elasticc2_brokerclassification_default '
                            f'WHERE "classifierId"=%(classifierid)s' )
                    cursor.execute( sql, { 'classifierid': brokerid } )
                    _logger.info( f"Cleaning classifier {brokerid} out of default partition" )
                    sql = ( f'DELETE FROM elasticc2_brokerclassification_default '
                            f'WHERE "classifierId"=%(classifierid)s' )
                    cursor.execute( sql, { 'classifierid': brokerid } )
                    _logger.info( "Reattaching default table" )
                    sql = ( 'ALTER TABLE elasticc2_brokerclassification '
                            'ATTACH PARTITION elasticc2_brokerclassification_default DEFAULT ' )
                    cursor.execute( sql )
                    _logger.info( "Commiting transaction" )
                    conn.commit()
                except Exception as ex:
                    _logger.exception( 'Exception running SQL' )
                    conn.rollback()
                    raise ex
