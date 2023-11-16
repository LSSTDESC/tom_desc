import sys
import argparse
import logging
import subprocess

import psycopg2
import psycopg2.extras

_logger = logging.getLogger(__name__)
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.INFO )

class ArgFormatter( argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter ):
    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )

def main():
    parser = argparse.ArgumentParser( 'import_elasticc2_subset_dump',
                                      description='Populated elasticc2 tables from SQL dump file',
                                      formatter_class=ArgFormatter,
                                      epilog="""Utility for populating elasticc2 tables with sample data.

This will take the data created by a separate run of
make_elasticc2_subset_dump.py and import it into the elasticc2 tables of
the running database.  It is intended to be used for small development
installations of the TOM.

It will only populate the elasticc2_ppdb*, elasticc2_diaobjecttruth,
elasticc2_diaforcedsource, elasticc2_brokermessage, and
elasticc2_brokerclassifier tables.  It will not populate
elasticc2_diaobject, etc.

Use this and the other utility with care.  In particular, it's on the user
to make sure that they have a fresh database with empty tables before
running restore_elasticc2_subset_dump.py.

This utility is up to date with the table structure as of 2023-11-16.
If the table structure has evolved since then, it may no longer work.

""")
    parser.add_argument( '-H', '--host', default='postgres', help='PostgreSQL host' )
    parser.add_argument( '-p', '--port', default=5432, type=int, help='PostgreSQL server port' )
    parser.add_argument( '-U', '--username', default='postgres', help='PostgreSQL username' )
    parser.add_argument( '-P', '--password', default='fragile', help='PostgreSQL password' )
    parser.add_argument( '-f', '--file', default='elasticc2_dump.sql',
                         help='Input pg_dump file' )
    args = parser.parse_args()

    # Order matters here
    tables = [
        'elasticc2_ppdbdiaobject',
        'elasticc2_ppdbdiasource',
        'elasticc2_ppdbdiaforcedsource',
        'elasticc2_ppdbalert',
        'elasticc2_diaobjecttruth',
        'elasticc2_brokerclassifier',
        'elasticc2_brokermessage',
    ]

    _logger.info( "Running pg_restore" )
    command = [ 'pg_restore', '-h', args.host, '-p', str(args.port), '-U', args.username,
                '-F', 'c', '-v', '-d', 'tom_desc', args.file ]
    res = subprocess.run( command, capture_output=False, env={'PGPASSWORD': args.password} )
    if res.returncode != 0:
        _logger.error( f"Subprocessed failed\ncommand: {res.args}" )
        sys.exit( 1 )

    dbcon = psycopg2.connect( dbname='tom_desc', host=args.host, port=args.port,
                              user=args.username, password=args.password,
                              cursor_factory=psycopg2.extras.RealDictCursor )
    dbcon.set_session( autocommit=True )
    cursor = dbcon.cursor()

    for tab in tables:
        _logger.info( f"Copying into {tab}" )
        cursor.execute( f"INSERT INTO {tab} SELECT * FROM temp_copy_{tab}" )

    _logger.info( "Updating elasticc2_brokermessage_brokerMessageId_seq" )
    cursor.execute( "SELECT setval( '\"elasticc2_brokermessage_brokerMessageId_seq\"',"
                    " (SELECT MAX(brokermessage_id)+1 FROM elasticc2_brokermessage) )" )

    _logger.info( "Updating elasticc2_brokerclassifier_classifierId_seq" )
    cursor.execute( "SELECT setval( '\"elasticc2_brokerclassifier_classifierId_seq\"',"
                    " (SELECT MAX(classifier_id)+1 FROM elasticc2_brokerclassifier) )" )

    _logger.info( "Deleting temporary tables" )
    for tab in tables:
        cursor.execute( f"DROP TABLE temp_copy_{tab}" )

    _logger.info( "Done" )

# **********************************************************************

if __name__ == "__main__":
    main()
