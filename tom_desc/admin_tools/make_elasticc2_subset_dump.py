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
    parser = argparse.ArgumentParser( 'make_elasticc2_subset_dump', description='Create SQL dump file',
                                      formatter_class=ArgFormatter,
                                      epilog="""Utility for creating a small sample elasticc2 data set.

This will copy the specified number of objects from the elasticc2 tables
to a dump file.  Use restore_elasticc2_subset_dump.py to load those into
a local test environment.

This utility must be run somewhere where the TOM's postgres database is
visible.  For the regular deployment on NERSC, this means on a shell
running on Spin in the same namespace as the TOM you want to copy.  It
means something else for other deployments.

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
    parser.add_argument( '-H', '--host', default='tom-postgres', help='PostgreSQL host' )
    parser.add_argument( '-p', '--port', default=5432, type=int, help='PostgreSQL server port' )
    parser.add_argument( '-U', '--username', default='postgres', help='PostgreSQL username' )
    parser.add_argument( '-P', '--password', default='fragile', help='PostgreSQL password' )
    parser.add_argument( '-o', '--outfile', default='elasticc2_dump.sql',
                         help='Output pg_dump file' )
    parser.add_argument( '-n', '--nobs', default=1000, type=int, help='Number of rows from ppdbdiaobject to copy' )
    parser.add_argument( '-s', '--skip-table-copy', default=False, action='store_true',
                         help="Skip temp table creation and copy; only use this if you know what you're doing." )
    parser.add_argument( '--only-sent', default=False, action='store_true',
                         help=( "Only include objects with at least one alert that has been sent.  This will "
                                "slow down the process, and is unnecessary once ELAsTiCC2 is over." ) )
    args = parser.parse_args()

    tables = [ 'elasticc2_brokerclassifier',
               'elasticc2_brokermessage',
               'elasticc2_diaobjecttruth',
               'elasticc2_ppdbalert',
               'elasticc2_ppdbdiaforcedsource',
               'elasticc2_ppdbdiaobject',
               'elasticc2_ppdbdiasource',
              ]

    dbcon = psycopg2.connect( dbname='tom_desc', host=args.host, port=args.port,
                              user=args.username, password=args.password,
                              cursor_factory=psycopg2.extras.RealDictCursor )
    dbcon.set_session( autocommit=True )
    cursor = dbcon.cursor()

    if not args.skip_table_copy:
        _logger.info( "Creating temp_copy tables" )
        for tab in tables:
            cursor.execute( f'CREATE TABLE temp_copy_{tab} AS SELECT * FROM {tab} WITH NO DATA' )

        _logger.info( f"Copying {args.nobs} objects" )
        # ...hopefully the "int" type on the argument makes us safe from Bobby Tables
        if not args.only_sent:
            cursor.execute( f'INSERT INTO temp_copy_elasticc2_ppdbdiaobject '
                            f'SELECT * FROM elasticc2_ppdbdiaobject ORDER BY RANDOM() LIMIT {args.nobs}' )
        else:
            cursor.execute( f'INSERT INTO temp_copy_elasticc2_ppdbdiaobject '
                            f'  SELECT * FROM elasticc2_ppdbdiaobject'
                            f'  WHERE diaobject_id IN '
                            f'    ( SELECT DISTINCT ON (o.diaobject_id) o.diaobject_id '
                            f'      FROM elasticc2_ppdbdiaobject o '
                            f'      INNER JOIN elasticc2_ppdbalert a ON o.diaobject_id=a.diaobject_id '
                            f'        AND a.alertsenttimestamp IS NOT NULL ) '
                            f'  ORDER BY RANDOM() LIMIT {args.nobs}' )

        for subtab in [ 'ppdbdiasource', 'ppdbdiaforcedsource', 'ppdbalert', 'diaobjecttruth' ]:
            _logger.info( f"Copying {subtab} for the {args.nobs} objects..." )
            cursor.execute( f'INSERT INTO temp_copy_elasticc2_{subtab} '
                            f'SELECT * FROM elasticc2_{subtab} '
                            f'WHERE diaobject_id IN '
                            f'  (SELECT diaobject_id FROM temp_copy_elasticc2_ppdbdiaobject)' )
            cursor.execute( f"SELECT COUNT(*) AS count FROM temp_copy_elasticc2_{subtab}" )
            count = cursor.fetchone()['count']
            _logger.info( f"...copied {count} rows" )

        _logger.info( f"Copying brokerclassifier" )
        cursor.execute( 'INSERT INTO temp_copy_elasticc2_brokerclassifier '
                        'SELECT * FROM elasticc2_brokerclassifier' )

        _logger.info( f"Copying brokermessage" )
        cursor.execute( 'INSERT INTO temp_copy_elasticc2_brokermessage '
                        'SELECT * FROM elasticc2_brokermessage '
                        'WHERE alert_id IN '
                        '  (SELECT alert_id FROM temp_copy_elasticc2_ppdbalert)' )
        cursor.execute( "SELECT COUNT(*) AS count FROM temp_copy_elasticc2_brokermessage" )
        count = cursor.fetchone()['count']
        _logger.info( f"...copied {count} rows" )


    _logger.info( "Running pg_dump" )
    command = [ 'pg_dump', '-f', args.outfile, '-F', 'c', '-v',
                '-d', 'tom_desc', '-h', args.host, '-p', str(args.port), '-U', args.username ]
    for t in tables:
        command.extend( [ '-t', f'temp_copy_{t}' ] )
    res = subprocess.run( command, capture_output=False, env={'PGPASSWORD': args.password} )
    if res.returncode != 0:
        _logger.error( f"Subprocessed failed\ncommand: {res.args}" )
        sys.exit( 1 )

    _logger.info( "Dropping temp tables" )
    for tab in tables:
        cursor.execute( f'DROP TABLE temp_copy_{tab}' )

    _logger.info( "Done" )
    

# **********************************************************************

if __name__ == "__main__":
    main()
