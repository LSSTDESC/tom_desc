import os
import psycopg2

with open( "/secrets/postgres_ro_password" ) as ifp:
    ropasswd = ifp.readline().strip()

conn = psycopg2.connect( host=os.getenv('DB_HOST'),
                         port=os.getenv('DB_PORT'),
                         user=os.getenv('DB_USER'),
                         password=os.getenv('DB_PASS'),
                         dbname=os.getenv('DB_NAME') )
cursor = conn.cursor()
cursor.execute( 'CREATE ROLE postgres_ro PASSWORD %(pw)s LOGIN', { 'pw': ropasswd } )
cursor.execute( 'GRANT CONNECT ON DATABASE tom_desc TO postgres_ro' )
cursor.execute( 'GRANT USAGE ON SCHEMA PUBLIC TO postgres_ro' )
cursor.execute( 'GRANT SELECT ON ALL TABLES IN SCHEMA public TO postgres_ro' )
cursor.execute( 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO postgres_ro' )
conn.commit()

