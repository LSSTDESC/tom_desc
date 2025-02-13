import json
import sys
import os
import os.path
import configparser
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql
import psycopg2.extras


class FASTDB(object):

    def __init__(self):

        fastdbservices = os.environ['HOME'] + '/.fastdbservices.ini'
        if os.path.exists(fastdbservices):
            config = configparser.ConfigParser()
            config.read(fastdbservices)
            self.db_name = config.get('fastdb', 'db_name',fallback='tom_desc')
            self.host = config.get('fastdb', 'host', fallback='tom-postgres-loadbalancer.desc-tom-buckley-dev.production.svc.spin.nersc.org')
            self.fastdb_reader = config.get('fastdb', 'fastdb_reader', fallback='fastdb_reader')
            self.fastdb_reader_passwd = config.get('fastdb', 'fastdb_reader_passwd', fallback='reader')
            self.fastdb_writer = config.get('fastdb', 'fastdb_writer', fallback=None)
            self.fastdb_writer_passwd = config.get('fastdb', 'fastdb_writer_passwd', fallback=None)


        else:
            print('Cannot find %s. Failed to get authentication' % fastdbservices)
            
    def submit_query(self, query, data=None):

        dbconn = None

        if data != None:
            if not isinstance( data, list ) and not isinstance( data, dict ):
                raise TypeError( "data must be either a list or a dict" )

        try:
            
            dbconn = psycopg2.connect( dbname=self.db_name, host=self.host,user=self.fastdb_reader, password=self.fastdb_reader_passwd,cursor_factory=psycopg2.extras.RealDictCursor )
            cursor = dbconn.cursor()

            if data != None:
                if isinstance( data, list ):
                    cursor.execute( query, (data,) )
                elif isinstance( data, dict ):
                    cursor.execute( query, data)
            else:
                cursor.execute(query)
                
            rows = cursor.fetchall()

            cursor.close()
            dbconn.close()

            return rows

        except Exception as ex:
            return( { 'status': 'error', 'error': str(ex) } )


    def insert_data(self, table, data):

        dbconn = None

        psycopg2.extras.register_uuid()

        if self.fastdb_writer_passwd is None:
            raise TypeError( "Error: No FASTDB writer password has been supplied" )

        if table is None:
            raise TypeError( "Error: no table name has been supplied" )
        
        if data != None:
            if not isinstance( data, list ) and not isinstance( data, dict ):
                raise TypeError( "Error: Data must be a list of dicts or a dict" )

        try:
            dbconn = psycopg2.connect( dbname=self.db_name, host=self.host,user=self.fastdb_writer, password=self.fastdb_writer_passwd,cursor_factory=psycopg2.extras.RealDictCursor )

            cursor = dbconn.cursor()

            if data != None:

                if isinstance( data, list ):
                    cols = data[0].keys()
                    count = len(data)
                elif isinstance( data, dict ):
                    cols = data.keys()
                    count = 1
                    
                query = sql.SQL("insert into {} ({}) values ({})").format(sql.Identifier(table),sql.SQL(', ').join(map(sql.Identifier, cols)),sql.SQL(', ').join(map(sql.Placeholder, cols)))

                execute_batch(cursor, query, data)
                
                cursor.close()
                dbconn.close()


                return( {'status': 'OK', 'rows stored': count} )
            
            else:
                 raise TypeError( "Insert failed: No data was supplied" )
                
        except Exception as ex:
            return( { 'status': 'error', 'error': str(ex) } )

    def update_data(self, table, data):

        dbconn = None
        if self.fastdb_writer_passwd is None:
            raise TypeError( "No FASTDB writer password has been supplied" )

        if table is None:
            raise TypeError( "Error: no table name has been supplied" )

        if data != None:
            if not isinstance( data, list ) and not isinstance( data, dict ):
                raise TypeError( "data must be a list of dicts or a dict" )

        try:
            dbconn = psycopg2.connect( dbname=self.db_name, host=self.host,user=self.fastdb_writer, password=self.fastdb_writer_passwd,cursor_factory=psycopg2.extras.RealDictCursor )

            cursor = dbconn.cursor()

            if data != None:

                if isinstance( data, list ):
                    cols = data[0].keys()
                    count = len(data)
                elif isinstance( data, dict ):
                    cols = data.keys()
                    count = 1

                if table == 'ds_to_pv_to_ss':
                    
                    query = "update ds_to_pv_to_ss set valid_flag = %(valid_flag)s,  update_time = %(update_time)s where dia_source = %(dia_source)s and processing_version = %(processing_version)s and snapshot_name = %(snapshot_name)s"

                elif table == 'dia_source':
                
                    query = "update dia_source set valid_flag = %(valid_flag)s, update_time = %(update_time)s where dia_source = %(dia_source)s and processing_version = %(processing_version)s"

                execute_batch(cursor, query, data)
                
                cursor.close()
                dbconn.close()

                return( {'status': 'OK', 'rows stored': count} )
            
            else:
                 raise TypeError( "Update failed: No data was supplied" )
                
        except Exception as ex:
            return( { 'status': 'error', 'error': str(ex) } )

