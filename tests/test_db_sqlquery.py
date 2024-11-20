import os
import sys
import re
import io
import pathlib
import datetime
import dateutil.parser
import pytz
import time
import subprocess

import pytest
import psycopg2
import psycopg2.extras
import pandas

sys.path.insert( 0, "/tom_desc" )

import db.models

# Use the last alertcycle fixture api_alertcycle_complete to get
# the database into a known state.  This is kind of slow, due to all the
# delays in the various polling servers, so it means a few min delay on
# the tests here actually running, but what can you do.  (At least it's
# a session fixture.)

class TestSQLWebInterface:

    def test_run_query( self, tomclient ):
        # Check single query without substitution
        res = tomclient.post( 'db/runsqlquery/', json={'query': "SELECT * FROM elasticc2_gentypeofclassid"} )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'ok'
        rows = data['rows']
        assert len(rows) == 54
        c2223 = [ r for r in rows if r['classid'] == 2223 ]
        assert len(c2223) == 5
        assert all( [ r['description'] == 'Ib/c' for r in c2223 ] )
        
        # Check single query with substitution
        res = tomclient.post( 'db/runsqlquery/',
                              json={ 'query': "SELECT * FROM elasticc2_gentypeofclassid WHERE classid=%(id)s",
                                     'subdict': { 'id': 2223 } } )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'ok'
        rows = data['rows']
        assert len(rows) == 5
        assert all( [ r['classid'] == 2223 for r in rows ] )

        # Check multiple query
        res = tomclient.post( 'db/runsqlquery/',
                              json= {
                                  'query': [
                                      "CREATE TEMP TABLE junk AS SELECT * FROM elasticc2_gentypeofclassid",
                                      "SELECT * FROM elasticc2_gentypeofclassid" ] } )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'ok'
        rows = data['rows']
        assert len(rows) == 54
        c2223 = [ r for r in rows if r['classid'] == 2223 ]
        assert len(c2223) == 5
        assert all( [ r['description'] == 'Ib/c' for r in c2223 ] )

        # Check multiple query with substitution
        res = tomclient.post( 'db/runsqlquery/',
                              json= {
                                  'query': [
                                      ( "CREATE TEMP TABLE junk AS SELECT * FROM elasticc2_gentypeofclassid "
                                        "WHERE classid=%(id)s" ),
                                      "SELECT * FROM pg_temp.junk" ] ,
                                  'subdict': [ { 'id': 2223 }, {} ] } )
                              
        data = res.json()
        assert data['status'] == 'ok'
        rows = data['rows']
        assert len(rows) == 5
        assert all( [ r['description'] == 'Ib/c' for r in rows ] )

        # Make sure we can't bobby tables
        res = tomclient.post( 'db/runsqlquery/', json={'query': "DROP TABLE elasticc2_gentypeofclassid"} )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'error'
        assert re.search( '^must be owner of table elasticc2_gentypeofclassid', data['error'] ) is not None

        # Check various error modes

        res = tomclient.post( 'db/runsqlquery/', json={'foo': 'bar'} )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'error'
        assert re.search( "^POST data must include 'query'", data['error'] ) is not None
        
        res = tomclient.post( 'db/runsqlquery/', json={'query': {'oops': 'oops'}} )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'error'
        assert re.search( "^query must be either a list of strings or a string", data['error'] ) is not None

        
        res = tomclient.post( 'db/runsqlquery/', json={ 'query': [ "SELECT * FROM elasticc2_gentypeofclassid",
                                                                   {'oops': 'oops'} ] } )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'error'
        assert re.search( "^queries must all be strings", data['error'] ) is not None

        res = tomclient.post( 'db/runsqlquery/',
                              json={
                                  'query': "SELECT * FROM elasticc2_gentypeofclassid WHERE classid=%(id)s",
                                  'subdict': [ { 'id': 2223 }, { 'foo': 'bar' } ] } )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'error'
        assert re.search( "^number of queries and subdicts must match", data['error'] )
                                      
        # TODO : more error modes

    # The next set of fixtures and tests probably violate some
    # assumption about how pytest is supposed to be used, but oh well.
    # The tests depend on some fixtures having been run and others not
    # having been run, in order.
        
    @pytest.fixture( scope='class' )
    def submit_long_query( self, tomclient, alert_cycle_complete ):
        res = tomclient.post( 'db/submitsqlquery/',
                              json= { 'query': 'SELECT * FROM elasticc2_brokermessage',
                                      'format': 'pandas' } );
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'ok'

        yield data['queryid']

        conn = psycopg2.connect( host='postgres', user='postgres', password='fragile', dbname='tom_desc' )
        cursor = conn.cursor()
        cursor.execute( 'DELETE FROM db_queryqueue' )

        direc = pathlib.Path( '/query_results' )
        for f in direc.glob( '*' ):
            f.unlink()

    @pytest.fixture( scope='class' )
    def run_long_query( self, submit_long_query ):
        res = subprocess.run( [ 'python', 'manage.py', 'long_query_runner', '-o' ], cwd='/tom_desc' )
        assert res.returncode == 0
        yield True

    @pytest.fixture( scope='class' )
    def purge_long_queries( self, run_long_query ):
        res = subprocess.run( [ 'python', 'manage.py', 'long_query_runner', '-p', '0.' ],
                              cwd='/tom_desc' )
        assert res.returncode == 0
        yield True
        
    def test_submit_long_query( self, tomclient, submit_long_query ):
        res = tomclient.post( 'db/runsqlquery/', json={'query': 'SELECT * FROM db_queryqueue'} )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'ok'
        rows = data['rows']
        assert any( [ str(r['queryid']) == submit_long_query for r in rows ] )

    def test_check_query_not_started( self, tomclient, submit_long_query ):
        res = tomclient.post( f'db/checksqlquery/{submit_long_query}/' )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'queued'
        assert data['queryid'] == submit_long_query
        assert data['queries'] == [ 'SELECT * FROM elasticc2_brokermessage' ]
        submittime = dateutil.parser.parse( data['submitted'] )
        if submittime.tzinfo is None:
            submittime = pytz.utc.localize( submittime )
        assert submittime < datetime.datetime.now( tz=datetime.timezone.utc )
        
    def test_query_result( self, tomclient, submit_long_query, run_long_query ):
        res = tomclient.post( f'db/checksqlquery/{submit_long_query}/' )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'finished'

        res = tomclient.post( f'db/getsqlqueryresults/{submit_long_query}/' )
        assert res.status_code == 200
        assert res.headers['Content-Type'] == 'application/octet-stream'
        bio = io.BytesIO( res.content )
        df = pandas.read_pickle( bio )
        assert len(df) == 1950

    def test_queries_purged( self, tomclient, submit_long_query, purge_long_queries ):
        direc = pathlib.Path( '/query_results' )
        assert len( [ i for i in  direc.glob( '*' ) ] ) == 0

        conn = psycopg2.connect( host='postgres', user='postgres', password='fragile', dbname='tom_desc' )
        cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )
        cursor.execute( 'SELECT COUNT(*) AS count FROM db_queryqueue' )
        rows = cursor.fetchall()
        assert rows[0]['count'] == 0
