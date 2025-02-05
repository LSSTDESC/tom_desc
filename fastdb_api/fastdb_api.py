import requests
import time
from requests import Request, Session
import json
import sys
import os
import os.path
import configparser
import logging

URL =  'https://desc-tom-fastdb-dev.lbl.gov/'
#URL = 'http://tom-app.desc-tom-buckley-dev.production.svc.spin.nersc.org/fastdb_dev/'
LOGIN_URL = URL + 'accounts/login/'
SUBMIT_LONG_QUERY_URL = URL + 'db/submitsqlquery/'
SUBMIT_SHORT_QUERY_URL = URL + 'db/runsqlquery/'
CHECK_LONG_SQL_QUERY_URL = URL + 'db/checksqlquery/'
GET_LONG_SQL_QUERY_URL = URL + 'db/getsqlqueryresults/'
DIA_SOURCE_URL = URL + 'fastdb_dev/create_dia_source'
DS_PV_SS_URL = URL + 'fastdb_dev/create_ds_pv_ss'
UPDATE_DS_PV_SS_URL = URL + 'fastdb_dev/update_ds_pv_ss_valid_flag'
UPDATE_DIA_SOURCE_URL = URL + 'fastdb_dev/update_dia_source_valid_flag'

_logger = logging.getLogger( "FastDB_API" )
if not _logger.hasHandlers():
    _logout = logging.StreamHandler( sys.stderr )
    _logger.addHandler( _logout )
    _formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S' )
    _logout.setFormatter( _formatter )
    _logger.setLevel( logging.INFO )
    _logger.propagate = False


class FASTDB(object):

    def _retry_send( self, url, data=None, json=None, method="post", connecttext="to connect to" ):
        sleeptime = self.retrysleep
        previous_fail = False
        for tries in range(self.retries):
            try:
                t0 = time.perf_counter()
                if method=='post':
                    res = self.session.post( url, data=data, json=json )
                elif method == 'get':
                    res = self.session.get( url, data=data, json=json )
                else:
                    raise ValueError( f"Unknown method {method}, must be get or post" )
                if res.status_code != 200:
                    raise RuntimeError( f"Got status {res.status_code} trying {connecttext} {url}" )
                if previous_fail:
                    dt = time.perf_counter() - t0
                    _logger.info( f"Connection to {url} succeeded in {dt:.3f} seconds." )
                return res
            except Exception:
                previous_fail = True
                dt = time.perf_counter() - t0
                if tries < self.retries - 1:
                    _logger.warning( f"Failed {connecttext} {url} after {dt:.3f} seconds, "
                                     f"got status {res.status_code}; "
                                     f"sleeping {sleeptime} seconds and retrying" )
                    time.sleep( sleeptime )
                    sleeptime += self.retrysleepinc
                else:
                    _logger.error( f"Failed {connecttext} {url} after {self.retries} tries, "
                                   f"last try took {dt:.3f} seconds.  Giving up." )
                    if res.status_code == 500:
                        _logger.error( f"Body of 500 return: {res.text}" )
                    raise


    def __init__(self, retries=5, retrysleep=1, retrysleepinc=2 ):
        """Connect to the FAST DB SQL WEB API.

        Paramters
        ---------
           retries : int, default 5
             If the any server connection fails because of network disconnections,
             retry this many times before giving up.

           retrysleep : int, default 1
             Sleep this many seconds after the first connection try failed.

           retrysleepinc : int, default 2
             Increase the sleep interval by this many seconds after each
             connection try fails.

        """

        self.retries = retries
        self.retrysleep = retrysleep
        self.retrysleepinc = retrysleepinc

        self.session = Session()
        fastdbservices = os.environ['HOME'] + '/.fastdbservices.ini'
        if os.path.exists(fastdbservices):
            config = configparser.ConfigParser()
            config.read(fastdbservices)
            username = config['fastdb']['user']
            password = config['fastdb']['passwd']

            req = self._retry_send( LOGIN_URL, method="get" )
            login_data = { 'username':username, 'password':password,
                           'csrfmiddlewaretoken':  self.session.cookies['csrftoken']}

            req = self._retry_send( LOGIN_URL, data=login_data, connecttext="to log in to" )
            self.session.headers.update( { 'X-CSRFToken': self.session.cookies['csrftoken'] } )

        else:
            print( f'Cannot find {fastdbservices}. Failed to authenticate' )


    def synchronous_long_query( self, query, subdict=None, format='csv', checkeach=30, maxwait=3600 ):
        queryid = None

        params = { 'query': query, 'format': format }
        if subdict is not None:
            params['subdict'] = subdict
        res = self._retry_send( SUBMIT_LONG_QUERY_URL, json=params )
        if res.headers['Content-Type'][:16] != 'application/json':
            raise TypeError( f"ERROR, didn't get json, got {res.headers['Content-Type']}" )
        else:
            data = res.json()
            if 'status' not in data.keys():
                raise ValueError( f"Unexpected response: {data}\n" )
            elif data['status'] == 'error':
                raise RuntimeError( f"Got an error: {data['error']}\n" )
            elif data['status'] != 'ok':
                raise RuntimeError( f"status is {data['status']} and I don't know how to cope.\n" )
            else:
                queryid = data['queryid']
                _logger.info( f"Submitted query {queryid}" )

        t0 = time.perf_counter()
        done = False
        totwait = 0
        while ( not done) and ( totwait < maxwait ):
            time.sleep( checkeach )

            res = self._retry_send( CHECK_LONG_SQL_QUERY_URL + queryid + '/' )
            if res.headers['Content-Type'][:16] != 'application/json':
                sys.stderr.write( f"ERROR, didn't get json, got {res.headers['Content-Type']}" )
            else:
                data = res.json()
                if 'status' not in data.keys():
                    _logger.error( f"Unexpected response: {data}\n" )
                elif data['status'] == 'error':
                    _logger.error( f"Got an error: {data['error']}\n" )

                _logger.info( f"Query status is {data['status']}")
                if ( data['status'] == 'finished' ):
                    done = True

            totwait = time.perf_counter() - t0

        res = self._retry_send( GET_LONG_SQL_QUERY_URL + queryid + '/' )
        ctype = res.headers['Content-Type']
        _logger.info( f"Long query results fetch got a {ctype} that is {len(res.content)} bytes long "
                      f"(res.text is {len(res.text)} characters long)." )
        if ctype[:8] == 'text/csv':
            return res.text
        elif ctype == 'application/octet-stream':
            return res.content
        else:
            raise TypeError( f"Got unknown type {ctype}, expected text/csv or application/octet-stream" )


    def submit_long_query(self, q, format='csv'):

        queryid = None
        self.format = format

        query = {'query':q, 'format':self.format}
        res = self._retry_send( SUBMIT_LONG_QUERY_URL, json=query )

        if res.headers['Content-Type'][:16] != 'application/json':
            raise TypeError( f"ERROR, didn't get json, got {res.headers['Content-Type']}" )
        else:
            data = res.json()
            if 'status' not in data.keys():
                raise ValueError( f"Unexpected response: {data}\n" )
            elif data['status'] == 'error':
                raise RuntimeError( f"Got an error: {data['error']}\n" )
            elif data['status'] != 'ok':
                raise RuntimeError( f"status is {data['status']} and I don't know how to cope.\n" )
            else:
                queryid = data['queryid']
                _logger.info( f"Submitted query {queryid}" )
                return queryid


    def submit_short_query(self, query, subdict=None ):


        if subdict is not None:
            query = {'query':query,'subdict':subdict}
        else:
            query = {'query':query}

        res = self._retry_send( SUBMIT_SHORT_QUERY_URL, json=query )

        if res.headers['Content-Type'][:16] != 'application/json':
            raise TypeError( f"ERROR, didn't get json, got {res.headers['Content-Type']}" )
        else:
            data = res.json()
            if 'status' not in data.keys():
                raise ValueError( f"Unexpected response: {data}\n" )
            elif data['status'] == 'error':
                raise RuntimeError( f"Got an error: {data['error']}\n" )
            elif data['status'] != 'ok':
                raise RuntimeError( f"status is {data['status']} and I don't know how to cope.\n" )
            else:
                return data['rows']


    def check_long_sql_query(self, queryid):

        url = CHECK_LONG_SQL_QUERY_URL + queryid +'/'
        r = self._retry_send( url )

        if r.headers['Content-Type'][:16] != 'application/json':
            raise TypeError( f"ERROR, didn't get json, got {res.headers['Content-Type']}" )
        else:
            return r.json()


    def get_long_sql_query(self, queryid):

        url = GET_LONG_SQL_QUERY_URL + queryid +'/'
        r = self._retry_send( url )

        ctype = r.headers['content-type']
        if ctype == 'text/csv; charset=utf-8':
            return r.text
        elif ctype == 'application/octet-stream':
            return r.content
        else:
            raise TypeError( f"Got unknown type {ctype}, expected 'text/csv; charset=utf8' "
                             f"or 'application/octet-stream'" )


    def insert_data(self,table,data):

        query = {'insert':data}
        if table == 'dia_source':
            r = self._retry_send( DIA_SOURCE_URL, json=query )
        if table == 'ds_to_pv_to_ss':
            r = self._retry_send( DS_PV_SS_URL, json=query )

        return r.json()

    def update_data(self,table,data):

        query = {'update':data}
        if table == 'dia_source':
            r = self._retry_send( UPDATE_DIA_SOURCE_URL, json=query )
        if table == 'ds_to_pv_to_ss':
            r = self._retry_send( UPDATE_DS_PV_SS_URL, json=query )

        return r.json()

