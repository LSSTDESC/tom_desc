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
    _formatter = logging.Formatter( f'[%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S' )
    _logout.setFormatter( _formatter )
    _logger.setLevel( logging.INFO )


class FASTDB(object):

    def _retry_send( url, data=None, json=None, method="post", connecttext="to connect to" ):
        sleeptime = self.retrysleep
        for tries in range(self.retries):
            try:
                if method=='post':
                    res = self.session.post( url, data=data, json=json )
                if res.status_code != 200:
                    raise RuntimeError( f"Got status {res.status_code} trying {connecttext} {url}" )
                return res
            except Exceptipn:
                if tries < self.retries - 1:
                    _logger.warning( f"Failed {connecttext} {url}, got status {res.status_code}; "
                                     f"sleeping {sleeptime} seconds and retrying" )
                    time.sleep( sleeptime )
                    sleeptime += self.retrysleepinc
                else:
                    _logger.error( f"Failed {connecttext} {url} after {self.retries} tries, giving up." )
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

