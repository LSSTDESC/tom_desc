import requests
from requests import Request, Session
import json
import sys
import os
import os.path
import configparser

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

class FASTDB(object):

    def __init__(self):

        self.session = Session()
        fastdbservices = os.environ['HOME'] + '/.fastdbservices.ini'
        if os.path.exists(fastdbservices):
            config = configparser.ConfigParser()
            config.read(fastdbservices)
            username = config['fastdb']['user']
            password = config['fastdb']['passwd']
            req = self.session.get(LOGIN_URL)
            if req.status_code != 200:
                raise RuntimeError( f"Got status {req.status_code} from first attempt to connect to {LOGIN_URL}" )

            login_data = {'username':username, 'password':password, 'csrfmiddlewaretoken':  self.session.cookies['csrftoken']}
            req = self.session.post(LOGIN_URL, data=login_data)

            if req.status_code != 200:
                raise RuntimeError( f"Failed to log in; http status: {req.status_code}" )

            self.session.headers.update( { 'X-CSRFToken': self.session.cookies['csrftoken'] } )

        else:
            print('Cannot find %s. Failed to authenticate' % fastdbservices)
            
    
    def submit_long_query(self,q,format='csv'):

        queryid = None
        self.format = format
        
        query = {'query':q, 'format':self.format}

        res = self.session.post(SUBMIT_LONG_QUERY_URL, json=query)
        if res.status_code != 200:
            sys.stderr.write( f"ERROR, got status {res.status_code}\n" )
        elif res.headers['Content-Type'][:16] != 'application/json':
            sys.stderr.write( f"ERROR, didn't get json, got {res.headers['Content-Type']}" )
        else:
            data = res.json()
            if 'status' not in data.keys():
                sys.stderr.write( f"Unexpected response: {data}\n" )
            elif data['status'] == 'error':
                sys.stderr.write( f"Got an error: {data['error']}\n" )
            elif data['status'] != 'ok':
                sys.stderr.write( f"status is {data['status']} and I don't know how to cope.\n" )
            else:
                queryid = data['queryid']
                print( f"Submitted query {queryid}" )
                return queryid
 
        

    def submit_short_query(self, query, subdict=None ):

        
        if subdict is not None:
            query = {'query':query,'subdict':subdict}
        else:
            query = {'query':query}

        res = self.session.post(SUBMIT_SHORT_QUERY_URL, json=query)
        if res.status_code != 200:
            sys.stderr.write( f"ERROR, got status {res.status_code}\n" )
        elif res.headers['Content-Type'][:16] != 'application/json':
            sys.stderr.write( f"ERROR, didn't get json, got {res.headers['Content-Type']}" )
        else:
            data = res.json()
            if 'status' not in data.keys():
                sys.stderr.write( f"Unexpected response: {data}\n" )
            elif data['status'] == 'error':
                sys.stderr.write( f"Got an error: {data['error']}\n" )
            elif data['status'] != 'ok':
                sys.stderr.write( f"status is {data['status']} and I don't know how to cope.\n" )
            else:
                return data['rows']
            

    def check_long_sql_query(self, queryid):


        url = CHECK_LONG_SQL_QUERY_URL + queryid +'/'
        r = self.session.post(url)
        print(r.status_code)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            sys.stderr.write( f"Got an error: {r.status_code}\n" )


    def get_long_sql_query(self, queryid):

        url = GET_LONG_SQL_QUERY_URL + queryid +'/'
        r = self.session.post(url)
        print(r.status_code)
        ctype = r.headers['content-type']
        if r.status_code == requests.codes.ok:
            if ctype == 'text/csv':
                return r.text
            elif ctype == 'application/octet-stream':
                return r.content
        else:
            sys.stderr.write( f"Got an error: {r.status_code}\n" )

            
    def data_insert(self,table,data):

        query = {'insert':data}
        if table == 'dia_source':
            r = self.session.post(DIA_SOURCE_URL, json=query)
        if table == 'ds_to_pv_to_ss':
            r = self.session.post(DS_PV_SS_URL, json=query)
            
        return r.json()

    def data_update(self,table,data):

        query = {'update':data}
        if table == 'dia_source':
            r = self.session.post(UPDATE_DIA_SOURCE_URL, json=query)
        if table == 'ds_to_pv_to_ss':
            r = self.session.post(UPDATE_DS_PV_SS_URL, json=query)
            
        return r.json()
        
