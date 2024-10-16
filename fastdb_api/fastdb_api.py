import requests
from requests import Request, Session
import json
import sys
import os
import os.path
import configparser

URL =  'https://desc-tom-fastdb-dev.lbl.gov/fastdb_dev/'
#URL = 'http://tom-app.desc-tom-buckley-dev.production.svc.spin.nersc.org/fastdb_dev/'
LOGIN_URL = URL + 'acquire_token'
TOKEN_URL = URL + 'api-token-auth/'
SUBMIT_LONG_QUERY_URL = URL + 'submit_long_query'
SUBMIT_SHORT_QUERY_URL = URL + 'submit_short_query'
CHECK_LONG_SQL_QUERY_URL = URL + 'check_long_sql_query'
GET_LONG_SQL_QUERY_URL = URL + 'get_long_sql_query'
DIA_SOURCE_URL = URL + 'bulk_create_dia_source_data'
DS_PV_SS_URL = URL + 'bulk_create_ds_pv_ss_data'
UPDATE_DS_PV_SS_URL = URL + 'bulk_update_ds_pv_ss_valid_flag'
UPDATE_DIA_SOURCE_URL = URL + 'bulk_update_dia_source_valid_flag'
AUTHENTICATE_USER = URL + 'authenticate_user'

class FASTDBDataAccess(object):

    def __init__(self):

        session = Session()
        fastdbservices = os.environ['HOME'] + '/.fastdbservices.ini'
        if os.path.exists(fastdbservices):
            config = configparser.ConfigParser()
            config.read(fastdbservices)
            username = config['fastdb']['user']
            password = config['fastdb']['passwd']
            req = session.get(LOGIN_URL)
            self.csrf_token = req.cookies['csrftoken']

            login_data = {'username':username, 'password':password, 'csrfmiddlewaretoken': self.csrf_token}
            req = session.post(TOKEN_URL, data=login_data)
            token = json.loads(req.text).get('token')

            if token:
                token = f"Token {token}"
                self.headers = {"Authorization": token}
        else:
            print('Cannot find %s. Failed to authenticate' % fastdbservices)
            
    
    def submit_long_query(self,q,format='csv'):

        queryid = None
        self.format = format
        
        query = json.dumps([{'csrfmiddlewaretoken': self.csrf_token},{'query':q},{'format':self.format}])
        print(query)
        if isinstance(query,str):
            print('string')

        res = requests.post(SUBMIT_LONG_QUERY_URL, json=query, headers=self.headers)
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
            query = json.dumps([{'csrfmiddlewaretoken': self.csrf_token},{'query':query},{'subdict':subdict},])
        else:
            query = json.dumps([{'csrfmiddlewaretoken': self.csrf_token},{'query':query},])
        res = requests.post(SUBMIT_SHORT_QUERY_URL, json=query, headers=self.headers)
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
                return data['data']
            

    def check_long_sql_query(self, queryid):

        query = json.dumps([{'csrfmiddlewaretoken': self.csrf_token},{'queryid':queryid},])

        r = requests.post(CHECK_LONG_SQL_QUERY_URL, json=query, headers=self.headers)
        print(r.status_code)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            print("Failed to authenticate")


    def get_long_sql_query(self, queryid):

        query = json.dumps([{'csrfmiddlewaretoken': self.csrf_token},{'queryid':queryid},])

        r = requests.post(GET_LONG_SQL_QUERY_URL, json=query, headers=self.headers)
        print(r.status_code)
        if r.status_code == requests.codes.ok:
            return r.text
        else:
            print("Failed to authenticate")
            
class FASTDBDataStore(object):

    def __init__(self):

        session = Session()
        fastdbservices = os.environ['HOME'] + '/.fastdbservices.ini'
        if os.path.exists(fastdbservices):
            config = configparser.ConfigParser()
            config.read(fastdbservices)
            username = config['fastdb']['user']
            password = config['fastdb']['passwd']
            req = session.get(LOGIN_URL)
            self.csrf_token = req.cookies['csrftoken']

            login_data = {'username':username, 'password':password, 'csrfmiddlewaretoken': self.csrf_token}
            req = session.post(TOKEN_URL, data=login_data)
            token = json.loads(req.text).get('token')
            
            if token:
                token = f"Token {token}"
                self.headers = {"Authorization": token}
        else:
            print('Cannot find %s. Failed to authenticate' % fastdbservices)


    def data_insert(self,table,data):

        query = json.dumps([{'csrfmiddlewaretoken': self.csrf_token},
                        {'insert':data},
        ])
        if table == 'dia_source':
            r = requests.post(DIA_SOURCE_URL, json=query, headers=self.headers)
        if table == 'ds_to_pv_to_ss':
            r = requests.post(DS_PV_SS_URL, json=query, headers=self.headers)
            
        return r.json()

    def data_update(self,table,data):

        query = json.dumps([{'csrfmiddlewaretoken': self.csrf_token},
                        {'update':data},
        ])
        if table == 'dia_source':
            r = requests.post(UPDATE_DIA_SOURCE_URL, json=query, headers=self.headers)
        if table == 'ds_to_pv_to_ss':
            r = requests.post(UPDATE_DS_PV_SS_URL, json=query, headers=self.headers)
            
        return r.json()
        
