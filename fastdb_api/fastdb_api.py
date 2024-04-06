import requests
from requests import Request, Session
import json
import os
import os.path
import configparser

URL =  'https://desc-tom.lbl.gov/fastdb_dev/'
LOGIN_URL = URL + 'acquire_token'
TOKEN_URL = URL + 'api-token-auth/'
RAW_QUERY_LONG_URL = URL + 'raw_query_long'
RAW_QUERY_SHORT_URL = URL + 'raw_query_short'
DIA_OBJECTS_URL = URL + 'get_dia_objects'
DIA_SOURCE_URL = URL + 'store_dia_source_data'
DS_PV_SS_URL = URL + 'store_ds_pv_ss_data'
UPDATE_DS_PV_SS_URL = URL + 'update_ds_pv_ss_valid_flag'
UPDATE_DIA_SOURCE_URL = URL + 'update_dia_source_valid_flag'
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
            print(self.csrf_token)

            login_data = {'username':username, 'password':password, 'csrfmiddlewaretoken': self.csrf_token}
            req = session.post(TOKEN_URL, data=login_data)
            token = json.loads(req.text).get('token')

            if token:
                token = f"Token {token}"
                self.headers = {"Authorization": token}
                print(self.headers)
        else:
            print('Cannot find %s. Failed to authenticate' % fastdbservices)
            
    
    def raw_query_long(self,q):

        query = json.dumps([{'csrfmiddlewaretoken': self.csrf_token},
                            {'query':q},])
        r = requests.post(RAW_QUERY_LONG_URL, json=query, headers=self.headers)
        if r.status_code != status.HTTP_403_FORBIDDEN:
            print("query started")


    def raw_query_short(self,q):

        query = json.dumps([{'csrfmiddlewaretoken': self.csrf_token},{'query':q},])
        r = requests.post(RAW_QUERY_SHORT_URL, json=query, headers=self.headers)
        print(r.status_code)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            print("Failed to authenticate")
            


class FASTDBDataStore(object):

    def __init__(self):

        session = Session()
        fastdbservices = os.environ['HOME'] + '/.fastdbservices.ini'
        print(fastdbservices)
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

        print(data)
        query = json.dumps([{'csrfmiddlewaretoken': self.csrf_token},
                        {'query':data},
        ])
        print(query)
        if table == 'dia_source':
            r = requests.post(DIA_SOURCE_URL, json=query, headers=self.headers)
        if table == 'ds_to_pv_to_ss':
            r = requests.post(DS_PV_SS_URL, json=query, headers=self.headers)
            
        return r.json()

    def data_update(self,table,data):

        print(data)
        query = json.dumps([{'csrfmiddlewaretoken': self.csrf_token},
                        {'query':data},
        ])
        print(query)
        if table == 'dia_source':
            r = requests.post(UPDATE_DIA_SOURCE_URL, json=query, headers=self.headers)
        if table == 'ds_to_pv_to_ss':
            r = requests.post(UPDATE_DS_PV_SS_URL, json=query, headers=self.headers)
            
        return r.json()
        
