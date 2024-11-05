import datetime
import json
import os
import pathlib
import psycopg2
from psycopg2.extras import execute_batch
from time import sleep
import logging

from fastdb_dev.models import LastUpdateTime, ProcessingVersions, HostGalaxy, Snapshots, DiaObject, DiaSource, DiaForcedSource,SnapshotTags
from fastdb_dev.models import DStoPVtoSS, DFStoPVtoSS, BrokerClassifier, BrokerClassification

from django.http import HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.shortcuts import render
from django.contrib.auth.mixins import PermissionRequiredMixin, LoginRequiredMixin

import django.views

from django.db import connection

import secrets
from http import cookies
import uuid

from rest_framework.settings import api_settings

_logger = logging.getLogger("fastdb_queries")
_log_path = pathlib.Path( os.environ.get('LOGDIR',"."), "/logs" )
_log_path.mkdir(parents=True, exist_ok=True)
_logout = logging.FileHandler( _log_path / "fastdb_queries.log" )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.DEBUG )

class CreateDiaSource(LoginRequiredMixin, django.views.View):
    raise_exception = True

    def post( self, request, *args, **kwargs ):

        try:
            data = json.loads(request.body)

            if not 'insert' in data:
                raise ValueError( "POST data must include 'insert'" )

            if isinstance( data['insert'], list ):
                inserts = data['insert']
                count = len(inserts)
            elif not isinstance( data['insert'], dict ):
                raise TypeError( "insert must be either a list of dicts or a dict" )
 
            dia_source_data = []
    
            for d in inserts:

                ds_uuid = uuid.uuid4()
                ds = DiaSource(uuid=ds_uuid)
                ds.dia_source=d['dia_source']
                ds.filter_name = d['filter_name']
                ds.ra = d['ra']
                ds.decl = d['decl']
                ds.ps_flux = d['ps_flux']
                ds.ps_flux_err = d['ps_flux_err']
                ds.snr = d['snr']
                ds.mid_point_tai = d['mid_point_tai']
                ds.valid_flag = d['valid_flag']
                
                do = DiaObject.objects.get(dia_object=d['dia_object'])
                ds.dia_object = do
                ds.fake_id = do.fake_id
                ds.season = do.season
                ds.processing_version = d['processing_version']
                ds.broker_count = d['broker_count']
                
                dia_source_data.append(ds)

            objs = DiaSource.objects.bulk_create(dia_source_data)
            _logger.debug( f"Stored {len(objs)} rows." )
            status = '%d Rows of Data stored' % count
            status = '%d Rows of Data stored' % len(objs)
            return JsonResponse({'status':'ok', 'rows':len(objs)})

        except Exception as ex:
            _logger.exception( ex )
            return JsonResponse( { 'status': 'error', 'error': str(ex) } )


class CreateDSPVSS(LoginRequiredMixin, django.views.View):
    raise_exception = True

    def post( self, request, *args, **kwargs ):

        try:
            data = json.loads(request.body)
    
            if not 'insert' in data:
                raise ValueError( "POST data must include 'insert'" )

            if isinstance( data['insert'], list ):
                inserts = data['insert']
                count = len(inserts)
            elif not isinstance( data['insert'], dict ):
                raise TypeError( "insert must be either a list of dicts or a dict" )
 
            dspvss_data = []

            for d in inserts:

                dspvss = DStoPVtoSS(dia_source=d['dia_source'])
                dspvss.processing_version = d['processing_version']
                dspvss.snapshot_name = d['snapshot_name']
                dspvss.valid_flag = d['valid_flag']
                dspvss.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)
                
                dspvss_data.append(dspvss)

            objs = DStoPVtoSS.objects.bulk_create(dspvss_data)
            _logger.debug( f"Stored {len(objs)} rows." )
            status = '%d Rows of Data stored' % count
            status = '%d Rows of Data stored' % len(objs)
            return JsonResponse({'status':'ok', 'rows':len(objs)})

        except Exception as ex:
            _logger.exception( ex )
            return JsonResponse( { 'status': 'error', 'error': str(ex) } )



class UpdateDSPVSSValidFlag(LoginRequiredMixin, django.views.View):
    raise_exception = True

    def post( self, request, *args, **kwargs ):

        try:

            data = json.loads(request.body)
    
            if not 'update' in data:
                raise ValueError( "POST data must include 'update'" )

            if isinstance( data['update'], list ):
                updates = data['update']
                count = len(updates)
            elif not isinstance( data['update'], dict ):
                raise TypeError( "update must be either a list of dicts or a dict" )
 
            
            dspvss_data = []

            cursor = connection.cursor()

            for d in updates:

                print(d)
                dd = {}
                dd['valid_flag'] = d['valid_flag']
                dd['dia_source'] = d['dia_source']
                dd['processing_version'] = d['processing_version']
                dd['snapshot_name'] = d['snapshot_name']

                dspvss_data.append(dd)
        
                query = "update ds_to_pv_to_ss set valid_flag = %(valid_flag)s where dia_source = %(dia_source)s and processing_version = %(processing_version)s and snapshot_name = %(snapshot_name)s"

            execute_batch(cursor,query,dspvss_data)
            _logger.debug( f"Updated {count} rows." )
            return JsonResponse({'status':'ok', 'rows':count})

        except Exception as ex:
            _logger.exception( ex )
            return JsonResponse( { 'status': 'error', 'error': str(ex) } )

class UpdateDiaSourceValidFlag(LoginRequiredMixin, django.views.View):
    raise_exception = True

    def post( self, request, *args, **kwargs ):

        try:

            data = json.loads(request.body)
    
            if not 'update' in data:
                raise ValueError( "POST data must include 'update'" )

            if isinstance( data['update'], list ):
                updates = data['update']
                count = len(updates)
            elif not isinstance( data['update'], dict ):
                raise TypeError( "update must be either a list of dicts or a dict" )

            cursor = connection.cursor()

            dia_source_data = []
    
            for d in updates:

                dd = {}
                dd['valid_flag'] = d['valid_flag']
                dd['dia_source'] = d['dia_source']
                dd['processing_version'] = d['processing_version']
                
                dia_source_data.append(dd)

            query = "update dia_source set valid_flag = %(valid_flag)s where dia_source = %(dia_source)s and processing_version = %(processing_version)s"

            execute_batch(cursor,query,dia_source_data)
            _logger.debug( f"Updated {count} rows." )
            return JsonResponse({'status':'ok', 'rows':count})

        except Exception as ex:
            _logger.exception( ex )
            return JsonResponse( { 'status': 'error', 'error': str(ex) } )
