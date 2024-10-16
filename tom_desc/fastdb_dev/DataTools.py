import datetime
import json
import os
import psycopg2
from psycopg2.extras import execute_batch
from time import sleep
import logging

from fastdb_dev.models import LastUpdateTime, ProcessingVersions, HostGalaxy, Snapshots, DiaObject, DiaSource, DiaForcedSource,SnapshotTags
from fastdb_dev.models import DStoPVtoSS, DFStoPVtoSS, BrokerClassifier, BrokerClassification
from db.models import QueryQueue

from fastdb_dev.serializers import DiaSourceSerializer
from fastdb_dev.serializers import DiaObjectSerializer
from rest_framework.renderers import JSONRenderer
from rest_framework.parsers import JSONParser
from rest_framework.decorators import api_view
from rest_framework.decorators import authentication_classes
from rest_framework.decorators import permission_classes
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.decorators import parser_classes
from rest_framework import permissions
from rest_framework import status
from rest_framework.authentication import SessionAuthentication
from rest_framework.authentication import TokenAuthentication
from rest_framework import generics

from django.views.decorators.csrf import csrf_exempt
from django.db.models import Q
from django.contrib.auth.decorators import login_required
from django.contrib.auth import authenticate, login, mixins
from django.contrib.auth.models import User
from django.core import serializers
from django.shortcuts import render
from django.http import HttpResponse,JsonResponse
from django.template import RequestContext
from django.middleware.csrf import get_token

from django.db import connection

import secrets
from http import cookies
import uuid

from rest_framework.settings import api_settings

_logger = logging.getLogger("fastdb_queries")
_logout = logging.FileHandler("/code/logs/queries.log")
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.DEBUG )


@api_view(['GET'])
def acquire_token(request):
    token = get_token(request)
    data={'csrftoken':token}
    return Response(data)

@api_view(['POST'])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])
def authenticate_user(request):

    print(request.user)
    if not request.user.is_authenticated:
        return Response(serializer.error, status=status.HTTP_403_FORBIDDEN)
    else:
        status = 'User %s authenticated'
        return Response(status, status=status.HTTP_200_OK)

def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    print(columns)
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

def _extract_queries( request ):

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)

    q = next((i for i,d in enumerate(data) if 'query' in d), None)
    if not q:
        raise ValueError( "POST data must include 'query'" )
    else:
        query_data = data[1]

    if isinstance( query_data['query'], list ):
        queries = query_data['query']
    elif not isinstance( query_data['query'], str ):
        raise TypeError( "query must be either a list of strings or a string" )
    else:
        queries = [ query_data['query'] ]
    if not all( [ isinstance(q, str) for q in queries  ] ):
        raise TypeError( "queries must all be strings" )

    s = next((s for s,d in enumerate(data) if 'subdict' in d), None)

    if not s:
        subdicts = [ {} for i in range(len(queries)) ]
    else:
        subdict_data = data[2]
        if isinstance( subdict_data['subdict'], list ):
            subdicts = subdict_data['subdict']
        elif not isinstance( subdict_data['subdict'], dict ):
            raise TypeError( "query must be either a list of dicts or a dict" )
        else:
            subdicts = [ subdict_data['subdict'] ]
        if len(subdicts) != len(queries):
            raise ValueError( "number of queries and subdicts must match" )
        if not all( [ isinstance(s, dict) for s in subdicts ] ):
            raise TypeError( "subdicts must all be dicts" )

    # Have to convert lists to tuples in the substitution dictionaries
    for subdict in subdicts:
        for key in subdict.keys():
            if isinstance( subdict[key], list ):
                subdict[key] = tuple( subdict[key] )

    return queries, subdicts, data


@api_view(['POST'])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])
def SubmitLongQuery(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=status.HTTP_403_FORBIDDEN)

    try:
        queries, subdicts, data = _extract_queries( request )

        format = 'csv'
        if 'format' in data:
            format = data[ 'format' ]
            if format not in [ 'csv', 'pandas', 'numpy' ]:
                raise ValueError( f"Unknown format {format}" )

        queryid = uuid.uuid4()
        newqueue = QueryQueue.objects.create( queryid=queryid, submitted=datetime.datetime.now(),queries=queries, subdicts=subdicts, format=format )

        return Response( { 'status': 'ok', 'queryid': str(queryid) } )

    except Exception as ex:
        _logger.exception( ex )
        return Response( { 'status': 'error', 'error': str(ex) } )



@api_view(['POST'])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])
def SubmitShortQuery(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=status.HTTP_403_FORBIDDEN)

    dbconn = None
    try:
        dbuser = "postgres_ro"
        pwfile = "/secrets/postgres_ro_password"
        with open( pwfile ) as ifp:
            password = ifp.readline().strip()
            
        queries, subdicts, data = _extract_queries( request )

        dbconn = psycopg2.connect( dbname=os.getenv('DB_NAME'), host=os.getenv('DB_HOST'),
                                   user=dbuser, password=password,
                                   cursor_factory=psycopg2.extras.RealDictCursor )
        cursor = dbconn.cursor()

        _logger.debug( "Starting query sequence" )
        _logger.debug( "Starting query sequence" )
        _logger.debug( queries )

        for query, subdict in zip( queries, subdicts ):
            _logger.debug( f'Query is {query}, subdict is {subdict}, dbuser is {dbuser}' )
            cursor.execute( query, subdict )
            _logger.debug( 'Query done' )
        _logger.debug( "Fetching" )
        rows = cursor.fetchall()

        _logger.debug( f"Returning {len(rows)} rows from query sequence." )
        return Response( {'status': 'ok', 'data':rows} )


    except Exception as ex:
        _logger.exception( ex )
        return Response( { 'status': 'error', 'error': str(ex) } )
    
    finally:
        if dbconn is not None:
            dbconn.rollback()
            dbconn.close()


@api_view(['POST'])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])

def CheckLongSQLQuery(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=status.HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)

    q = next((i for i,d in enumerate(data) if 'queryid' in d), None)

    if not q:
        raise ValueError( "POST data must include 'queryid'" )
    else:
        query_data = data[1]
        queryid = query_data['queryid']
        
    try:
        queueobj = QueryQueue.objects.filter( queryid=queryid )
        if len( queueobj ) == 0:
            return Response( { 'status': 'error',
                                   'error': f"Unknown query {queryid}" } )
        queueobj = queueobj[0]

        response = { 'queryid': queueobj.queryid,
                     'queries': queueobj.queries,
                     'subdicts': queueobj.subdicts,
                     'submitted': queueobj.submitted.isoformat() }

        if queueobj.error:
            response.update( { 'status': 'error',
                               'finished': queueobj.finished.isoformat(),
                               'error': queueobj.errortext } )
            if queueobj.started is not None:
                response['started'] = queueobj.started.isoformat()

        elif queueobj.finished is not None:
            response.update( { 'status': 'finished',
                               'started': queueobj.started.isoformat(),
                               'finished': queueobj.finished.isoformat() } )

        elif queueobj.started is not None:
            response.update( { 'status': 'started',
                               'started': queueobj.started.isoformat() } )

        else:
            response.update( { 'status': 'queued' } )

        return Response( response )

    except Exception as ex:
        _logger.exception( ex )
        return Response( { 'status': 'error', 'error': str(ex) } )

@api_view(['POST'])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])

def GetLongSQLQuery(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=status.HTTP_403_FORBIDDEN)


    raw_data = json.loads(request.body)
    data = json.loads(raw_data)

    q = next((i for i,d in enumerate(data) if 'queryid' in d), None)

    if not q:
        raise ValueError( "POST data must include 'queryid'" )
    else:
        query_data = data[1]
        queryid = query_data['queryid']

        try:
            queueobj = QueryQueue.objects.filter( queryid=queryid )
            if len( queueobj ) == 0:
                return HttpResponse( f"Unknown query {queueobj.queryid}",
                                     content_type="text/plain; charset=utf-8",
                                     status=500 )

            queueobj = queueobj[0]
            if queueobj.error:
                return HttpResponse( f"Query errored out: {queueobj.errortext}",
                                     content_type="text/plain; charset=utf-8",
                                     status=500 )

            if queueobj.finished is None:
                if queueobj.started is None:
                    return HttpResponse( f"Query {queryid} hasn't started yet",
                                         content_type="text/plain; charset=utf-8",
                                         status=500 )
                else:
                    return HttpResponse( f"Query {queryid} hasn't finished yet",
                                         content_type="text/plain; charset=utf-8",
                                         status=500 )


            if ( queueobj.format == "numpy" ) or ( queueobj.format == "pandas" ):
                with open( f"/query_results/{str(queueobj.queryid)}", "rb" ) as ifp:
                    return HttpResponse( ifp.read(), content_type='application/octet-stream' )
            elif ( queueobj.format == "csv" ):
                with open( f"/query_results/{str(queueobj.queryid)}", "r" ) as ifp:
                    return HttpResponse( ifp.read(), content_type='text/csv; charset=utf-8' )
            else:
                return HttpResponse( f"Query is finished, but results are in an unknown format "
                                     f"{queueobj.format}", content_type="text/plain; charset=utf-8",
                                     status=500 )
        except Exception as ex:
            _logger.exception( ex )
            return HttpResponse( str(ex), content_type="text/plain; charset=utf-8", status=500 )

    
@api_view(['POST'])
@parser_classes([JSONParser])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])

def store_dia_source_data(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)

    for d in data:
        print(d)
        if 'query' in d:
            dia_source_data = d['query']

    count = len(dia_source_data)

    for data in dia_source_data:
        ds_uuid = uuid.uuid4()
        ds = DiaSource(uuid=ds_uuid)
        ds.dia_source=data['dia_source']
        ds.filter_name = data['filter_name']
        ds.ra = data['ra']
        ds.decl = data['decl']
        ds.ps_flux = data['ps_flux']
        ds.ps_flux_err = data['ps_flux_err']
        ds.snr = data['snr']
        ds.mid_point_tai = data['mid_point_tai']
        ds.valid_flag = data['valid_flag']

        do = DiaObject.objects.get(dia_object=data['dia_object'])
        ds.dia_object = do
        ds.fake_id = do.fake_id
        ds.season = do.season
        ds.processing_version = data['processing_version']
        ds.broker_count = data['broker_count']
        
        ds.save()

    status = '%d Rows of Data stored' % count
    return Response(status)
                
@api_view(['POST'])
def store_ds_pv_ss_data(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)
    for d in data:
        if 'query' in d:
             ds_pv_ss_data = d['query']
    count = len(ds_pv_ss_data)

    for data in ds_pv_ss_data:

        dspvss = DStoPVtoSS(dia_source=data['dia_source'])
        dspvss.processing_version = data['processing_version']
        dspvss.snapshot_name = data['snapshot_name']
        dspvss.valid_flag = data['valid_flag']
        dspvss.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)
    
        dspvss.save()

        status = '%d Rows of Data stored' % count
    return Response(status)

@api_view(['POST'])
def update_ds_pv_ss_valid_flag(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)
    for d in data:
        if 'query' in d:
             ds_pv_ss_data = d['query']
    count = len(ds_pv_ss_data)

    cursor = connection.cursor()

    for data in ds_pv_ss_data:

        query = "update ds_to_pv_to_ss set valid_flag = %d where dia_source = %d and processing_version = '%s' and snapshot_name = '%s'" % (data['valid_flag'],data['dia_source'],data['processing_version'],data['snapshot_name'])
        cursor.execute(query)

        status = '%d Rows of Data stored' % count
    return Response(status)

@api_view(['POST'])
def update_dia_source_valid_flag(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)
    for d in data:
        if 'query' in d:
             dia_source_data = d['query']
    count = len(dia_source_data)

    cursor = connection.cursor()

    for data in dia_source_data:

        query = "update dia_source set valid_flag = %d where dia_source = %d and processing_version = '%s'" % (data['valid_flag'],data['dia_source'],data['processing_version'])
        cursor.execute(query)

        status = '%d Rows of Data stored' % count
    return Response(status)

@api_view(['POST'])
@parser_classes([JSONParser])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])

def bulk_create_dia_source_data(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)

    bc = next((i for i,d in enumerate(data) if 'insert' in d), None)

    if not bc:
        raise ValueError( "POST data must include 'insert'" )
    else:
        bc_data = data[1]['insert']
        count = len(bc_data)

    dia_source_data = []
    
    for data in bc_data:

        ds_uuid = uuid.uuid4()
        ds = DiaSource(uuid=ds_uuid)
        ds.dia_source=data['dia_source']
        ds.filter_name = data['filter_name']
        ds.ra = data['ra']
        ds.decl = data['decl']
        ds.ps_flux = data['ps_flux']
        ds.ps_flux_err = data['ps_flux_err']
        ds.snr = data['snr']
        ds.mid_point_tai = data['mid_point_tai']
        ds.valid_flag = data['valid_flag']

        do = DiaObject.objects.get(dia_object=data['dia_object'])
        ds.dia_object = do
        ds.fake_id = do.fake_id
        ds.season = do.season
        ds.processing_version = data['processing_version']
        ds.broker_count = data['broker_count']
        
        dia_source_data.append(ds)

    objs = DiaSource.objects.bulk_create(dia_source_data)
    status = '%d Rows of Data stored' % count
    return Response(status)
                
@api_view(['POST'])
def bulk_create_ds_pv_ss_data(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)

    bc = next((i for i,d in enumerate(data) if 'insert' in d), None)
    
    if not bc:
        raise ValueError( "POST data must include 'insert'" )
    else:
        bc_data = data[1]['insert']
        count = len(bc_data)

    dspvss_data = []

    for data in bc_data:

        dspvss = DStoPVtoSS(dia_source=data['dia_source'])
        dspvss.processing_version = data['processing_version']
        dspvss.snapshot_name = data['snapshot_name']
        dspvss.valid_flag = data['valid_flag']
        dspvss.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)
    
        dspvss_data.append(dspvss)

    objs = DStoPVtoSS.objects.bulk_create(dspvss_data)
    status = '%d Rows of Data stored' % count
    return Response(status)

@api_view(['POST'])
def bulk_update_ds_pv_ss_valid_flag(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)

    bu = next((i for i,d in enumerate(data) if 'update' in d), None)

    if not bu:
        raise ValueError( "POST data must include 'update'" )
    else:
        bu_data = data[1]['update']
        count = len(bu_data)

    dspvss_data = []

    cursor = connection.cursor()

    for data in bu_data:

        d = {}
        d['valid_flag'] = data['valid_flag']
        d['dia_source'] = data['dia_source']
        d['processing_version'] = data['processing_version']
        d['snapshot_name'] = data['snapshot_name']

        dspvss_data.append(d)
        
    query = "update ds_to_pv_to_ss set valid_flag = %(valid_flag)s where dia_source = %(dia_source)s and processing_version = %(processing_version)s and snapshot_name = %(snapshot_name)s"

    execute_batch(cursor,query,dspvss_data)

    status = '%d Rows of Data stored' % count
    return Response(status)

@api_view(['POST'])
def bulk_update_dia_source_valid_flag(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)

    bu = next((i for i,d in enumerate(data) if 'update' in d), None)

    if not bu:
        raise ValueError( "POST data must include 'update'" )
    else:
        bu_data = data[1]['update']
        count = len(bu_data)

    cursor = connection.cursor()

    dia_source_data = []
    
    for data in bu_data:

        d = {}
        d['valid_flag'] = data['valid_flag']
        d['dia_source'] = data['dia_source']
        d['processing_version'] = data['processing_version']

        dia_source_data.append(d)

    query = "update dia_source set valid_flag = %(valid_flag)s where dia_source = %(dia_source)s and processing_version = %(processing_version)s"

    execute_batch(cursor,query,dia_source_data)

    status = '%d Rows of Data stored' % count
    return Response(status)
