import datetime
import json
import os
import psycopg2
from time import sleep

from alerts.models import LastUpdateTime, ProcessingVersions, HostGalaxy, Snapshots, DiaObject, DiaSource, DiaForcedSource,SnapshotTags
from alerts.models import DStoPVtoSS, DFStoPVtoSS, BrokerClassifier, BrokerClassification

from alerts.serializers import DiaSourceSerializer
from alerts.serializers import DiaObjectSerializer
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

from django.views.decorators.csrf import csrf_exempt
from django.db.models import Q
from django.contrib.auth.decorators import login_required
from django.contrib.auth import authenticate, login
from django.contrib.auth.models import User
from django.core import serializers
from django.shortcuts import render
from django.http import HttpResponse
from django.template import RequestContext
from django.middleware.csrf import get_token

from django.db import connection

from multiprocessing import Process

import secrets
from http import cookies
import uuid

from rest_framework.settings import api_settings

@api_view(['GET'])
def acquire_token(request):
    token = get_token(request)
    data={'csrftoken':token}
    return Response(data)

@api_view(['POST'])
def get_dia_sources(request):

    query = "select dsrc.* from ds_to_pv_to_ss as ds, dia_source as dsrc where ds.snapshot_name=%s and ds.dia_source=dsrc.dia_source;"
    raw_data = json.loads(request.body)
    query_data = json.loads(raw_data)

    params = request.query_params
    snapshot_name  =  params['snapshot_name']
    data = DiaSource.objects.raw(query,[snapshot_name])
    serializer = DiaSourceSerializer(data, many=True)
    return Response(serializer.data)

@api_view(['POST'])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])
def raw_query_long(request):

    print(request.user)
    if not request.user.is_authenticated:
        return Response(serializer.error, status=status.HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)

    for d in data:
        print(data)
        if 'query' in d:
            query = d['query']

    p = Process(target=raw_query_process, args=(query,))
    p.start()

    status = 'Query started'
    return Response(status)

@api_view(['POST'])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])
def raw_query_short(request):

    print(request.user)
    if not request.user.is_authenticated:
        return Response(serializer.error, status=status.HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    data = json.loads(raw_data)

    for d in data:
        print(data)
        if 'query' in d:
            query = d['query']

    cursor = connection.cursor()
    cursor.execute(query)
    json_data = dictfetchall(cursor)
    
    return Response(json_data)

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
 
@api_view(['POST'])
@parser_classes([JSONParser])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])
def get_dia_objects(request):
    
    print(request.user)
    if not request.user.is_authenticated:
        return Response(serializer.error, status=HTTP_403_FORBIDDEN)
    
    raw_data = json.loads(request.body)
    filter_data = json.loads(raw_data)
    print(filter_data)
    nfd = len(filter_data)
    if nfd == 0:
        all_objects = DiaObject.objects.all()
    else:
        # Creating initial Q object
        filter_objects = Q()

        # Looping through the array of objects
        for data in filter_data:
            # The main part. Calling get_filter and passing the filter object data.
            print(data)
            if 'field' in data:
                filter_objects &= get_filter(data['field'], data['condition'], data['value'])
        filtered_data = DiaObject.objects.filter(filter_objects)
    
    serializer = DiaObjectSerializer(filtered_data, many=True)
    return Response(serializer.data)

def get_filter(field_name, filter_condition, filter_value):
 
    if filter_condition.strip() == "gt":
        kwargs = {
            '{0}__gt'.format(field_name): filter_value
        }
        return Q(**kwargs)

    if filter_condition.strip() == "gte":
        kwargs = {
            '{0}__gte'.format(field_name): filter_value
        }
        return Q(**kwargs)

    if filter_condition.strip() == "lt":
        kwargs = {
            '{0}__lt'.format(field_name): filter_value
        }
        return Q(**kwargs)

    if filter_condition.strip() == "lte":
        kwargs = {
            '{0}__lte'.format(field_name): filter_value
        }
        return Q(**kwargs)

    if filter_condition.strip() == "equal":
        kwargs = {
            '{0}__exact'.format(field_name): filter_value
        }
        return Q(**kwargs)
    
    if filter_condition.strip() == "not_equal":
        kwargs = {
            '{0}__exact'.format(field_name): filter_value
        }
        return ~Q(**kwargs)

    if filter_condition.strip() == "contains":
        kwargs = {
            '{0}__contains'.format(field_name): filter_value
        }
        return Q(**kwargs)

    if filter_condition.strip() == "range":
        kwargs = {
            '{0}__range'.format(field_name): (filter_value[0],filter_value[1])
        }
        return Q(**kwargs)

    if filter_condition.strip() == "year":
        kwargs = {
            '{0}__year'.format(field_name): filter_value
        }
        return Q(**kwargs)

@api_view(['POST'])
def store_dia_source_data(request):

    if not request.user.is_authenticated:
        return Response(serializer.error, status=HTTP_403_FORBIDDEN)

    raw_data = json.loads(request.body)
    print(raw_data)
    data = json.loads(raw_data)
    for d in data:
        print(d)
        if 'query' in d:
            dia_source_data = d['query']
#    data = json.loads(raw_data)
#    dia_source_data = data[1]
    count = len(dia_source_data)
    print(count)

    for data in dia_source_data:
        print(data)
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

        print(data)
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

def raw_query_process(query):

    secret = os.environ['FASTDB_READER_PASSWORD']
    conn_string = "host='fastdb-fastdb-psql' dbname='fastdb' user='fastdb_reader' password='%s'" % secret.strip()
    print("Connecting to database %s" % conn_string)
    conn = psycopg2.connect(conn_string)
    
    cursor = conn.cursor()
    print("Connected")

    print(query)

    cursor.execute(query)
    json_data = dictfetchall(cursor)

    sleep(60)
    print(json_data)

    cursor.close()
    conn.close()
