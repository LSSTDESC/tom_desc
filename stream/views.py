import sys
import os
import json
import traceback
import io
import psycopg2
import psycopg2.extras
from django.http import HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required, permission_required
from django.shortcuts import render, get_object_or_404
from django.db.models import F
import django.views
from rest_framework import pagination
from rest_framework import permissions
from rest_framework import viewsets
from rest_framework import response

# from stream.filters import AlertFilter, EventFilter, TopicFilter
# from stream.models import Alert, Event, Target, Topic, RknopTest
# from stream.serializers import AlertSerializer, EventDetailSerializer, EventSerializer
# from stream.serializers import TargetSerializer, TopicSerializer
#  from stream.serializers.v1 import serializers as v1_serializers
from stream.models import ElasticcDiaObject, ElasticcDiaSource, ElasticcDiaForcedSource
from stream.models import ElasticcDiaAlert, ElasticcDiaTruth
from stream.models import ElasticcDiaAlertPrvSource, ElasticcDiaAlertPrvForcedSource
from stream.serializers import ElasticcDiaObjectSerializer, ElasticcDiaTruthSerializer
from stream.serializers import ElasticcDiaForcedSourceSerializer, ElasticcDiaSourceSerializer

# class TargetViewSet(viewsets.ModelViewSet):
#     """
#     API endpoint that allows targets to be viewed or edited.
#     """
#     # TODO: should we order Targets ?
#     queryset = Target.objects.all()
#     serializer_class = TargetSerializer
#     permission_classes = [permissions.IsAuthenticated]
#     pagination_class = pagination.PageNumberPagination


# class AlertViewSet(viewsets.ModelViewSet):
#     """
#     API endpoint that allows groups to be viewed or edited.
#     """
#     filterset_class = AlertFilter
#     # permission_classes = [permissions.IsAuthenticated]
#     queryset = Alert.objects.all()
#     serializer_class = AlertSerializer

#     class Meta:
#         # https://docs.djangoproject.com/en/dev/ref/models/options/#ordering
#         ordering = [F('alert_timestamp').desc(nulls_last=True), F('timestamp').desc(nulls_last=True)]

#     def get_serializer_class(self):
#         if self.request.version in ['v0', 'v1']:
#             return v1_serializers.AlertSerializer
#         return AlertSerializer


# class TopicViewSet(viewsets.ModelViewSet):
#     filterset_class = TopicFilter
#     # permission_classes = [permissions.IsAuthenticated]
#     queryset = Topic.objects.all()
#     serializer_class = TopicSerializer


# class EventViewSet(viewsets.ModelViewSet):
#     filterset_class = EventFilter
#     queryset = Event.objects.all()

#     def get_serializer_class(self):
#         if self.action == 'retrieve':
#             return EventDetailSerializer
#         return EventSerializer

# # ======================================================================


# @method_decorator(login_required, name='dispatch')
# class DumpRknopTest(django.views.View):
#     def get(self, request, *args, **kwargs):
#         them = RknopTest.objects.all()
#         for it in them:
#             ret.append( { "number": it.number, "description": it.description } )
#         return HttpResponse(json.dumps(ret))
        

# ======================================================================

class ElasticcDiaObjectViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [permissions.IsAuthenticated]
    queryset = ElasticcDiaObject.objects.all()
    serializer_class = ElasticcDiaObjectSerializer

class ElasticcDiaSourceViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [permissions.IsAuthenticated]
    queryset = ElasticcDiaSource.objects.all()
    serializer_class = ElasticcDiaSourceSerializer

class ElasticcDiaForcedSourceViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [permissions.IsAuthenticated]
    queryset = ElasticcDiaForcedSource.objects.all()
    serializer_class = ElasticcDiaForcedSourceSerializer
    
class ElasticcDiaTruthViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [permissions.IsAuthenticated]
    queryset = ElasticcDiaTruth.objects.all()
    serializer_class = ElasticcDiaTruthSerializer

    def retrieve( self, request, pk=None ):
        queryset = ElasticcDiaTruth.objects.all()
        truth = get_object_or_404( queryset, diaSourceId=pk )
        serializer = ElasticcDiaTruthSerializer( truth )
        return response.Response( serializer.data )

# ======================================================================
# I think that using the REST API and serializers is a better way to do
# this, but I'm still learning how all that works.  For now, put this here
# with a lot of manual work so that I can at least get stuff in

@method_decorator(login_required, name='dispatch')
class MaybeAddElasticcDiaObject(django.views.View):
    def post(self, request, *args, **kwargs):
        data = json.loads( request.body )
        curobj = ElasticcDiaObject.load_or_create( data )
        resp = { 'status': 'ok', 'message': f'ObjectID: {curobj.diaObjectId}' }
        return JsonResponse( resp )

@method_decorator(login_required, name='dispatch')
class MaybeAddElasticcAlert(django.views.View):
    def load_one_object( self, data ):
        # Load the main object
        curobj = ElasticcDiaObject.load_or_create( data['diaObject'] )
        # Load the main source
        data['diaSource']['diaObject'] = curobj
        cursrc = ElasticcDiaSource.load_or_create( data['diaSource'] )
        # Load the previous sources
        prevs = []
        if data['prvDiaSources'] is not None:
            for prvdata in data['prvDiaSources']:
                prvdata['diaObject'] = curobj
                prevs.append( ElasticcDiaSource.load_or_create( prvdata ) )
        # Load the previous forced sources
        forced = []
        if data['prvDiaForcedSources'] is not None:
            for forceddata in data['prvDiaForcedSources']:
                forceddata['diaObject'] = curobj
                forced.append( ElasticcDiaForcedSource.load_or_create( forceddata ) )
        # Load the alert
        # TODO : check to see if the alertId already exists???
        # Right now, this will error out due to the unique constraint
        curalert = ElasticcDiaAlert( alertId = data['alertId'], diaSource = cursrc, diaObject = curobj )
        curalert.save()
        # Load the linkages to the previouses
        for prv in prevs:
            tmp = ElasticcDiaAlertPrvSource( diaAlert=curalert, diaSource=prv )
            tmp.save()
        for prv in forced:
            tmp = ElasticcDiaAlertPrvForcedSource( diaAlert=curalert, diaForcedSource=prv )
            tmp.save()
                          
        return { 'alertId': curalert.alertId,
                 'diaSourceId': cursrc.diaSourceId,
                 'diaObjectId': curobj.diaObjectId,
                 'prvDiaSourceIds': [ prv.diaSourceId for prv in prevs ],
                 'prvDiaForcedSourceIds': [ prv.diaForcedSourceId for prv in forced ] }
        
    def post(self, request, *args, **kwargs):
        try:
            data = json.loads( request.body )
            loaded = []
            if isinstance( data, dict ):
                loaded.append( self.load_one_object( data ) )
            else:
                for item in data:
                    loaded.append( self.load_one_object( item ) )
            resp = { 'status': 'ok', 'message': loaded }
            return JsonResponse( resp )
        except Exception as e:
            strstream = io.StringIO()
            traceback.print_exc( file=strstream )
            resp = { 'status': 'error',
                     'message': 'Exception in AddElasticcAlert',
                     'exception': str(e),
                     'traceback': strstream.getvalue() }
            strstream.close()
            return JsonResponse( resp )

# ======================================================================
# OK... I really  need to make this and MaybeAddElasticcAlert derived
#  classes of a common superclass

@method_decorator(login_required, name='dispatch')
class MaybeAddElasticcTruth(django.views.View):
    def load_one_object( self, data ):
        curobj = ElasticcDiaTruth.load_or_create( data )
        return curobj.diaSourceId
    
    def post( self, request, *args, **kwargs ):
        try:
            data = json.loads( request.body )
            loaded = []
            if isinstance( data, dict ):
                loaded.append( self.load_one_object( data ) )
            else:
                for item in data:
                    loaded.append( self.load_one_object( item ) )
            resp = { 'status': 'ok', 'message': loaded }
            return JsonResponse( resp )
        except Exception as e:
            strstream = io.StringIO()
            traceback.print_exc( file=strstream )
            resp = { 'status': 'error',
                     'message': f'Exception in {self.__class__.__name__}',
                     'exception': str(e),
                     'traceback': strstream.getvalue() }
            strstream.close()
            return JsonResponse( resp )

# ======================================================================
# A low-level query interface.
#
# That is, of course, EXTREMELY scary.  This is why you need to make
# sure the user tom_desc_ro is a readonly user.  Still scary, but not
# Cthulhuesque.

@method_decorator(login_required, name='dispatch')
class RunSQLQuery(django.views.View):
    def post( self, request, *args, **kwargs ):
        data = json.loads( request.body )
        if not 'query' in data:
            raise ValueError( "Must pass a query" )
        subdict = {}
        if 'subdict' in data:
            subdict = data['subdict']
        with open( "/secrets/postgres_ro_password" ) as ifp:
            password = ifp.readline()
        password.strip()
        dbconn = psycopg2.connect( dbname=os.getenv('DB_NAME'), host=os.getenv('DB_HOST'),
                                   user='tom_desc_ro', password=password,
                                   cursor_factory=psycopg2.extras.RealDictCursor )
        # sys.stderr.write( f'Query is {data["query"]}, subdict is {subdict}\n' )
        cursor = dbconn.cursor()
        cursor.execute( data['query'], subdict )
        return JsonResponse( { 'status': 'ok', 'rows': cursor.fetchall() } )
        
