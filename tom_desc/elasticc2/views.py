import sys
import re
import json
import datetime

import django.db
import django.views
import django.forms.models
from django.http import HttpResponse, JsonResponse, HttpResponseForbidden, HttpResponseBadRequest
from django.shortcuts import render, get_object_or_404
from django.contrib.auth.mixins import PermissionRequiredMixin, LoginRequiredMixin

import rest_framework

from elasticc2.models import BrokerMessage, PPDBDiaObject, PPDBDiaSource, PPDBDiaForcedSource
from elasticc2.serializers import PPDBDiaObjectSerializer, PPDBDiaSourceSerializer, PPDBDiaForcedSourceSerializer

# ======================================================================
# DJango REST interfaces

class PPDBDiaObjectViewSet( rest_framework.viewsets.ReadOnlyModelViewSet ):
    permission_classes = [ rest_framework.permissions.IsAuthenticated ]
    queryset = PPDBDiaObject.objects.all()
    serializer_class = PPDBDiaObjectSerializer

class PPDBDiaSourceViewSet( rest_framework.viewsets.ReadOnlyModelViewSet ):
    permission_classes = [ rest_framework.permissions.IsAuthenticated ]
    queryset = PPDBDiaSource.objects.all()
    serializer_class = PPDBDiaSourceSerializer

class PPDBDiaForcedSourceViewSet( rest_framework.viewsets.ReadOnlyModelViewSet ):
    permission_classes = [ rest_framework.permissions.IsAuthenticated ]
    queryset = PPDBDiaForcedSource.objects.all()
    serializer_class = PPDBDiaForcedSourceSerializer

class PPDBDiaObjectSourcesViewSet( rest_framework.viewsets.ReadOnlyModelViewSet ):
    permission_classes = [ rest_framework.permissions.IsAuthenticated ]
    queryset = PPDBDiaObject.objects.all()
    serializer_class = None

    def list( self, request, pk=None ):
        return rest_framework.response.Response( status=rest_framework.status.HTTP_400_BAD_REQUEST,
                                                 data="Must give an object id" )
    
    def retrieve( self, request, pk=None ):
        obj = get_object_or_404( self.queryset, pk=pk )
        objserializer = PPDBDiaObjectSerializer( obj )
        objdict = dict( objserializer.data )
        srcs = PPDBDiaSource.objects.filter( ppdbdiaobject_id=pk )
        objdict[ 'ppdbdiasources' ] = []
        for src in srcs:
            ser = PPDBDiaSourceSerializer( src )
            objdict[ 'ppdbdiasources' ].append( ser.data )
        objdict[ 'ppdbdiaforcedsources' ] = []
        frcsrcs = PPDBDiaForcedSource.objects.filter( ppdbdiaobject_id=pk )
        for src in frcsrcs:
            ser = PPDBDiaForcedSourceSerializer( src )
            objdict[ 'ppdbdiaforcedsources' ].append( ser.data )
        return rest_framework.response.Response( objdict )

class PPDBDiaObjectAndPrevSourcesForSourceViewSet( rest_framework.viewsets.ReadOnlyModelViewSet ):
    permission_classes = [ rest_framework.permissions.IsAuthenticated ]
    queryset = PPDBDiaSource.objects.all()
    serializer_class = None

    def list( self, request, pk=None ):
        return rest_framework.response.Response( status=rest_framework.status.HTTP_400_BAD_REQUEST,
                                                 data="Must give a source id" )

    def retrieve( self, request, pk=None ):
        sys.stderr.write( f"Trying to get source {pk}" )
        src = get_object_or_404( PPDBDiaSource.objects.all(), pk=pk )
        sys.stderr.write( f"Trying to get object {src.ppdbdiaobject_id}" )
        obj = get_object_or_404( PPDBDiaObject.objects.all(), pk=src.ppdbdiaobject_id )
        objserializer = PPDBDiaObjectSerializer( obj )
        objdict = dict( objserializer.data )
        objdict[ 'ppdbdiasources' ] = []
        objdict[ 'ppdbdiaforcedsources' ] = []
        srcs = ( PPDBDiaSource.objects
                 .filter( ppdbdiaobject_id=src.ppdbdiaobject_id )
                 .filter( midpointtai__lte=src.midpointtai ) )
        frcsrcs = ( PPDBDiaForcedSource.objects
                    .filter( ppdbdiaobject_id=src.ppdbdiaobject_id )
                    .filter( midpointtai__lte=src.midpointtai ) )
        for src in srcs:
            ser = PPDBDiaSourceSerializer( src )
            objdict[ 'ppdbdiasources' ].append( ser.data )
        for src in frcsrcs:
            ser = PPDBDiaForcedSourceSerializer( src )
            objdict[ 'ppdbdiaforcedsources' ].append( ser.data )
        return rest_framework.response.Response( objdict )
    
# ======================================================================
# A REST interface (but not using the Django REST framework) for
# viewing and posting broker messages.  Posting broker messages
# (via the PUT method) requires the elasticc2.elasticc_broker
# permission.

        
class BrokerMessageView(PermissionRequiredMixin, django.views.View):
    raise_exception = True

    def has_permission( self ):
        if self.request.method == 'PUT':
            return self.request.user.has_perm( "elasticc2.elasticc_broker" )
        else:
            return bool(self.request.user.is_authenticated)

    def get_queryset( self, request, info, offset=0, num=100 ):
        n = None
        if isinstance( info, int ):
            msgs = BrokerMessage.objects.filter(pk=info)

        else:
            argre = re.compile( '^([a-z0-9]+)=(.*)$' )
            args = {}
            argstr = str(info).split("/")
            for argtxt in argstr:
                match = argre.search( argtxt )
                if match is not None:
                    args[match.group(1).lower()] = match.group(2)
                else:
                    args[argtxt] = None

            if ( len(args) == 1 ) and ( "alertid" in args.keys() ):
                alertid = args['alertid']
                msgs = BrokerMessage.objects.filter( alertId__in=str(alertid).split(",") )

            elif ( len(args) == 1 ) and ( ( "objectid" in args.keys() ) or ( "diaobjectid" in args.keys() ) ):
                objid = list(args.values())[0]
                # django doesn't seem to have a native way to do joins on things that aren't foreign keys
                # So, I need to do this raw thing.
                # This also means I have to do a separate query to get the count, hence the "n=None"
                # business at the top.
                params={ 'objids': tuple( str(objid).split(",") ) }
                with django.db.connection.cursor() as cursor:
                    cursor.execute( 'SELECT COUNT(b."brokerMessageId") FROM elasticc_brokermessage b'
                                    ' INNER JOIN elasticc_diasource s ON b."diaSourceId"=s."diaSourceId"'
                                    ' WHERE s."diaObjectId" IN %(objids)s', params=params )
                    row = cursor.fetchone()
                    n = row[0]
                msgs = BrokerMessage.objects.raw(
                    'SELECT * FROM elasticc_brokermessage b INNER JOIN elasticc_diasource s'
                    ' ON b."diaSourceId"=s."diaSourceId" WHERE s."diaObjectId" IN %(objids)s', params=params )

            elif ( len(args) == 1 ) and ( ( "sourceid" in args.keys() ) or ( "diasourceid" in args.keys() ) ):
                sourceid = list(args.values())[0]
                msgs = BrokerMessage.objects.filter( diaSourceId__in=str(sourceid).split(",") )

            else:
                raise ValueError( f"Can't parse argument string \"{info}\"." )

        if n is None:
            n = msgs.count()
            
        if ( offset is None ) and ( num is None ):
            return msgs, n
        elif num is None:
            return msgs[offset:], n
        elif offset is None:
            return msgs[0:num], n
        else:
            return msgs[offset:offset+num], n

    def offset_num( self, request ):
        vals = { 'offset': 0,
                 'num': 100 }
        for args in [ request.GET, request.POST ]:
            for val in vals.keys():
                if val in args.items():
                    if args[val] == "None":
                        vals[val] = None
                    else:
                        vals[val] = int(args[val])
        return vals['offset'], vals['num']
        
    def get( self, request, info=None ):
        # EVentually I want to make get return html
        return self.post( request, info )
        
    def post( self, request, info=None ):
        offset, num = self.offset_num( request )

        try:
            msgs, nmsgs = self.get_queryset( request, info, offset, num )
        except ValueError as ex:
            return JsonResponse( { "error": str(ex) } )
        
        if isinstance( info, int ):
            return JsonResponse( {} if msgs.count()==0 else msgs[0].to_dict() )
        else:
            return JsonResponse( { 'offset': offset,
                                   'num': num,
                                   'totalcount': nmsgs,
                                   'count': msgs.count(),
                                   'brokermessages': [ msg.to_dict() for msg in msgs ] } )
        
        
    def put( self, request, *args, **kwargs ):

        data = json.loads( request.body )
        if not isinstance( data, list ):
            data = [ data ]
        messageinfo = []
        # Reformulate the data array into what BrokerMessage.load_batch is expecting
        for datum in data:
            datum['elasticcPublishTimestamp'] = datetime.datetime.fromtimestamp( datum['elasticcPublishTimestamp']
                                                                                 / 1000,
                                                                                 tz=datetime.timezone.utc )
            datum['brokerIngestTimestamp'] = datetime.datetime.fromtimestamp( datum['brokerIngestTimestamp']
                                                                              / 1000,
                                                                              tz=datetime.timezone.utc )
            messageinfo.append( { 'topic': 'REST_push',
                                  'timestamp': datetime.datetime.now( tz=datetime.timezone.utc ),
                                  'msgoffset': -1,
                                  'msg': datum } )
            

        batchret = BrokerMessage.load_batch( messageinfo, logger=_logger )
        dex = -1 if batchret['firstbrokerMessageId'] is None else batchret['firstbrokerMessageId']
        resp = JsonResponse( { 'brokerMessageId': dex,
                               'num_loaded': batchret['addedmsgs'] },
                             status=201 )
        # I really wish there were a django function for this, as I'm not sure that
        # my string replace will always do the right thing.  What I'm after is the
        # url of the current view, but without any parameters passed
        fullpath = request.build_absolute_uri()
        loc = re.sub( '\?.*$', '', fullpath )
        if loc[-1] != "/":
            loc += "/"
        resp.headers['Location'] =  f'{loc}{dex}'

        return resp

