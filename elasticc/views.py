import sys
import re
import io
import traceback
import json
import datetime
import django.urls
from django.http import HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.shortcuts import render, get_object_or_404
from django.contrib.auth.decorators import login_required, permission_required
from django.contrib.auth.mixins import PermissionRequiredMixin, LoginRequiredMixin
# from guardian.mixins import PermissionRequiredMixin
import django.views
from rest_framework import pagination
from rest_framework import permissions
from rest_framework import viewsets
from rest_framework import response

from elasticc.models import DiaObject, DiaSource, DiaForcedSource
from elasticc.models import DiaAlert, DiaTruth
from elasticc.models import DiaAlertPrvSource, DiaAlertPrvForcedSource
from elasticc.models import BrokerMessage, BrokerClassifier, BrokerClassification
from elasticc.serializers import DiaObjectSerializer, DiaTruthSerializer
from elasticc.serializers import DiaForcedSourceSerializer, DiaSourceSerializer, DiaAlertSerializer

# ======================================================================

class DiaObjectViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [permissions.IsAuthenticated]
    queryset = DiaObject.objects.all()
    serializer_class = DiaObjectSerializer

class DiaSourceViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [permissions.IsAuthenticated]
    queryset = DiaSource.objects.all()
    serializer_class = DiaSourceSerializer

class DiaForcedSourceViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [permissions.IsAuthenticated]
    queryset = DiaForcedSource.objects.all()
    serializer_class = DiaForcedSourceSerializer
    
class DiaTruthViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [permissions.IsAuthenticated]
    queryset = DiaTruth.objects.all()
    serializer_class = DiaTruthSerializer

    def retrieve( self, request, pk=None ):
        queryset = DiaTruth.objects.all()
        truth = get_object_or_404( queryset, diaSourceId=pk )
        serializer = DiaTruthSerializer( truth )
        return response.Response( serializer.data )

class DiaAlertViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [permissions.IsAuthenticated]
    queryset = DiaAlert.objects.all()
    serializer_class = DiaAlertSerializer
    
# ======================================================================
# I think that using the REST API and serializers is a better way to do
# this, but I'm still learning how all that works.  For now, put this here
# with a lot of manual work so that I can at least get stuff in
#
# (NOTE: I discovered that PermissionReqiredMixin has to go before
# django.views.View in the inheritance list.  This seems to me like something
# the documentation ought to call out anywhere it mentions the mixin....)

# @method_decorator(login_required, name='dispatch')
class MaybeAddDiaObject(PermissionRequiredMixin, django.views.View):
    permission_required = 'elasticc.elasticc_admin'
    raise_exception = True
    
    def post(self, request, *args, **kwargs):
        data = json.loads( request.body )
        curobj = DiaObject.load_or_create( data )
        resp = { 'status': 'ok', 'message': f'ObjectID: {curobj.diaObjectId}' }
        return JsonResponse( resp )

# @method_decorator(login_required, name='dispatch')
class MaybeAddAlert(PermissionRequiredMixin, django.views.View):
    permission_required = 'elasticc.elasticc_admin'
    raise_exception = True

    def post(self, request, *args, **kwargs):
        try:
            data = json.loads( request.body )
            if isinstance( data, dict ):
                data = [ data ]
            loaded = {}

            # Load the objects
            objdata = [ entry['diaObject'] for entry in data ]
            objects = DiaObject.bulk_load_or_create( objdata )
            loaded['objects'] = [ i.diaObjectId for i in objects ]
            objdict = { obj.diaObjectId: obj for obj in objects }

            # Load the sources
            for entry in data:
                entry['diaSource']['diaObject'] = objdict[ entry['diaObject']['diaObjectId'] ]
            sourcedata = [ entry['diaSource'] for entry in data ]
            sources = DiaSource.bulk_load_or_create( sourcedata )
            loaded['sources'] = [ i.diaSourceId for i in sources ]
            srcdict = { src.diaSourceId: src for src in sources }

            # Load the alerts
            alertdata = []
            for entry in data:
                alertdata.append( { 'alertId': entry['alertId'],
                                    'diaSource': srcdict[ entry['diaSource']['diaSourceId'] ],
                                    'diaObject': objdict[ entry['diaObject']['diaObjectId'] ] } )
            alerts = DiaAlert.bulk_load_or_create( alertdata )
            loaded['alerts'] = [ i.alertId for i in alerts ]
            
            resp = { 'status': 'ok', 'message': loaded }
            return JsonResponse( resp )
        except Exception as e:
            strstream = io.StringIO()
            traceback.print_exc( file=strstream )
            resp = { 'status': 'error',
                     'message': 'Exception in AddAlert',
                     'exception': str(e),
                     'traceback': strstream.getvalue() }
            strstream.close()
            return JsonResponse( resp )


# @method_decorator(login_required, name='dispatch')
class MaybeAddTruth(PermissionRequiredMixin, django.views.View):
    permission_required = 'elasticc.elasticc_admin'
    raise_exception = True

    def load_one_object( self, data ):
        curobj = DiaTruth.load_or_create( data )
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


class BrokerMessagePut(PermissionRequiredMixin, django.views.View):
    permission_required = 'elasticc.elasticc_broker'
    raise_exception = True

    def get( self, request ):
        return JsonResponse( { 'status': 'Nothing to see here, move along.' } )
    
    def put( self, request, *args, **kwargs ):
        data = json.loads( request.body )

        # Make the BrokerMessage object
        # This could lead to duplication; if the same classification
        # is sent more than once from a broker, it will get saved multiple
        # times.
        msgobj = BrokerMessage(
            alertId = data['alertId'],
            diaSourceId = data['diaSourceId'],
            # Note: an IEEE double can hold ~16 digits of precision
            # Given that it's ~10⁹ seconds since the Epoch now, the
            # IEEE double should be able to hold the number of microseconds
            # with integrity.  So, just divide by 1e6 to go from number of
            # microseconds to POSIX timestamp.
            elasticcPublishTimestamp = datetime.datetime.fromtimestamp( data['elasticcPublishTimestamp']/1e6,
                                                                        tz=datetime.timezone.utc ),
            brokerIngestTimestamp = datetime.datetime.fromtimestamp( data['brokerIngestTimestamp']/1e6,
                                                                     tz=datetime.timezone.utc )
        )
        msgobj.save()


        # Next, dig in, and make sure we know all the BrokerClassifier things
        broker_name = data['brokerName']
        broker_version = data['brokerVersion']
        classifiers = {}
        for classification in data['classifications']:
            # There's the _possibility_ of confusion here, but I'm going to write
            #  it off as not very likely.
            # (The ghost of Bobby Tables will haunt me forever.)
            cferkey = f'Name: {classification["classifierName"]} Params: {classification["classifierParams"]}'
            if cferkey in classifiers.keys():
                continue
            curcfers = BrokerClassifier.objects.all().filter(
                brokerName = broker_name,
                brokerVersion = broker_version,
                classifierName = classification['classifierName'],
                classifierParams = classification['classifierParams'] )
            if len(curcfers) > 0:
                cfer = curcfers[0]
            else:
                cfer = BrokerClassifier(
                    brokerName = broker_name,
                    brokerVersion = broker_version,
                    classifierName = classification['classifierName'],
                    classifierParams = classification['classifierParams'] )
                cfer.save()
            classifiers[cferkey] = cfer

        # Now, save all the classifications
        for classification in data['classifications']:
            cfer = classifiers[f'Name: {classification["classifierName"]} '
                               f'Params: {classification["classifierParams"]}']
            cfication = BrokerClassification(
                dbMessage = msgobj,
                dbClassifier = cfer,
                classId = classification['classId'],
                probability = classification['probability'] )
            cfication.save()

        resp = JsonResponse( { 'dbMessageIndex': msgobj.dbMessageIndex }, status=201 )
        # I really wish there were a django function for this, as I'm not sure that
        # my string replace will always do the right thing.  What I'm after is the
        # url of the current view, but without any parameters passed
        fullpath = request.build_absolute_uri()
        loc = re.sub( '\?.*$', '', fullpath )
        if loc[-1] != "/":
            loc += "/"
        resp.headers['Location'] =  f'{loc}{msgobj.dbMessageIndex}'

        return resp

# I would sort of like to make this the same class as BrokerMessagePut, but I haven't
# figure out how to tell the django method dispatcher to use a url pattern for only
# some HTTP methods
class BrokerMessageGet(LoginRequiredMixin, django.views.View):
# class BrokerMessageGet(django.views.View):
    raise_exception = True

    def get( self, request, msgid ):
        msgobj = BrokerMessage.objects.get(pk=msgid)
        resp = {
            'alertId': msgobj.alertId,
            'diaSourceId': msgobj.diaSourceId,
            'elasticcPublishTimestamp': int( msgobj.elasticcPublishTimestamp.timestamp() * 1e6 ),
            'brokerIngestTimestamp': int( msgobj.brokerIngestTimestamp.timestamp() * 1e6 ),
            'brokerName': "<unknown>",
            'brokerVersion': "<unknown>",
            'classifications': []
            }
        clsfctions = BrokerClassification.objects.all().filter( dbMessage=msgobj )
        first = True
        for classification in clsfctions:
            clsifer = classification.dbClassifier
            if first:
                resp['brokerName'] = clsifer.brokerName
                resp['brokerVersion'] = clsifer.brokerVersion
                first = False
            else:
                if ( ( clsifer.brokerName != resp['brokerName'] ) or
                     ( clsifer.brokerVersion != resp['brokerVersion'] ) ):
                    raise ValueError( "Mismatching brokerName and brokerVersion in the database! "
                                      "This shouldn't happen!" )
            resp['classifications'].append( { 'classifierName': clsifer.classifierName,
                                              'classifierParams': clsifer.classifierParams,
                                              'classId': classification.classId,
                                              'probability': classification.probability } )

        return JsonResponse( resp )
