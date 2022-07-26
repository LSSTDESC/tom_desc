import sys
import re
import io
import traceback
import json
import time
import datetime
import logging
import django.db
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
from elasticc.models import DiaAlert, DiaTruth, DiaObjectTruth
from elasticc.models import DiaAlertPrvSource, DiaAlertPrvForcedSource
from elasticc.models import BrokerMessage, BrokerClassifier, BrokerClassification
from elasticc.serializers import DiaObjectSerializer, DiaTruthSerializer
from elasticc.serializers import DiaForcedSourceSerializer, DiaSourceSerializer, DiaAlertSerializer

# I tried inherting from the root logger, but it
#  doesn't seem to have the formatting built in;
#  I guess djano makes its own formatting instead
#  of using logging's.  Sigh.
_logger = logging.getLogger(__name__)
_logout = logging.StreamHandler( sys.stderr )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' )
_logout.setFormatter( _formatter )
_logger.propagate = False
_logger.addHandler( _logout )
_logger.setLevel( logging.INFO )

# ======================================================================
# DJango REST interfaces

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
# Interfaces for loading alerts into the database

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

    def load_one_object( self, data ):
        # Load the main object
        # t0 = time.perf_counter()
        curobj = DiaObject.load_or_create( data['diaObject'] )
        # self.objloadtime += time.perf_counter() - t0
        # Load the main source
        # t0 = time.perf_counter()
        data['diaSource']['diaObject'] = curobj
        cursrc = DiaSource.load_or_create( data['diaSource'] )
        # self.sourceloadtime += time.perf_counter() - t0
        if True:
            # Load the previous sources
            prevs = []
            if data['prvDiaSources'] is not None:
                for prvdata in data['prvDiaSources']:
                    prvdata['diaObject'] = curobj
                    prvobj = DiaSource.load_or_create( prvdata )
                    prevs.append( prvobj )
            # Load the previous forced sources
            forced = []
            if data['prvDiaForcedSources'] is not None:
                for forceddata in data['prvDiaForcedSources']:
                    forceddata['diaObject'] = curobj
                    forcedobj = DiaForcedSource.load_or_create( forceddata )
                    forced.append( forcedobj )
        else:
            prevs = []
            forced = []
        # Load the alert
        # TODO : check to see if the alertId already exists???
        # Right now, this will error out due to the unique constraint
        # t0 = time.perf_counter()
        curalert = DiaAlert( alertId = data['alertId'], diaSource = cursrc, diaObject = curobj )
        # curalert.save()
        self.alertloadtime += time.perf_counter() - t0
        if True:
            # Load the linkages to the previouses
            for prv in prevs:
                tmp = DiaAlertPrvSource( diaAlert=curalert, diaSource=prv )
                tmp.save()
            for prv in forced:
                tmp = DiaAlertPrvForcedSource( diaAlert=curalert, diaForcedSource=prv )
                tmp.save()
                          
        return { 'alertId': curalert.alertId,
                 }
                 # 'diaSourceId': cursrc.diaSourceId,
                 # 'diaObjectId': curobj.diaObjectId,
                 # 'prvDiaSourceIds': prevs,
                 # 'prvDiaForcedSourceIds': forced }
        
    def post(self, request, *args, **kwargs):
        try:
            # _logger.info( "Starting MaybeAddAlert.post" )
            # self.objloadtime = 0
            # self.sourceloadtime = 0
            # self.alertloadtime = 0
            data = json.loads( request.body )
            if isinstance( data, dict ):
                data = [ data ]
            loaded = {}

            # Note: I pass all of the things, not just the things that are
            #  are not already there, to bulk_load_or_create so that I'll
            #  have those objects available if necessary for a later step.
            # (cf: the objdata=, srcdata= comprehensions)
            
            # Load the objects
            objids = { entry['diaObject']['diaObjectId'] for entry in data }
            curobjids = set( DiaObject.which_exist( objids ) )
            newobjids = objids - curobjids
            # objdata = [ entry['diaObject'] for entry in data if entry['diaObject']['diaObjectId'] in newobjids ]
            objdata = [ entry['diaObject'] for entry in data ]
            objects = DiaObject.bulk_load_or_create( objdata )
            loaded['objects'] = [ i.diaObjectId for i in objects if i.diaObjectId in newobjids ]
            objdict = { obj.diaObjectId: obj for obj in objects }

            # Load the sources
            srcids = { entry['diaSource']['diaSourceId'] for entry in data }
            cursrcids = set( DiaSource.which_exist( srcids ) )
            newsrcids = srcids - cursrcids
            for entry in data:
                entry['diaSource']['diaObject'] = objdict[ entry['diaObject']['diaObjectId'] ]
            # sourcedata = [ entry['diaSource'] for entry in data if entry['diaSource']['diaSourceId'] in newsrcids ]
            sourcedata = [ entry['diaSource'] for entry in data if entry['diaSource']['diaSourceId'] ]
            sources = DiaSource.bulk_load_or_create( sourcedata )
            loaded['sources'] = [ i.diaSourceId for i in sources if i.diaSourceId in newsrcids ]

            # Load the previous sources
            prvdiasources = []
            prvdiasourceids = set()
            for alert in data:
                if alert['prvDiaSources'] is not None:
                    for prvdiasource in alert['prvDiaSources']:
                        if prvdiasource['diaSourceId'] in prvdiasourceids:
                            # The same previous source will probably show up
                            #  in multiple alerts among the batch we're loading
                            continue
                        prvdiasource['diaObject'] = objdict[ alert['diaObject']['diaObjectId'] ]
                        prvdiasources.append( prvdiasource )
                        prvdiasourceids.add( prvdiasource['diaSourceId' ] )
            curprvdiasourceids = set( DiaSource.which_exist( prvdiasourceids ) )
            newprvdiasourceids = prvdiasourceids - curprvdiasourceids
            prvsourceobjs = DiaSource.bulk_load_or_create( prvdiasources )
            loaded['sources'].extend( [ i.diaSourceId for i in prvsourceobjs if i.diaSourceId in newprvdiasourceids ] )

            # Make the dictionary of sources including previous
            sources.extend( prvsourceobjs )
            srcdict = { src.diaSourceId: src for src in sources }
                                         
                                         
            # Load the forceds ources
            forced = []
            forcedids = set()
            for alert in data:
                if alert['prvDiaForcedSources'] is not None:
                    for forcedsource in alert['prvDiaForcedSources']:
                        if forcedsource['diaForcedSourceId'] in forcedids:
                            # The same forced source will probably show up
                            # in multiple alerts among the batch we're loading
                            continue
                        forcedsource['diaObject'] = objdict[ alert['diaObject']['diaObjectId' ] ]
                        forced.append( forcedsource )
                        forcedids.add( forcedsource['diaForcedSourceId'] )
            curforcedids = set( DiaForcedSource.which_exist( forcedids ) )
            newforcedids = forcedids - curforcedids
            forcedobjs = DiaForcedSource.bulk_load_or_create( forced )
            loaded['forcedsources'] = [ i.diaForcedSourceId for i in forcedobjs
                                        if i.diaForcedSourceId in newforcedids ]
            forceddict = { fs.diaForcedSourceId: fs for fs in forcedobjs }
            
            # Load the alerts
            alertids = { entry['alertId'] for entry in data }
            curalertids = set( DiaAlert.which_exist( alertids ) )
            newalertids = alertids - curalertids
            alertdata = []
            for entry in data:
                alertdata.append( { 'alertId': entry['alertId'],
                                    'diaSource': srcdict[ entry['diaSource']['diaSourceId'] ],
                                    'diaObject': objdict[ entry['diaObject']['diaObjectId'] ] } )
            alerts = DiaAlert.bulk_load_or_create( alertdata )
            loaded['alerts'] = [ i.alertId for i in alerts if i.alertId in newalertids ]

            # Not loading the previous source images
            # These tables will have hundreds of millions or billions
            # of lines, and we can figure it all out algorithmically
            
            # # Load the previous source linkages
            # sourcelinks = []
            # for entry in data:
            #     if entry['prvDiaSources'] is not None:
            #         sourcelinks.extend( [ { 'diaAlert_id': entry['alertId'],
            #                                 'diaSource_id': i['diaSourceId'] }
            #                               for i in entry['prvDiaSources'] ] )
            # DiaAlertPrvSource.bulk_load_or_create( sourcelinks )
            
            # # Load the forced source linkages
            # forcedlinks = []
            # for entry in data:
            #     if entry['prvDiaForcedSources'] is not None:
            #         forcedlinks.extend( [ { 'diaAlert_id': entry['alertId'],
            #                                 'diaForcedSource_id': i['diaForcedSourceId'] }
            #                               for i in entry['prvDiaForcedSources'] ] )
            # DiaAlertPrvForcedSource.bulk_load_or_create( forcedlinks )
            
            resp = { 'status': 'ok', 'message': loaded }
            # _logger.info( f'objloadtime={self.objloadtime}, sourceloadtime={self.sourceloadtime}, '
            #               f'alertloadtime={self.alertloadtime}' )
            # _logger.info( f'Returning {sys.getsizeof(resp)} byte response.' )
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

    def post( self, request, *args, **kwargs ):
        try:
            data = json.loads( request.body )
            if isinstance( data, dict ):
                curobj = DiaTruth.load_or_create( data )
                loaded = [ curobj.diaSourceId ]
            else:
                objs, missing = DiaTruth.bulk_load_or_create( data )
                loaded = [ i.diaSourceId for i in objs ]
            resp = { 'status': 'ok', 'message': loaded, 'missing': list( missing ) }
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

class MaybeAddObjectTruth(PermissionRequiredMixin, django.views.View):
    permission_required = 'elasticc.elasticc_admin'
    raise_exception =  True

    def post( self, request, *args, **kwargs ):
        try:
            data = json.loads( request.body )
            loaded = []
            if isinstance( data, dict ):
                loaded.append( DiaObjectTruth.load_or_create( data ) ).diaObjectId
            else:
                objs = DiaObjectTruth.bulk_load_or_create( data )
                loaded.extend( [ obj.diaObject_id for obj in objs ] )
            resp = { 'status':'ok', 'message': loaded }
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
# A REST interface (but not using the Django REST framework) for
# viewing and posting broker messages.  Posting broker messages
# (via the PUT method) requires the elasticc.elasticc_broker
# permission.

        
class BrokerMessageView(PermissionRequiredMixin, django.views.View):
    raise_exception = True

    def has_permission( self ):
        if self.request.method == 'PUT':
            return self.request.user.has_perm( "elasticc.elasticc_broker" )
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
                    cursor.execute( 'SELECT COUNT(b."dbMessageIndex") FROM elasticc_brokermessage b'
                                    ' INNER JOIN elasticc_diasource s ON b."diaSourceId"=s."diaSourceId"'
                                    ' WHERE s."diaObject_id" IN %(objids)s', params=params )
                    row = cursor.fetchone()
                    n = row[0]
                msgs = BrokerMessage.objects.raw(
                    'SELECT * FROM elasticc_brokermessage b INNER JOIN elasticc_diasource s'
                    ' ON b."diaSourceId"=s."diaSourceId" WHERE s."diaObject_id" IN %(objids)s', params=params )

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

# ======================================================================
# ======================================================================
# ======================================================================
# Interactive views

class AlertExplorer( LoginRequiredMixin, django.views.View ):

    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        pass

# ======================================================================
# ======================================================================
# ======================================================================
                                                              
class Testing( PermissionRequiredMixin, django.views.View ):

    def has_permission( self ):
        if self.request.method == 'PUT':
            return self.request.user.has_perms( "elasticc.elasticc_broker" )
        else:
            return bool(self.request.user.is_authenticated)

    def get( self, request, info=None ):
        text = "<!DOCTYPE html>\n<html>\n<body>\n"
        text += "<h3>Testing Get</h3>\n"
        text += f"<p>Path thingy: \"{request.path_info}\"</p>\n"
        text += f"<p>Info is integer?: \"{isinstance(info, int)}\"</p>\n"
        text += f"<p>info: \"{info}\"</p>\n"
        text += f"<p>{len(request.GET)} GET parameters:</p>\n<ul>\n"
        for key, val in request.GET.items():
            text += f"<li>\"{key}\" = \"{val}\"</li>\n"
        text += f"</ul>\n<p>{len(request.POST)} POST parameters:</p>\n<ul>\n"
        for key, val in request.POST.items():
            text += f"<li>\"{key}\" = \"{val}\"</li>\n"
        text += "</ul>\n</body></html>"
        return HttpResponse( text )

    def post( self, request ):
        return self.get( request )
            
