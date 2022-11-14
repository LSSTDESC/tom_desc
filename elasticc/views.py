import sys
import pathlib
import re
import io
import traceback
import json
import time
import datetime
import logging
import django.db
from django.db.models import Q
from django.db import transaction, connection
import django.urls
from django.http import HttpResponse, JsonResponse
from django.template import loader
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
# _logger.setLevel( logging.INFO )
_logger.setLevel( logging.DEBUG )

# ======================================================================

class ElasticcMainView( LoginRequiredMixin, django.views.View ):
    def get( self, request ):
        templ = loader.get_template( "elasticc/elasticc.html" )
        return HttpResponse( templ.render( {}, request ) )

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
# ======================================================================
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

        # Load the alert
        # TODO : check to see if the alertId already exists???
        # Right now, this will error out due to the unique constraint
        # t0 = time.perf_counter()
        curalert = DiaAlert( alertId = data['alertId'], diaSource = cursrc, diaObject = curobj )
        # curalert.save()
        # self.alertloadtime += time.perf_counter() - t0

        # Load linkages to the previouses.  I'm not sure I really want to
        #   do this.  These will become very big tables, and we could
        #   figure it all out algorithmically.
        # if True:
        #     for prv in prevs:
        #         tmp = DiaAlertPrvSource( diaAlert=curalert, diaSource=prv )
        #         tmp.save()
        #     for prv in forced:
        #         tmp = DiaAlertPrvForcedSource( diaAlert=curalert, diaForcedSource=prv )
        #         tmp.save()
                          
        return { 'alertId': curalert.alertId,
                 }
                 # 'diaSourceId': cursrc.diaSourceId,
                 # 'diaObjectId': curobj.diaObjectId,
                 # 'prvDiaSourceIds': prevs,
                 # 'prvDiaForcedSourceIds': forced }
        
    def post(self, request, *args, **kwargs):
        try:
            data = json.loads( request.body )
            if isinstance( data, dict ):
                data = [ data ]
            loaded = {}

            # Load the objects
            _logger.info( "Loading objects." )
            objstoload = [ entry['diaObject'] for entry in data ]
            if len(objstoload) > 0:
                nobjectsadded = DiaObject.bulk_insert_onlynew( objstoload  )
            else:
                nobjectsadded = 0
            loaded[ 'objects' ] = nobjectsadded
                    
            # Load the sources
            # "seen" is because the same prvdiasource probably shows up in multiple alerts we're loading
            seen = set()
            sources = [ ]
            for alert in data:
                seen.add( alert['diaSource']['diaSourceId'] )
                sources.append( alert['diaSource'] )
                if alert['prvDiaSources'] is not None:
                    for prvdiasource in alert['prvDiaSources']:
                      if prvdiasource['diaSourceId'] not in seen:
                          seen.add( prvdiasource['diaSourceId'] )
                          sources.append( prvdiasource )
            _logger.info( f"Loading {len(sources)} sources..." )
            if len(sources) > 0:
                nsourcesadded = DiaSource.bulk_insert_onlynew( sources )
            else:
                nsourcesadded = 0
            loaded[ 'sources' ] = nsourcesadded
            _logger.info( f"...done loading sources" )
            
            # Load the forced sources
            seen = set()
            sources = []
            for alert in data:
                if alert['prvDiaForcedSources'] is not None:
                    for prvdiaforcedsource in alert['prvDiaForcedSources']:
                        if prvdiaforcedsource['diaForcedSourceId'] not in seen:
                            seen.add( prvdiaforcedsource['diaForcedSourceId'] )
                            sources.append( prvdiaforcedsource )
            _logger.info( f"Loading {len(sources)} previous sources..." )
            if len(sources) > 0:
                nforcedsourcesadded = DiaForcedSource.bulk_insert_onlynew( sources )
            else:
                nforcedsourcesadded = 0
            loaded[ 'forcedsources' ] = nforcedsourcesadded
            _logger.info( f"...done loading previous sources" )
                                
            # Load the alerts
            alerts = []
            for alert in data:
                alerts.append( { 'alertId': alert['alertId'],
                                 'diaObjectId': alert['diaObject']['diaObjectId'],
                                 'diaSourceId': alert['diaSource']['diaSourceId'],
                                 'alertSentTimestamp': None } )
            if len(alerts) > 0:
                nalertsadded = DiaAlert.bulk_insert_onlynew( alerts )
            else:
                nalertsadded = 0
            loaded[ 'alerts' ] = nalertsadded
                
            # Not going to load the previous source and previous forced
            # source linkages.  Those tables are huge, and it's
            # additional data stored and time loading that probably
            # isn't worth it, as we can reconstruct all of that
            # algorithmically.
            
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
                loaded.append( DiaObjectTruth.load_or_create( data ) ).diaObject_id
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

# @method_decorator(login_required, name='dispatch')
class MarkAlertSent(PermissionRequiredMixin, django.views.View):
    permission_required = 'elasticc.elasticc_admin'
    raise_exception = True

    def post( self, request, *args, **kwargs ):
        try:
            now = datetime.datetime.now( tz=datetime.timezone.utc )
            ids = []
            data = json.loads( request.body )
            if isinstance( data, dict ):
                data = [ data ]
            for datum in data:
                alert = DiaAlert.objects.get( pk=datum )
                alert.alertSentTimestamp = now
                alert.save()
                ids.append( datum )
            return JsonResponse( { 'status': 'ok',
                                   'alertIds': ids,
                                   'timestamp': now.isoformat() } )
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
# ======================================================================
# ======================================================================
# API interfaces for getting stuff

def get_alerts( offset, num, truth=False ):
    with connection.cursor() as cursor:
        cursor.execute( 'SELECT DISTINCT ON (a."alertId") a."alertId",o."diaObjectId",'
                        's."diaSourceId",s."midPointTai",ps."diaSourceId" AS firstsource,'
                        'ps."midPointTai" AS firstmjd FROM elasticc_diaalert a '
                        'INNER JOIN elasticc_diaobject o ON a."diaObjectId"=o."diaObjectId" '
                        'INNER JOIN elasticc_diasource s ON a."diaSourceId"=s."diaSourceId" '
                        'INNER JOIN elasticc_diasource ps ON ps."diaObjectId"=o."diaObjectId" '
                        'ORDER BY a."alertId",ps."midPointTai" LIMIT %(num)s OFFSET %(offset)s',
                        { "num": num, "offset": offset } )
        rows = cursor.fetchall()
    # I wish there were a dict cursor
    alertid_dex = 0
    diaobjectid_dex = 1
    diasourceid_dex = 2
    midpointtai_dex = 3
    firstsource_dex = 4
    firstmjd_dex = 5
    alerts = []
    truthyness = []
    objids = []
    for row in rows:
        alertobj = DiaAlert.objects.get( pk=row[alertid_dex] )
        prvsources = ( DiaSource.objects
                       .filter( diaObjectId=alertobj.diaObject.diaObjectId )
                       .filter( midPointTai__lt=alertobj.diaSource.midPointTai ) )
        prvforced = ( [] if alertobj.diaSource.midPointTai < row[firstmjd_dex] + 0.5
                      else ( DiaForcedSource.objects
                             .filter( diaObjectId=alertobj.diaObject.diaObjectId )
                             .filter( midPointTai__gt=row[firstmjd_dex]-30 )
                             .filter( midPointTai__lt=alertobj.diaSource.midPointTai ) ) )
        alert = { 'alertId': alertobj.alertId,
                  'diaSource': alertobj.diaSource.to_dict(),
                  'prvDiaSources': [ s.to_dict() for s in prvsources ],
                  'prvDiaForcedSources': [ s.to_dict() for s in prvforced ],
                  'prvDiaNondetectionLimits': None,
                  'diaObject': alertobj.diaObject.to_dict(),
                  'cutoutDifference': None,
                  'cutoutTemplate': None }
        if ( alertobj.diaSource.midPointTai >= row[firstmjd_dex] + 0.5 ):
            alert['prvDiaForcedSources'] = list( 
            )
        alerts.append( alert )
        if alertobj.diaObject.diaObjectId not in objids:
            objids.append( alertobj.diaObject.diaObjectId )
        if truth:
            # _logger.info( f"About to try to get DiaTruth pk {alertobj.diaSource.diaSourceId}" )
            truthobj = DiaTruth.objects.get( pk=alertobj.diaSource.diaSourceId )
            truthyness.append( truthobj.to_dict() )

    objtruth = {}
    if truth:
        for objid in objids:
            objtruthobj = DiaObjectTruth.objects.get( pk=objid )
            objtruth[ objid ] = objtruthobj.to_dict()

    return alerts, truthyness, objtruth

class GetAlerts(LoginRequiredMixin, django.views.View):
    def get( self, request, *args, **kwargs ):
        return self.post( request, *args, **kwargs )

    def post( self, request, *args, **kwargs ):
        try:
            data = json.loads( request.body )
            # TODO : specific alertid
            offset = int( data['offset'] )
            num = int( data['num'] )
            totnum = DiaAlert.objects.count()
            if offset > totnum:
                return JsonResponse( { 'status': 'ok',
                                       'offset': offset,
                                       'num': 0,
                                       'totnum': totnum,
                                       'alerts': [] } )

            alerts, junk, morejunk = get_alerts( offset, num, False )
            return JsonResponse( { 'status': 'ok',
                                   'offset': offset,
                                   'num': len(alerts),
                                   'totnum': totnum,
                                   'alerts': alerts } )
            
        except Exception as e:
            strstream = io.StringIO()
            traceback.print_exc( file=strstream )
            resp = { 'status': 'error',
                     'message': 'Exception in GetAlerts',
                     'exception': str(e),
                     'traceback': strstream.getvalue() }
            strstream.close()
            return JsonResponse( resp )

class GetAlertsAndTruth(LoginRequiredMixin, django.views.View):
    def get( self, request, *args, **kwargs ):
        return self.post( request, *args, **kwargs )

    def post( self, request, *args, **kwargs ):
        try:
            data = json.loads( request.body )
            # TODO : specific alertid
            offset = int( data['offset'] )
            num = int( data['num'] )
            totnum = DiaAlert.objects.count()
            if offset > totnum:
                return JsonResponse( { 'status': 'ok',
                                       'offset': offset,
                                       'num': 0,
                                       'totnum': totnum,
                                       'alerts': [],
                                       'truth': [],
                                       'objecttruth': {} } )

            alerts, truth, objecttruth = get_alerts( offset, num, True )
            return JsonResponse( { 'status': 'ok',
                                   'offset': offset,
                                   'num': len(alerts),
                                   'totnum': totnum,
                                   'alerts': alerts,
                                   'truth': truth,
                                   'objecttruth': objecttruth } )
            
        except Exception as e:
            strstream = io.StringIO()
            traceback.print_exc( file=strstream )
            resp = { 'status': 'error',
                     'message': 'Exception in GetAlerts',
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
            

        addedmsgs = BrokerMessage.load_batch( messageinfo, logger=_logger )

        dex = addedmsgs[0].brokerMessageId if len(addedmsgs) > 0 else -1
        resp = JsonResponse( { 'brokerMessageId': dex,
                               'num_loaded': len(addedmsgs) }, status=201 )
        # I really wish there were a django function for this, as I'm not sure that
        # my string replace will always do the right thing.  What I'm after is the
        # url of the current view, but without any parameters passed
        fullpath = request.build_absolute_uri()
        loc = re.sub( '\?.*$', '', fullpath )
        if loc[-1] != "/":
            loc += "/"
        resp.headers['Location'] =  f'{loc}{dex}'


        # # Make the BrokerMessage object
        # # This could lead to duplication; if the same classification
        # # is sent more than once from a broker, it will get saved multiple
        # # times.
        # msgobj = BrokerMessage(
        #     alertId = data['alertId'],
        #     diaSourceId = data['diaSourceId'],
        #     # Note: an IEEE double can hold ~16 digits of precision
        #     # Given that it's ~10⁹ seconds since the Epoch now, the
        #     # IEEE double should be able to hold the number of microseconds
        #     # with integrity.  So, just divide by 1e6 to go from number of
        #     # microseconds to POSIX timestamp.
        #     elasticcPublishTimestamp = datetime.datetime.fromtimestamp( data['elasticcPublishTimestamp']/1e6,
        #                                                                 tz=datetime.timezone.utc ),
        #     brokerIngestTimestamp = datetime.datetime.fromtimestamp( data['brokerIngestTimestamp']/1e6,
        #                                                              tz=datetime.timezone.utc )
        # )
        # msgobj.save()


        # # Next, dig in, and make sure we know all the BrokerClassifier things
        # broker_name = data['brokerName']
        # broker_version = data['brokerVersion']
        # classifiers = {}
        # for classification in data['classifications']:
        #     # There's the _possibility_ of confusion here, but I'm going to write
        #     #  it off as not very likely.
        #     # (The ghost of Bobby Tables will haunt me forever.)
        #     cferkey = f'Name: {classification["classifierName"]} Params: {classification["classifierParams"]}'
        #     if cferkey in classifiers.keys():
        #         continue
        #     curcfers = BrokerClassifier.objects.all().filter(
        #         brokerName = broker_name,
        #         brokerVersion = broker_version,
        #         classifierName = classification['classifierName'],
        #         classifierParams = classification['classifierParams'] )
        #     if len(curcfers) > 0:
        #         cfer = curcfers[0]
        #     else:
        #         cfer = BrokerClassifier(
        #             brokerName = broker_name,
        #             brokerVersion = broker_version,
        #             classifierName = classification['classifierName'],
        #             classifierParams = classification['classifierParams'] )
        #         cfer.save()
        #     classifiers[cferkey] = cfer

        # # Now, save all the classifications
        # for classification in data['classifications']:
        #     cfer = classifiers[f'Name: {classification["classifierName"]} '
        #                        f'Params: {classification["classifierParams"]}']
        #     cfication = BrokerClassification(
        #         dbMessage = msgobj,
        #         dbClassifier = cfer,
        #         classId = classification['classId'],
        #         probability = classification['probability'] )
        #     cfication.save()

        # resp = JsonResponse( { 'dbMessageIndex': msgobj.dbMessageIndex }, status=201 )
        # # I really wish there were a django function for this, as I'm not sure that
        # # my string replace will always do the right thing.  What I'm after is the
        # # url of the current view, but without any parameters passed
        # fullpath = request.build_absolute_uri()
        # loc = re.sub( '\?.*$', '', fullpath )
        # if loc[-1] != "/":
        #     loc += "/"
        # resp.headers['Location'] =  f'{loc}{msgobj.dbMessageIndex}'

        return resp

# ======================================================================
# ======================================================================
# ======================================================================
# Interactive views

class BrokerSorter:
    def getbrokerstruct( self ):
        brokers = {}
        cfers = BrokerClassifier.objects.all().order_by( 'brokerName', 'brokerVersion',
                                                         'classifierName', 'classifierParams' )
        # There's probably a faster pythonic way to make
        # a hierarchy like this, but oh well.  This works.
        curbroker = None
        curversion = None
        curcfer = None
        for cfer in cfers:
            if cfer.brokerName != curbroker:
                curbroker = cfer.brokerName
                curversion = cfer.brokerVersion
                curcfer = cfer.classifierName
                brokers[curbroker] = {
                    curversion: {
                        curcfer: [ [ cfer.classifierParams, cfer.classifierId ] ]
                    }
                }
            elif cfer.brokerVersion != curversion:
                curversion = cfer.brokerVersion
                curcfer = cfer.classifierName
                brokers[curbroker][curversion] = {
                    curcfer: [ [ cfer.classifierParams, cfer.classifierId ] ] }
            elif cfer.classifierName != curcfer:
                curcfer = cfer.classifierName
                brokers[curbroker][curversion][curcfer] = [ [ cfer.classifierParams, cfer.classifierId ] ]
            else:
                brokers[curbroker][curversion][curcfer].append( [ cfer.classifierParams, cfer.classifierId ] )

        return brokers

class AlertExplorer( LoginRequiredMixin, django.views.View ):

    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        pass

# ======================================================================
    
class ElasticcSummary( LoginRequiredMixin, django.views.View ):
                       
    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        templ = loader.get_template( "elasticc/summary.html" )
        context = { 'tabcounts': [] }

        for tab in [ DiaAlert, DiaObject, DiaSource, DiaForcedSource ]:
            context['tabcounts'].append( { 'name': tab.__name__,
                                           'count': tab.objects.count() } )
            if tab == DiaAlert:
                notdated = tab.objects.filter( alertSentTimestamp=None ).count()
                dated = tab.objects.filter( ~Q(alertSentTimestamp=None) ).count()
                context['tabcounts'][-1]['sent'] = dated
                context['tabcounts'][-1]['unsent'] = notdated

        # Get all of the classifiers.  I found that later just grouping on
        # classifierId was faster than a join and grouping on the four
        # elements of classifiers.  (Perhaps no surprise, but I had thought
        # better of PostgreSQL.)

        classifiers = list( BrokerClassifier.objects.all() )
        # classifiers.sort( key=lambda e: ( e.brokerName, e.brokerVersion, e.classifierName, e.classifierParams ) )
        classifiers = { c.classifierId: c for c in classifiers }
        
        context['brokers'] = []
        brokermap = {}
        with connection.cursor() as cursor:
            _logger.debug( "Getting brokers & numbers of classifications." )
            cursor.execute( 'SELECT "classifierId", COUNT("classificationId") as n '
                            'FROM elasticc_brokerclassification '
                            'GROUP BY "classifierId"' )
            rows = cursor.fetchall()
            for i, row in enumerate(rows):
                context['brokers'].append( { 'brokerName': classifiers[row[0]].brokerName,
                                             'brokerVersion': classifiers[row[0]].brokerVersion,
                                             'classifierName': classifiers[row[0]].classifierName,
                                             'classifierParams': classifiers[row[0]].classifierParams,
                                             'nclassifications': row[1],
                                             'nbadalertid': 0,
                                             'nstreamedclassified': 0 } )
                brokermap[ row[0] ] = i
            # _logger.debug( "Looking for bad alert IDs." )
            # cursor.execute( 'SELECT c."classifierId",COUNT(c."classificationId") as n '
            #                 'FROM elasticc_brokerclassification c '
            #                 'INNER JOIN elasticc_brokermessage m ON c."brokerMessageId"=m."brokerMessageId" '
            #                 'LEFT JOIN elasticc_diaalert a ON a."alertId"=m."alertId" '
            #                 'WHERE a."alertId" IS NULL '
            #                 'GROUP BY c."classifierId"' )
            # rows = cursor.fetchall()
            # for row in rows:
            #     dex = brokermap[ row[0] ]
            #     context['brokers'][dex]['nbadalertid'] = row[1]
            # _logger.debug( "Done looking for bad alert IDs." )

        context['brokers'].sort( key=lambda e: ( e['brokerName'], e['brokerVersion'],
                                                 e['classifierName'], e['classifierParams'] ) )

        return HttpResponse( templ.render( context, request ) )
                                
# ======================================================================
    
class ElasticcAdminSummary( PermissionRequiredMixin, django.views.View ):
    permission_required = 'elasticc.elasticc_admin'
    raise_exception = True

    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        templ = loader.get_template( "elasticc/admin_summary.html" )
        context = { "testing": "Hello, world!" }

        context['tabcounts'] = []
        # context['tabcounts'] = [ { 'name': 'blah', 'count': 42 } ]
        for tab in [ DiaObject, DiaSource, DiaForcedSource,
                     DiaAlert, DiaTruth, DiaObjectTruth ]:
            context['tabcounts'].append( { 'name': tab.__name__,
                                           'count': tab.objects.count() } )
            if tab == DiaAlert:
                notdated = tab.objects.filter( alertSentTimestamp=None ).count()
                dated = tab.objects.filter( ~Q(alertSentTimestamp=None) ).count()
                context['tabcounts'][-1]['sent'] = dated
                context['tabcounts'][-1]['unsent'] = notdated
        # _logger.info( f'context = {context}' )

        with connection.cursor() as cursor:
            cursor.execute( 'SELECT COUNT(o."diaObjectId"),t.gentype,m."classId",m.description '
                            'FROM elasticc_diaobject o '
                            'LEFT JOIN elasticc_diaobjecttruth t ON t."diaObjectId"=o."diaObjectId" '
                            'LEFT JOIN elasticc_gentypeofclassid m ON m.gentype=t.gentype '
                            'GROUP BY t.gentype,m."classId",m.description '
                            'ORDER BY m."classId"' )
            rows=cursor.fetchall()
            context['objtypecounts'] = rows

        return HttpResponse( templ.render( context, request ) )
        
# ======================================================================
# This class is poorly named.  It does not show histograms.

class ElasticcAlertStreamHistograms( LoginRequiredMixin, django.views.View ):

    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        templ = loader.get_template( "elasticc/alertstreamhists.html" )
        context = { 'weeks': {} }

        tmpldir = pathlib.Path(__file__).parent / "static/elasticc/alertstreamhists"
        files = list( tmpldir.glob( "*.svg" ) )
        files.sort()
        fnamematch = re.compile( "^([0-9]{4})-([0-9]{2})-([0-9]{2})\.svg$" )
        for fname in files:
            match = fnamematch.search( fname.name )
            if match is not None:
                date = datetime.date( int(match.group(1)), int(match.group(2)), int(match.group(3)) )
                year, week, weekday = date.isocalendar()
                wk = f"{year} week {week}"
                if wk not in context['weeks']:
                    context['weeks'][wk] = {}
                context['weeks'][wk][date.strftime( "%a %Y %b %d" )] = fname.name

        _logger.info( f"Context is: {context}" )
        return HttpResponse( templ.render( context, request ) )

# ======================================================================

class ElasticcBrokerStreamGraphs( LoginRequiredMixin, django.views.View ):

    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        templ = loader.get_template( "elasticc/brokerstreamrate.html" )
        context = { "brokers": {} }
        fnmatch = re.compile( "^(.*)_(\d{4})-(\d{2})-(\d{2})\.svg$" )

        outdir = pathlib.Path(__file__).parent / "static/elasticc/brokerstreamgraphs"
        files = outdir.glob( "*.svg" )
        brokers = {}
        for fname in files:
            _logger.info( f"Parsing file {fname.name}" )
            match = fnmatch.search( str(fname.name) )
            if match is None:
                continue
            broker = match.group(1)
            if broker not in brokers.keys():
                brokers[broker] = {}
            filedate = datetime.date( int(match.group(2)), int(match.group(3)), int(match.group(4)) )
            week = filedate.isocalendar()[1]
            weekstr = f'{filedate.year} Week {week}'
            if weekstr not in brokers[broker].keys():
                brokers[broker][weekstr] = {}
            brokers[broker][weekstr][filedate.isoformat()] = fname.name

        # Sort these dictionaries
        brokers = dict( sorted( brokers.items() ) )
        for broker in brokers.keys():
            brokers[broker] = dict( sorted( brokers[broker].items() ) )
            for week in brokers[broker]:
                brokers[broker][week] = dict( sorted( brokers[broker][week].items() ) )

        _logger.info( f"Brokers is: {brokers}" )
        return HttpResponse( templ.render( { "brokers": brokers }, request ) )

# ======================================================================

class ElasticcBrokerCompletenessGraphs( LoginRequiredMixin, django.views.View, BrokerSorter ):

    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        templ = loader.get_template( "elasticc/brokercompletenessperweek.html" )
        context = {}
        context['brokers'] = self.getbrokerstruct()
        return HttpResponse( templ.render( context, request ) )

# ======================================================================

class ElasticcBrokerCompletenessVsNumDets( LoginRequiredMixin, django.views.View, BrokerSorter ):

    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        templ = loader.get_template( "elasticc/brokercompletenessvsndets.html" )
        context = {}
        brokers = self.getbrokerstruct()

        plotdir = pathlib.Path(__file__).parent / "static/elasticc/brokercompletenessvsndets"
        for broker, versions in brokers.items():
            for version, cfers in versions.items():
                for cfer,params in cfers.items():
                    for p in params:
                        fp = plotdir / f"{p[1]}.svg"
                        if not fp.exists():
                            p.append( "<File missing>" )
                        else:
                            p.append( datetime.datetime.fromtimestamp( fp.stat().st_mtime )
                                      .strftime( "%Y-%m-%d %H:%M" ) )

        context = { 'brokers': brokers }
        return HttpResponse( templ.render( context, request ) )
        
# ======================================================================

class ElasticcBrokerTimeDelayGraphs( LoginRequiredMixin, django.views.View, BrokerSorter ):
    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        templ = loader.get_template( "elasticc/brokerdelaygraphs.html" )
        context = { 'brokers': [] }
        graphdir = pathlib.Path(__file__).parent / "static/elasticc/brokertiminggraphs"
        with open( graphdir / "updatetime.txt" ) as ifp:
            context['updatetime'] = ifp.readline().strip()
        files = list( graphdir.glob( "*.svg" ) )
        files.sort()
        weekmatch = re.compile( '^(.*)_(\d{4}-\d{2}-\d{2})\.svg$' )
        summedmatch = re.compile( '^(.*)_summed\.svg$' )
        brokers = set()
        for fname in files:
            match = summedmatch.search( fname.name )
            if match is not None:
                brokers.add( match.group(1) )
        brokers = list(brokers)
        brokers.sort()

        context['brokers'] = {}
        
        for broker in brokers:
            context['brokers'][broker] = { 'sum': f'{broker}_summed.svg', 'weeks': {} }
            for fname in files:
                match = weekmatch.search( fname.name )
                if ( match is not None ) and ( match.group(1) == broker ):
                    week = match.group(2)
                    context['brokers'][broker]['weeks'][week] = fname.name
            
        return HttpResponse( templ.render( context, request ) )
    

# ======================================================================

class ElasticcTmpBrokerHistograms( LoginRequiredMixin, django.views.View, BrokerSorter ):

    def get( self, request, info=None ):
        return self.post( request, info )

    def post ( self, request, info=None ):
        templ = loader.get_template( "elasticc/tmp_brokeralerthist.html" )
        context = { 'brokers': {} }
        rundir = pathlib.Path(__file__).parent
        context['brokers'] = self.getbrokerstruct()
        return HttpResponse( templ.render( context, request ) )
        

# ======================================================================

class ElasticcLatestConfMatrix( LoginRequiredMixin, django.views.View, BrokerSorter ):

    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        templ = loader.get_template( "elasticc/basicmetrics.html" )
        context = { 'brokers': {} }
        rundir = pathlib.Path(__file__).parent
        with open( rundir / "static/elasticc/confmatrix_update.txt" ) as ifp:
            context['updatetime'] = ifp.readline().strip()
        context['brokers'] = self.getbrokerstruct()
        return HttpResponse( templ.render( context, request ) )

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
            
