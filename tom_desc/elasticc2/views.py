import sys
import re
import pathlib
import datetime
import json
import logging

import django.db
import django.views
import django.forms.models
from django.db import transaction, connection
from django.db.models import Q
from django.http import HttpResponse, JsonResponse, HttpResponseForbidden, HttpResponseBadRequest
from django.shortcuts import render, get_object_or_404
from django.contrib.auth.mixins import PermissionRequiredMixin, LoginRequiredMixin
from django.template import loader

import rest_framework

from elasticc2.models import PPDBDiaObject, PPDBDiaSource, PPDBDiaForcedSource, PPDBAlert, DiaObjectTruth
from elasticc2.models import DiaObject, DiaSource, DiaForcedSource, BrokerClassifier, CassBrokerMessageBySource
from elasticc2.serializers import PPDBDiaObjectSerializer, PPDBDiaSourceSerializer, PPDBDiaForcedSourceSerializer

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

class Elasticc2MainView( LoginRequiredMixin, django.views.View ):
    """ELAsTiCC2 front page (HTML)."""
    
    def get( self, request ):
        templ = loader.get_template( "elasticc2/elasticc2.html" )
        return HttpResponse( templ.render( {}, request ) )

# ======================================================================

class Elasticc2KnownClassifiers( LoginRequiredMixin, django.views.View ):
    """Show a list of ELAsTiCC2 known classifiers (HTML)."""

    def get( self, request ):
        templ = loader.get_template( 'elasticc2/classifiers.html' )

        cfers = list( BrokerClassifier.objects.all() )
        cfers.sort( key=lambda x : f"{x.brokername}{x.brokerversion}{x.classifiername}{x.classifierparams}" )

        context = { "cfers": [ { 'id': x.classifier_id,
                                 'brokername': x.brokername,
                                 'brokerversion': x.brokerversion,
                                 'classifiername': x.classifiername,
                                 'classifierparams': x.classifierparams }
                               for x in cfers ] }
        return HttpResponse( templ.render( context, request ) )


# ======================================================================

class Elasticc2AdminSummary( PermissionRequiredMixin, django.views.View ):
    """An admin summary for ELAsTiCC2 (HTML).

    Requires the elasticc.elasticc_admin permission.
    """
    
    permission_required = 'elasticc.elasticc_admin'
    raise_exception = True

    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        templ = loader.get_template( "elasticc2/admin_summary.html" )
        context = { "testing": "Hello, world!" }

        context['tabcounts'] = []
        # context['tabcounts'] = [ { 'name': 'blah', 'count': 42 } ]
        for tab in [ PPDBDiaObject, PPDBDiaSource, PPDBDiaForcedSource, DiaObjectTruth, PPDBAlert,
                     DiaObject, DiaSource, DiaForcedSource ]:
            context['tabcounts'].append( { 'name': tab.__name__,
                                           'count': tab.objects.count() } )
            if tab == PPDBAlert:
                notdated = tab.objects.filter( alertsenttimestamp=None ).count()
                dated = tab.objects.filter( ~Q(alertsenttimestamp=None) ).count()
                context['tabcounts'][-1]['sent'] = dated
                context['tabcounts'][-1]['unsent'] = notdated
        # _logger.info( f'context = {context}' )

        with connection.cursor() as cursor:
            cursor.execute( 'SELECT COUNT(o.diaobject_id),t.gentype,m.classid,m.description '
                            'FROM elasticc2_diaobject o '
                            'LEFT JOIN elasticc2_diaobjecttruth t ON t.diaobject_id=o.diaobject_id '
                            'LEFT JOIN elasticc2_gentypeofclassid m ON m.gentype=t.gentype '
                            'GROUP BY t.gentype,m.classid,m.description '
                            'ORDER BY m."classid"' )
            rows=cursor.fetchall()
            context['objtypecounts'] = rows

        return HttpResponse( templ.render( context, request ) )

# ======================================================================    

class Elasticc2AlertStreamHistograms( LoginRequiredMixin, django.views.View ):
    """Show histograms of streaming rate sending out ELAsTiCC2 alerts."""
    
    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        templ = loader.get_template( "elasticc2/alertstreamhists.html" )
        context = { 'weeks': {} }

        tmpldir = pathlib.Path(__file__).parent / "static/elasticc2/alertstreamhists"
        _logger.debug( f"Looking in directory {tmpldir}" )
        files = list( tmpldir.glob( "*.svg" ) )
        files.sort()
        _logger.debug( f"Found {len(files)} files" )
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

        # _logger.info( f"Context is: {context}" )
        return HttpResponse( templ.render( context, request ) )


# ======================================================================

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

# ======================================================================
    
class Elasticc2BrokerTimeDelayGraphs( LoginRequiredMixin, django.views.View, BrokerSorter ):
    def get( self, request, info=None ):
        return self.post( request, info )

    def post( self, request, info=None ):
        templ = loader.get_template( "elasticc2/brokerdelaygraphs.html" )
        context = { 'brokers': [] }
        graphdir = pathlib.Path(__file__).parent / "static/elasticc2/brokertiminggraphs"
        try:
            with open( graphdir / "updatetime.txt" ) as ifp:
                context['updatetime'] = ifp.readline().strip()
        except FileNotFoundError:
            context['updatetime'] = "(unknown)"
        files = list( graphdir.glob( "*.svg" ) )
        files.sort()
        weekmatch = re.compile( '^(.*)-(\d{4}-\d{2}-\d{2})\.svg$' )
        summedmatch = re.compile( '^(.*)-cumulative\.svg$' )
        brokers = set()
        for fname in files:
            match = summedmatch.search( fname.name )
            if match is not None:
                brokers.add( match.group(1) )
        brokers = list(brokers)
        brokers.sort()

        context['brokers'] = {}
        
        for broker in brokers:
            context['brokers'][broker] = { 'sum': f'{broker}-cumulative.svg', 'weeks': {} }
            for fname in files:
                match = weekmatch.search( fname.name )
                if ( match is not None ) and ( match.group(1) == broker ):
                    week = match.group(2)
                    context['brokers'][broker]['weeks'][week] = fname.name

        # ****
        sys.stderr.write( f"context = {context}\n" )
        # ****
        return HttpResponse( templ.render( context, request ) )
    

# ======================================================================
# DJango REST interfaces

class PPDBDiaObjectViewSet( rest_framework.viewsets.ReadOnlyModelViewSet ):
    """REST interface to show PPDBDiaSource (read only).

    /ppdbdiaobject will return a paginated list of objects (json)
    /ppdbdiaobject/<diaobjectid:int> will return a json dump of the info for one object

    """
    permission_classes = [ rest_framework.permissions.IsAuthenticated ]
    queryset = PPDBDiaObject.objects.all()
    serializer_class = PPDBDiaObjectSerializer

class PPDBDiaSourceViewSet( rest_framework.viewsets.ReadOnlyModelViewSet ):
    """REST interface to show PPDBDiaSource (read only).

    /ppdbdiasource will return a paginated list of sources (json)
    /ppdbdiasource/<diaobjectid:int> will return a json dump of the info for one source

    """
    permission_classes = [ rest_framework.permissions.IsAuthenticated ]
    queryset = PPDBDiaSource.objects.all()
    serializer_class = PPDBDiaSourceSerializer

class PPDBDiaForcedSourceViewSet( rest_framework.viewsets.ReadOnlyModelViewSet ):
    """REST interface to show PPDBDiaForcedSource (read only).

    /ppdbdiasforcedource will return a paginated list of forced sources (json)
    /ppdbdiaforcedsource/<diaobjectid:int> will return a json dump of the info for one forced source

    """
    permission_classes = [ rest_framework.permissions.IsAuthenticated ]
    queryset = PPDBDiaForcedSource.objects.all()
    serializer_class = PPDBDiaForcedSourceSerializer

class PPDBDiaObjectSourcesViewSet( rest_framework.viewsets.ReadOnlyModelViewSet ):
    """REST interface to return one PPBDDiaObject and all associated sources (read only).

    /ppdbdiaobjectwithsources/<diaobjectid:int> returns json
    """
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
        srcs = PPDBDiaSource.objects.filter( diaobject_id=pk )
        objdict[ 'diasources' ] = []
        for src in srcs:
            ser = PPDBDiaSourceSerializer( src )
            objdict[ 'diasources' ].append( ser.data )
        objdict[ 'diaforcedsources' ] = []
        frcsrcs = PPDBDiaForcedSource.objects.filter( diaobject_id=pk )
        for src in frcsrcs:
            ser = PPDBDiaForcedSourceSerializer( src )
            objdict[ 'diaforcedsources' ].append( ser.data )
        return rest_framework.response.Response( objdict )

class PPDBDiaObjectAndPrevSourcesForSourceViewSet( rest_framework.viewsets.ReadOnlyModelViewSet ):
    """REST interface to return the PPDBDiaObject, plus all previous sources and forced sources, for a given source.

    /ppdbdiaobjectofsource/<diasourceid:int> - returns JSON with the
    PPDBDiaObject of the specified source, plus all sources and forced
    sources that are earlier than the specified source.

    """
    permission_classes = [ rest_framework.permissions.IsAuthenticated ]
    queryset = PPDBDiaSource.objects.all()
    serializer_class = None

    def list( self, request, pk=None ):
        return rest_framework.response.Response( status=rest_framework.status.HTTP_400_BAD_REQUEST,
                                                 data="Must give a source id" )

    def retrieve( self, request, pk=None ):
        sys.stderr.write( f"Trying to get source {pk}" )
        src = get_object_or_404( PPDBDiaSource.objects.all(), pk=pk )
        sys.stderr.write( f"Trying to get object {src.diaobject_id}" )
        obj = get_object_or_404( PPDBDiaObject.objects.all(), pk=src.diaobject_id )
        objserializer = PPDBDiaObjectSerializer( obj )
        objdict = dict( objserializer.data )
        objdict[ 'diasources' ] = []
        objdict[ 'diaforcedsources' ] = []
        srcs = ( PPDBDiaSource.objects
                 .filter( diaobject_id=src.diaobject_id )
                 .filter( midpointtai__lte=src.midpointtai ) )
        frcsrcs = ( PPDBDiaForcedSource.objects
                    .filter( diaobject_id=src.diaobject_id )
                    .filter( midpointtai__lte=src.midpointtai ) )
        for src in srcs:
            ser = PPDBDiaSourceSerializer( src )
            objdict[ 'diasources' ].append( ser.data )
        for src in frcsrcs:
            ser = PPDBDiaForcedSourceSerializer( src )
            objdict[ 'diaforcedsources' ].append( ser.data )
        return rest_framework.response.Response( objdict )
    
# ======================================================================
# A REST interface (but not using the Django REST framework) for
# viewing and posting broker messages.  Posting broker messages
# (via the PUT method) requires the elasticc2.elasticc_broker
# permission.

        
class BrokerMessageView(PermissionRequiredMixin, django.views.View):
    """A REST interface for getting and putting broker messages.
    
    Requires elasticc.elasticc_broker permission for PUT, just logged in for GET.

    GET or POST : TBD

    PUT : add a new batch of broker classifications.  The data in the
    body is expected to be text in JSON format.  It must be either a
    single dictionary, or a list of dictionaries.  EAch dictionary must
    corresp to the brokerClassification schema version 0.9.1 (see
    https://github.com/LSSTDESC/elasticc/tree/main/alert_schema),
    ***with the additional field*** brokerPublishTimestamp that gives a
    standard AVRO-style timestamp (big intger milliseconds since the
    Epoch) for when the broker had completed the classification and was
    about to post it to the API.  (This corresponds to the Kafka message
    header timestamp for brokers that publish to a Kafka stream, which
    the DESC TOM then pulls.)

    """

    raise_exception = True

    def has_permission( self ):
        if self.request.method == 'PUT':
            return self.request.user.has_perm( "elasticc.elasticc_broker" )
        else:
            return bool(self.request.user.is_authenticated)

    def get_queryset( self, request, info, offset=0, num=100 ):
        raise RuntimeError( "This is broken, fix for CassBrokerMessageBySource." )
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
                    cursor.execute( 'SELECT COUNT(b."brokerMessageId") FROM elasticc2_brokermessage b'
                                    ' INNER JOIN elasticc2_diasource s ON b."diaSourceId"=s."diaSourceId"'
                                    ' WHERE s."diaObjectId" IN %(objids)s', params=params )
                    row = cursor.fetchone()
                    n = row[0]
                msgs = BrokerMessage.objects.raw(
                    'SELECT * FROM elasticc2_brokermessage b INNER JOIN elasticc2_diasource s'
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
        # Reformulate the data array into what CassBrokerMessageBySource.load_batch is expecting
        _logger.debug( f"BrokerMessageView.put: len(data)={len(data)}" )
        for datum in data:
            datum['elasticcPublishTimestamp'] = datetime.datetime.fromtimestamp( datum['elasticcPublishTimestamp']
                                                                                 / 1000,
                                                                                 tz=datetime.timezone.utc )
            datum['brokerIngestTimestamp'] = datetime.datetime.fromtimestamp( datum['brokerIngestTimestamp']
                                                                              / 1000,
                                                                              tz=datetime.timezone.utc )
            if 'brokerPublishTimestamp' in datum:
                pubtime = datetime.datetime.fromtimestamp( datum['brokerPublishTimestamp'] / 1000,
                                                           tz=datetime.timezone.utc )
                del datum['brokerPublishTimestamp']
            else:
                pubtime = None
            messageinfo.append( { 'topic': 'REST_push',
                                  'timestamp': pubtime,
                                  'msgoffset': -1,
                                  'msg': datum } )
            

        _logger.debug( f"BrokerMessageView.put: load_batch of {len(messageinfo)} messages." )
        batchret = CassBrokerMessageBySource.load_batch( messageinfo, logger=_logger )
        dex = -1 if batchret['firstbrokermessage_id'] is None else batchret['firstbrokermessage_id']
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
