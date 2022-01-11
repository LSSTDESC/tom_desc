import json
from django.http import HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required, permission_required
from django.shortcuts import render
from django.db.models import F
import django.views
from rest_framework import pagination
from rest_framework import permissions
from rest_framework import viewsets

from stream.filters import AlertFilter, EventFilter, TopicFilter
from stream.models import Alert, Event, Target, Topic, RknopTest
from stream.models import ElasticcDiaObject, ElasticcSSObject, ElasticcDiaSource, ElasticcAlert
from stream.serializers import AlertSerializer, EventDetailSerializer, EventSerializer, TargetSerializer, TopicSerializer
from stream.serializers.v1 import serializers as v1_serializers


class TargetViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows targets to be viewed or edited.
    """
    # TODO: should we order Targets ?
    queryset = Target.objects.all()
    serializer_class = TargetSerializer
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = pagination.PageNumberPagination


class AlertViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """
    filterset_class = AlertFilter
    # permission_classes = [permissions.IsAuthenticated]
    queryset = Alert.objects.all()
    serializer_class = AlertSerializer

    class Meta:
        # https://docs.djangoproject.com/en/dev/ref/models/options/#ordering
        ordering = [F('alert_timestamp').desc(nulls_last=True), F('timestamp').desc(nulls_last=True)]

    def get_serializer_class(self):
        if self.request.version in ['v0', 'v1']:
            return v1_serializers.AlertSerializer
        return AlertSerializer


class TopicViewSet(viewsets.ModelViewSet):
    filterset_class = TopicFilter
    # permission_classes = [permissions.IsAuthenticated]
    queryset = Topic.objects.all()
    serializer_class = TopicSerializer


class EventViewSet(viewsets.ModelViewSet):
    filterset_class = EventFilter
    queryset = Event.objects.all()

    def get_serializer_class(self):
        if self.action == 'retrieve':
            return EventDetailSerializer
        return EventSerializer

# ======================================================================


@method_decorator(login_required, name='dispatch')
class DumpRknopTest(django.views.View):
    def get(self, request, *args, **kwargs):
        them = RknopTest.objects.all()
        ret = []
        for it in them:
            ret.append( { "number": it.number, "description": it.description } )
        return HttpResponse(json.dumps(ret))
        


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
