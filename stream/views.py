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
        

