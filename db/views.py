import sys
import os
import json
import psycopg2
import psycopg2.extras
from django.http import HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.shortcuts import render
from django.contrib.auth.mixins import PermissionRequiredMixin, LoginRequiredMixin
import django.views

# ======================================================================
# A low-level query interface.
#
# That is, of course, EXTREMELY scary.  This is why you need to make
# sure the postgres user postgres_ro is a readonly user.  Still scary,
# but not Cthulhuesque.

class RunSQLQuery(LoginRequiredMixin, django.views.View):
    raise_exception = True

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
                                   user='postgres_ro', password=password,
                                   cursor_factory=psycopg2.extras.RealDictCursor )
        # sys.stderr.write( f'Query is {data["query"]}, subdict is {subdict}\n' )
        cursor = dbconn.cursor()
        try:
            cursor.execute( data['query'], subdict )
        except Exception as ex:
            return JsonResponse( { 'status': 'error', 'error': str(ex) } )
        return JsonResponse( { 'status': 'ok', 'rows': cursor.fetchall() } )
        
