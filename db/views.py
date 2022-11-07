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
        if self.request.user.has_perm( "elasticc.elasticc_admin" ):
            dbuser = "postgres_elasticc_admin_ro"
            pwfile = "/secrets/postgres_elasticc_admin_ro_password"
        else:
            dbuser = "postgres_elasticc_ro"
            pwfile = "/secrets/postgres_elasticc_ro_password"
        with open( pwfile ) as ifp:
            password = ifp.readline().strip()
        
        data = json.loads( request.body )
        dbconn = psycopg2.connect( dbname=os.getenv('DB_NAME'), host=os.getenv('DB_HOST'),
                                   user=dbuser, password=password,
                                   cursor_factory=psycopg2.extras.RealDictCursor )
        try:
            if 'queries' in data:
                if not isinstance( data['queries'], list ):
                    raise TypeError( f"queries must be a list, you passed a {type(data['queries'])}" )
                queries = data['queries']
                if 'subdicts' in data:
                    if not isinstance( data['subdicts'], list ):
                        raise TypeError( f"subdicts must be a list, you passed a {type(data['subdicts'])}" )
                    if len( data['subdicts'] ) != len( queries ):
                        raise ValueError( f"Passed {len(queries)} queries but {len(data['subdicts'])} subdicts" )
                    subdicts = data['subdicts']
                else:
                    subdicts = [ {} for i in range(len(queries)) ]

            elif 'query' in data:
                queries = [ data['query'] ]
                if 'subdict' in data:
                    if not isinstance( data['subdict'], dict ):
                        raise TypeError( f"subdict must be a dictionary, you passed a {type(data['subdict'])}" )
                    subdicts = [ data['subdict'] ]

            else:
                raise ValueError( "Must pass either queries or query" )

            # Have to convert lists to tuples in the substitution dictionaries
            for subdict in subdicts:
                for key in subdict.keys():
                    if isinstance( subdict[key], list ):
                        subdict[key] = tuple( subdict[key] )

            cursor = dbconn.cursor()
            sys.stderr.write( "Starting query sequence\n" )
            for query, subdict in zip( queries, subdicts ):
                sys.stderr.write( f'Query is {query}, subdict is {subdict}, dbuser is {dbuser}\n' )
                cursor.execute( query, subdict )
                sys.stderr.write( 'Query done\n' )
            sys.stderr.write( "Fetching\n" )
            rows = cursor.fetchall()
            sys.stderr.write( f"Returning {len(rows)} rows from query sequence.\n" )
        except Exception as ex:
            return JsonResponse( { 'status': 'error', 'error': str(ex) } )
        finally:
            dbconn.rollback()
            dbconn.close()
        return JsonResponse( { 'status': 'ok', 'rows': rows } )
        
