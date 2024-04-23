import sys
import os
import json
import uuid
import datetime
import psycopg2
import psycopg2.extras
from django.http import HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.shortcuts import render
from django.contrib.auth.mixins import PermissionRequiredMixin, LoginRequiredMixin
import django.views
from db.models import QueryQueue

# WARNING -- I've harcoded a couple of paths here, in anticipation
#  that they'll be bind-mounted there inside whatever container
#  is running the TOM.

# ======================================================================
# A low-level query interface.
#
# That is, of course, EXTREMELY scary.  This is why you need to make
# sure the postgres user postgres_ro is a readonly user.  Still scary,
# but not Cthulhuesque.

class RunSQLQuery(LoginRequiredMixin, django.views.View):
    raise_exception = True

    def post( self, request, *args, **kwargs ):
        dbuser = "postgres_ro"
        pwfile = "/secrets/postgres_ro_password"
        # dbuser = "postgres_elasticc_admin_ro"
        # pwfile = "/secrets/postgres_elasticc_admin_ro_password"
        # if self.request.user.has_perm( "elasticc.elasticc_admin" ):
        #     dbuser = "postgres_elasticc_admin_ro"
        #     pwfile = "/secrets/postgres_elasticc_admin_ro_password"
        # else:
        #     dbuser = "postgres_elasticc_ro"
        #     pwfile = "/secrets/postgres_ro_password"
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


# ======================================================================

class SubmitLongSQLQuery(LoginRequiredMixin, django.views.View):
    raise_exception = True

    def post( self, request, *args, **kwargs ):

        data = json.loads( request.body )

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
                    subdicts = [ {} ]

            else:
                raise ValueError( "Must pass either queries or query" )

            format = 'csv'
            if 'format' in data:
                format = data[ 'format' ]
                if format not in [ 'csv', 'pandas', 'numpy' ]:
                    raise ValueError( f"Unknown format {format}" )

            queryid = uuid.uuid4()
            newqueue = QueryQueue.objects.create( queryid=queryid, submitted=datetime.datetime.now(),
                                                  queries=queries, subdicts=subdicts, format=format )

            return JsonResponse( { 'status': 'ok', 'queryid': str(queryid) } )

        except Exception as ex:
            return JsonResponse( { 'status': 'error', 'error': str(ex) } )

# ======================================================================

class CheckLongSQLQuery(LoginRequiredMixin, django.views.View):
    raise_exception = True

    def post( self, request, queryid, *args, **kwargs ):
        try:
            queueobj = QueryQueue.objects.filter( queryid=queryid )
            if len( queueobj ) == 0:
                return JsonResponse( { 'status': 'error',
                                       'error': f"Unknown query {queryid}" } )
            queueobj = queueobj[0]

            response = { 'queryid': queueobj.queryid,
                         'queries': queueobj.queries,
                         'subdicts': queueobj.subdicts,
                         'submitted': queueobj.submitted.isoformat() }

            if queueobj.error:
                response.update( { 'status': 'error',
                                   'finished': queryobj.finished.isoformat(),
                                   'error': queryobj.errortext } )
                if queueobj.started is not None:
                    response['started'] = queueobj.started.isoformat()

            if queueobj.finished is not None:
                response.update( { 'status': 'finished',
                                   'started': queueobj.started.isoformat(),
                                   'finished': queueobj.finished.isoformat() } )

            elif queueobj.started is not None:
                response.update( { 'status': 'started',
                                   'started': queueobj.started.isoformat() } )

            else:
                response.update( { 'status': 'queued' } )

            return JsonResponse( response )

        except Exception as ex:
            return JsonResponse( { 'status': 'error', 'error': str(ex) } )

# ======================================================================

class GetLongSQLQueryResults(LoginRequiredMixin, django.views.View):
    raise_exception = True

    def post( self, request, queryid, *args, **kwargs ):
        try:
            queueobj = QueryQueue.objects.filter( queryid=queryid )
            if len( queueobj ) == 0:
                return HttpResponse( f"Unknown query {queueobj.queryid}",
                                     content_type="text/plain; charset=utf-8",
                                     status=500 )

            queueobj = queueobj[0]
            if queueobj.error:
                return HttpResponse( f"Query errored out: {queueobj.errortext}",
                                     content_type="text/plain; charset=utf-8",
                                     status=500 )

            if queueobj.finished is None:
                if queueobj.started is None:
                    return HttpResponse( f"Query {queryid} hasn't started yet",
                                         content_type="text/plain; charset=utf-8",
                                         status=500 )
                else:
                    return HttpResponse( f"Query {queryid} hasn't finished yet",
                                         content_type="text/plain; charset=utf-8",
                                         status=500 )


            if ( queueobj.format == "numpy" ) or ( queueobj.format == "pandas" ):
                with open( f"/query_results/{str(queueobj.queryid)}", "rb" ) as ifp:
                    return HttpResponse( ifp.read(), content_type='applciation/octet-stream' )
            elif ( queueobj.format == "csv" ):
                with open( f"/query_results/{str(queueobj.queryid)}", "r" ) as ifp:
                    return HttpResponse( ifp.read(), content_type='text/csv; charset=utf-8' )
            else:
                return HttpResponse( f"Query is finished, but results are in an unknown format "
                                     f"{queueobj.format}", content_type="text/plain; charset=utf-8",
                                     status=500 )
        except Exception as ex:
            return HttpResponse( str(ex), content_type="text/plain; charset=utf-8", status=500 )
