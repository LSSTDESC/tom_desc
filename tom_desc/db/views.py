import sys
import os
import json
import uuid
import datetime
import logging
import pathlib
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

_logger = logging.getLogger("db_views")
_logout = logging.FileHandler( pathlib.Path( os.getenv('LOGDIR', "/logs" )) / "db_views.log" )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
# _logger.setLevel( logging.DEBUG )
_logger.setLevel( logging.INFO )


def _extract_queries( request ):
    data = json.loads( request.body )

    if not 'query' in data:
        raise ValueError( "POST data must include 'query'" )

    if isinstance( data['query'], list ):
        queries = data['query']
    elif not isinstance( data['query'], str ):
        raise TypeError( "query must be either a list of strings or a string" )
    else:
        queries = [ data['query'] ]
    if not all( [ isinstance(q, str) for q in queries  ] ):
        raise TypeError( "queries must all be strings" )

    if not 'subdict' in data:
        subdicts = [ {} for i in range(len(queries)) ]
    else:
        if isinstance( data['subdict'], list ):
            subdicts = data['subdict']
            print(subdicts)
        elif not isinstance( data['subdict'], dict ):
            raise TypeError( "subdict must be either a list of dicts or a dict" )
        else:
            subdicts = [ data['subdict'] ]
        #if len(subdicts) != len(queries):
        #    raise ValueError( "number of queries and subdicts must match" )
        if not all( [ isinstance(s, dict) for s in subdicts ] ):
            raise TypeError( "subdicts must all be dicts" )

    # Have to convert lists to tuples in the substitution dictionaries
    for subdict in subdicts:
        for key in subdict.keys():
            if isinstance( subdict[key], list ):
                subdict[key] = tuple( subdict[key] )

    # Look for a return format
    return_format = 0
    if 'format' in data:
        return_format = data['format']

    return queries, subdicts, data, return_format


class RunSQLQuery(LoginRequiredMixin, django.views.View):
    raise_exception = True

    def post( self, request, *args, **kwargs ):
        dbconn = None
        try:
            dbuser = "postgres_ro"
            pwfile = "/secrets/postgres_ro_password"
            with open( pwfile ) as ifp:
                password = ifp.readline().strip()

            queries, subdicts, data, return_format = _extract_queries( request )

            dbconn = psycopg2.connect( dbname=os.getenv('DB_NAME'), host=os.getenv('DB_HOST'),
                                       user=dbuser, password=password,
                                       cursor_factory=psycopg2.extras.RealDictCursor )
            cursor = dbconn.cursor()

            _logger.debug( "Starting query sequence" )
            _logger.debug( f"queries={queries}" )
            _logger.debug( f"subdicts={subdicts}" )
            for query, subdict in zip( queries, subdicts ):
                _logger.debug( f'Query is {query}, subdict is {subdict}, dbuser is {dbuser}' )
                cursor.execute( query, subdict )
                _logger.debug( 'Query done' )
            _logger.debug( "Fetching" )
            rows = cursor.fetchall()

            _logger.debug( f"Returning {len(rows)} rows from query sequence." )
            return JsonResponse( { 'status': 'ok', 'rows': rows } )

        except Exception as ex:
            _logger.exception( ex )
            return JsonResponse( { 'status': 'error', 'error': str(ex) } )

        finally:
            if dbconn is not None:
                dbconn.rollback()
                dbconn.close()



# ======================================================================

class SubmitLongSQLQuery(LoginRequiredMixin, django.views.View):
    raise_exception = True

    def post( self, request, *args, **kwargs ):

        try:
            queries, subdicts, data, return_format = _extract_queries( request )

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
            _logger.exception( ex )
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
                                   'finished': queueobj.finished.isoformat(),
                                   'error': queueobj.errortext } )
                if queueobj.started is not None:
                    response['started'] = queueobj.started.isoformat()

            elif queueobj.finished is not None:
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
            _logger.exception( ex )
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
                    return HttpResponse( ifp.read(), content_type='application/octet-stream' )
            elif ( queueobj.format == "csv" ):
                with open( f"/query_results/{str(queueobj.queryid)}", "r" ) as ifp:
                    return HttpResponse( ifp.read(), content_type='text/csv; charset=utf-8' )
            else:
                return HttpResponse( f"Query is finished, but results are in an unknown format "
                                     f"{queueobj.format}", content_type="text/plain; charset=utf-8",
                                     status=500 )
        except Exception as ex:
            _logger.exception( ex )
            return HttpResponse( str(ex), content_type="text/plain; charset=utf-8", status=500 )
