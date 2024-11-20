import sys
import io
import psycopg2
import psycopg2.extras
import numpy
import pandas

import django.db
from django.db import models
import django.contrib.postgres.indexes as indexes
from django.utils.functional import cached_property
from django.contrib.postgres.fields import ArrayField

# Create your models here.

# Support the Q3c Indexing scheme

class q3c_ang2ipix(models.Func):
    function = "q3c_ang2ipix"

class Float32Field(models.Field):
    def db_type( self, connection ):
        return "real"

# A hack so that I can have index names that go up to
#   the 63 characters postgres allows, instead of the
#   30 that django allows
class LongNameBTreeIndex(indexes.BTreeIndex):
    @cached_property
    def max_name_length(self):
        return 63 - len(models.Index.suffix) + len(self.suffix)

# A parent class for my "create" and "load_or_create" methods
# Classes that derive from this must define:
#   _dict_kws -- properties of the object that should be stuffed into a dictionary in to_dict
#   _create_kws -- a list of keywords needed to create the object
#   _pk -- the primary key of the object
#   _create_kws_map [optional] -- a dict of { data_keyword : model_keyword }
#       if data keyword isn't in the keys, the map will be model_keyword = data_keyword.lower()
class Createable(models.Model):
    class Meta:
        abstract = True

    def to_dict( self ):
        selfdict = {}
        for key in self._dict_kws:
            if hasattr( self, '_irritating_django_id_map') and ( key in self._irritating_django_id_map ):
                self_key = self._irritating_django_id_map[key]
            else:
                self_key = key
            selfdict[key] = getattr( self, self_key )
        return selfdict

    @classmethod
    def data_to_createdict( cls, data, kwmap=None ):
        """Convert the dict data into the dict necessary to create an object of this class.

        data is a either a dictionary, or a list of dictionaries; in the
        latter case, the keys of all dictionaries in the list MUST
        match.  Its keys, converted to lowercase, will be compared to
        the _create_kws property of the class.  The ones that match will
        be used for that property of the created object.

        kwmap is a dict of { data_keyword : model_keyword } to override
        this standard lowercase conversion.  If None, it will default to
        the _create_kws_map property of the object.

        Returns either a dict of { model parameter : value }, or a list
        of such dicts, based on whether a dict or a list is passed in
        data.

        """
        if kwmap is None:
            kwmap = cls._create_kws_map if hasattr( cls, '_create_kws_map' ) else {}

        # I could probably do this with a dict comprehension, but this will be more... comprehensible
        datamap = {}
        protodata = data[0] if isinstance( data, list ) else data
        for kw in protodata.keys():
            if kw in kwmap:
                datamap[ kwmap[kw] ] = kw
            else:
                datamap[ kw.lower() ] = kw

        if isinstance( data, list ):
            kwargs = []
            for datum in data:
                thesekwargs = {}
                for kw in cls._create_kws:
                    thesekwargs[kw] = datum[ datamap[kw] ] if kw in datamap else None
                kwargs.append( thesekwargs )
        else:
            kwargs = {}
            for kw in cls._create_kws:
                kwargs[kw] = data[ datamap[kw] ] if kw in datamap else None
                # Do some preemptive type conversions
                # to avoid django/postgres screaming
                if isinstance(kwargs[kw], numpy.int64 ):
                     kwargs[kw] = int( kwargs[kw] )

        return kwargs

    @classmethod
    def create( cls, data, kwmap=None ):
        """Create a new object based on data.  data must be a dict of { kw: value } for a single table row.

        See data_to_createdict for definition of parameters

        """

        kwargs = cls.data_to_createdict( data, kwmap=kwmap )
        curobj = cls( **kwargs )
        curobj.save()
        return curobj

    @classmethod
    def load_or_create( cls, data ):
        try:
            curobj = cls.objects.get( pk=data[cls._pk] )
            # VERIFY THAT STUFF MATCHES????
            return curobj
        except cls.DoesNotExist:
            return cls.create( data )

    @classmethod
    def which_exist( cls, pks ):
        """Pass a list of primary key, get a list of the ones that already exist."""
        q = cls.objects.filter( pk__in=pks )
        return [ getattr(i, i._pk) for i in q ]

    # This version uses postgres COPY and tries to be faster than mucking
    # about with ORM constructs.
    @classmethod
    def bulk_insert_or_upsert( cls, data, kwmap=None, upsert=False ):
        """Try to efficiently insert a bunch of data into the database.

        Parameters
        ----------
           data: dict or list
             Data can be:
              * a dict of { kw: iterable }.  All of the iterables must
                have the same length, and must be something that
                pandas.DataFrame could handle
              * a list of dicts.  The keys in all dicts must be the same

           kwmap: dict, default None
             A map of { dict_keyword : model_keyword } of conversions
             from data to the class model.  (See data_to_createdict().)
             Defaults to the _create_kws_map property of the object.

           upsert: bool, default False
             If False, then objects whose primary key is already in the
             database will be ignored.  If True, then objects whose
             primary key is already in the database will be updated with
             the values in dict.  (SQL will have ON CONFLICT DO NOTHING
             if False, ON CONFLICT DO UPDATE if True.)


        Returns
        -------
           inserted: int
             The number of rows actually inserted (which may be less than len(data)).

        """
        conn = None
        origautocommit = None
        gratuitous = None
        cursor = None
        try:
            # Jump through hoops to get access to the psycopg2
            #   connection from django.  We need this to
            #   turn off autocommit so we can use a temp table.
            gratuitous = django.db.connection.cursor()
            conn = gratuitous.connection
            origautocommit = conn.autocommit
            conn.autocommit = False
            cursor = conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor )
            # Yeah.... if anybody ever creates a django application named "bulk" and then
            #   has a model "upsert", we're about to totally screw that up.
            cursor.execute( "DROP TABLE IF EXISTS bulk_upsert" )
            cursor.execute( f"CREATE TEMP TABLE bulk_upsert (LIKE {cls._meta.db_table})" )
            # NOTE: I have a little bit of worry here that pandas is going to destroy
            # datatypes-- in particular, that it will convert my int64s to either int32 or
            # float our double, thereby losing precision.  I've checked it, and it seems
            # to be doing the right thing.  But I have had issues in the past with
            # pandas silently converting data to doubles.
            df = pandas.DataFrame( cls.data_to_createdict( data, kwmap=kwmap ) )
            strio = io.StringIO()
            df.to_csv( strio, index=False, header=False, sep='\t', na_rep='\\N' )
            strio.seek(0)
            # Have to quote the column names because many have mixed case.
            # (...didn't work, there were extra " in the command sent.
            # this will break if columns aren't all lower case)
            # columns = [ f'"{c}"' for c in df.columns.values ]
            columns = df.columns.values
            cursor.copy_from( strio, "bulk_upsert", columns=columns, size=1048576 )
            if not upsert:
                conflict = "DO NOTHING"
            else:
                conflict = "DO UPDATE SET " + ",".join( f"{c}=EXCLUDED.{c}" for c in columns )
            q = f"INSERT INTO {cls._meta.db_table} SELECT * FROM bulk_upsert ON CONFLICT {conflict}"
            cursor.execute( q )
            ninserted = cursor.rowcount
            # I don't think I should have to do this; shouldn't it happen automatically
            #   with conn.commit()?  But it didn't seem to.  Maybe it only happens
            #   with conn.close(), but I don't want to do that because it screws
            #   with django.  (I'm probably doing naughty things by even digging to
            #   get conn, but hey, I need it for efficiency.)
            cursor.execute( "DROP TABLE bulk_upsert" )
            conn.commit()
            return ninserted
        except Exception as e:
            if conn is not None:
                conn.rollback()
            raise e
        finally:
            if cursor is not None:
                cursor.close()
                cursor = None
            if gratuitous is not None:
                gratuitous.close()
                gratuitous = None
            if origautocommit is not None and conn is not None:
                conn.autocommit = origautocommit
                origautocommit = None
                conn = None

    # NOTE -- this version returns all the objects that were
    #   either loaded or created.  I've got other classes that
    #   have their own "bulk_load_or_create" that don't return
    #   the objects, and that use the database unique checks
    #   to avoid duplication.  I should think about merging
    #   them.
    # This one only detects existing objects based on the
    #   primary key, which is *not enough* for some of the
    #   other things I'm missing.
    @classmethod
    def bulk_load_or_create( cls, data, kwmap=None ):
        """Pass an array of dicts."""
        data = cls.data_to_createdict( data, kwmap=kwmap )
        pks = [ i[cls._pk] for i in data ]
        curobjs = list( cls.objects.filter( pk__in=pks ) )
        exists = set( [ getattr(i, i._pk) for i in curobjs ] )
        newobjs = set()
        for newdata in data:
            if newdata[cls._pk] in exists:
                continue
            kwargs = {}
            for kw in cls._create_kws:
                kwargs[kw] = newdata[kw] if kw in newdata else None
            newobjs.add( cls( **kwargs ) )
        if len(newobjs) > 0:
            addedobjs = cls.objects.bulk_create( newobjs )
            curobjs.extend( addedobjs )
        return curobjs


# A queue for tracking long SQL queries

class QueryQueue(models.Model):
    queryid = models.UUIDField( primary_key=True )
    submitted = models.DateTimeField()
    started = models.DateTimeField( null=True, default=None )
    finished = models.DateTimeField( null=True, default=None )
    error = models.BooleanField( default=False )
    errortext = models.TextField( null=True, default=None )
    queries = ArrayField( models.TextField(), default=list )
    subdicts = ArrayField( models.JSONField(), default=list )
    format = models.TextField( default='csv' )

