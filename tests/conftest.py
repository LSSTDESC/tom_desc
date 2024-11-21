import sys
import os
import pathlib
import datetime
import time
import pytz
import random
import subprocess
import pytest

from pymongo import MongoClient

# Make sure the django environment is fully set up
sys.path.insert( 0, "/tom_desc" )
os.environ["DJANGO_SETTINGS_MODULE"] = "tom_desc.settings"
import django
django.setup()

import elasticc2.models
from tom_client import TomClient

# Additional fixtures in other files
sys.path.insert( 0, os.getenv("PWD") )
pytest_plugins = [ 'alertcyclefixtures' ]


@pytest.fixture( scope="session" )
def tomclient():
    return TomClient( "http://tom:8080", username="root", password="testing" )

@pytest.fixture( scope="session" )
def mongoclient():
    host = os.getenv( 'MONGOHOST' )
    username = os.getenv( 'MONGODB_ALERT_READER' )
    password = os.getenv( 'MONGODB_ALERT_READER_PASSWORD' )
    client = MongoClient( f"mongodb://{username}:{password}@{host}:27017/?authSource=alerts" )
    return client


@pytest.fixture( scope="session" )
def mongoadmin():
    host = os.getenv( 'MONGOHOST' )
    username = os.getenv( 'MONGODB_ADMIN' )
    password = os.getenv( 'MONGODB_ADMIN_PASSWORD' )
    client = MongoClient( f"mongodb://{username}:{password}@{host}:27017/" )
    return client


@pytest.fixture( scope="session" )
def apibroker_client():
    return TomClient( "http://tom:8080", username="apibroker", password="testing" )

@pytest.fixture( scope="session" )
def elasticc2_ppdb( tomclient ):
    basedir = pathlib.Path( "/elasticc2data" )
    dirs = []
    for subdir in basedir.glob( '*' ):
        if subdir.is_dir():
            result = subprocess.run( [ "python", "manage.py", "load_snana_fits", "-d", str(subdir), "--ppdb", "--do" ],
                                     cwd="/tom_desc", capture_output=True )
            assert result.returncode == 0

    yield True

    elasticc2.models.DiaObjectTruth.objects.all().delete()
    elasticc2.models.PPDBAlert.objects.all().delete()
    elasticc2.models.PPDBDiaForcedSource.objects.all().delete()
    elasticc2.models.PPDBDiaSource.objects.all().delete()
    elasticc2.models.PPDBDiaObject.objects.all().delete()


@pytest.fixture
def load_elasticc2_database_snapshot():
    # Make sure that the database is in a state where this won't be a disaster
    models = [ elasticc2.models.BrokerClassifier,
               elasticc2.models.BrokerMessage,
               elasticc2.models.DiaForcedSource,
               elasticc2.models.DiaObject,
               elasticc2.models.DiaObjectTruth,
               elasticc2.models.DiaSource,
               elasticc2.models.PPDBAlert,
               elasticc2.models.PPDBDiaForcedSource,
               elasticc2.models.PPDBDiaObject,
               elasticc2.models.PPDBDiaSource,
               elasticc2.models.DiaObjectInfo,
               elasticc2.models.BrokerSourceIds ]

    for m in models:
        assert m.objects.count() == 0

    # Load
    res = subprocess.run( [ "pg_restore",
                            "--data-only",
                            "-h", "postgres",
                            "-U", "postgres",
                            "-d", "tom_desc",
                            "elasticc2_alertcycle_complete.psqlc" ],
                          cwd="/tests",
                          env={ 'PGPASSWORD': 'fragile' },
                          capture_output=True )
    assert res.returncode == 0

    yield True

    # Unload
    for m in models:
        m.objects.all().delete()
