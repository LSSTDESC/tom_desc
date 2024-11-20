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

sys.path.insert( 0, "/tom_desc" )
os.environ["DJANGO_SETTINGS_MODULE"] = "tom_desc.settings"
import django
django.setup()

import elasticc2.models
from tom_client import TomClient

# Additional fixtures
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


