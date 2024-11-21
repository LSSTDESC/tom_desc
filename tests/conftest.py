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
# sys.path.insert( 0, os.getenv("PWD") )
# pytest_plugins = [ 'alertcyclefixtures' ]


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

    return models


@pytest.fixture
def elasticc2_database_snapshot():
    models = load_elasticc2_database_snapshot()
    yield True
    for m in models:
        m.objects.all().delete()

@pytest.fixture( scope='class' )
def elasticc2_database_snapshot_class():
    models = load_elasticc2_database_snapshot()
    yield True
    for m in models:
        m.objects.all().delete()



@pytest.fixture( scope="class" )
def random_broker_classifications():
    brokers = {
        'rbc_test1': {
            '1.0': {
                'classifiertest1': [ '1.0' ],
                'classifiertest2': [ '1.0' ]
            }
        },
        'rbc_test2': {
            '3.5': {
                'testing1': [ '42' ],
                'testing2': [ '23' ]
            }
        }
    }

    minsrc = 10
    maxsrc = 20
    mincls = 1
    maxcls = 20

    msgs = []
    for brokername, brokerspec in brokers.items():
        for brokerversion, versionspec in brokerspec.items():
            for classifiername, clsspec in versionspec.items():
                for classifierparams in clsspec:
                    nsrcs = random.randint( minsrc, maxsrc )
                    for src in range(nsrcs):
                        ncls = random.randint( mincls, maxcls )
                        probleft = 1.0
                        classes = []
                        probs = []
                        for cls in range( ncls ):
                            classes.append( cls )
                            prob = random.random() * probleft
                            probleft -= prob
                            probs.append( prob )
                        classes.append( ncls )
                        probs.append( probleft )

                        msgs.append( { 'sourceid': src,
                                       'brokername': brokername,
                                       'alertid': src,
                                       'elasticcpublishtimestamp': datetime.datetime.now( tz=pytz.utc ),
                                       'brokeringesttimestamp': datetime.datetime.now( tz=pytz.utc ),
                                       'brokerversion': brokerversion,
                                       'classifiername': classifiername,
                                       'classifierparams': classifierparams,
                                       'classid': classes,
                                       'probability': probs } )

    yield msgs

