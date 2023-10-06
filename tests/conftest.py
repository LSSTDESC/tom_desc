import sys
import os
import pathlib
import datetime
import pytz
import random
import subprocess
import pytest

sys.path.insert( 0, "/tom_desc" )
os.environ["DJANGO_SETTINGS_MODULE"] = "tom_desc.settings"
import django
django.setup()

import elasticc2.models
from tom_client import TomClient

@pytest.fixture( scope="session" )
def tomclient():
    return TomClient( "http://tom:8080", username="root", password="testing" )

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
            import pdb; pdb.set_trace()
            assert result.returncode == 0

    yield True

    elasticc2.models.DiaObjectTruth.objects.all().delete()
    elasticc2.models.PPDBAlert.objects.all().delete()
    elasticc2.models.PPDBDiaForcedSource.objects.all().delete()
    elasticc2.models.PPDBDiaSource.objects.all().delete()
    elasticc2.models.PPDBDiaObject.objects.all().delete()

# DOESN'T WORK -- fakebroker is listening to specific topics
# # This is a hack so that each time I run the tests,
# #   it will write to a different kafka topic.
# # Ideally, fixtures clean up after themselves, but
# #   that would mean running something *on* the kafka
# #   server to delete the topic, and that's hard.
# #   This will hopefully give the same effect of having
# #   empty topics.
# @pytest.fixture( scope="session" )
# def topictag():
#     f = pathlib.Path( "/tests/topictag" )
#     if not f.exists():
#         with open(f, "w") as ofp:
#             ofp.write( "1" )
#         return "1"
#     else:
#         with open(f) as ifp:
#             topictag = int( ifp.readline() )
#         topictag = str( topictag + 1 )
#         with open(f, "w") as ofp:
#             ofp.write( topictag )
#         return topictag

@pytest.fixture( scope="session" )
def alerts_300days( elasticc2_ppdb ):
    result = subprocess.run( [ "python", "manage.py", "send_elasticc2_alerts", "-d", "60578",
                               "-k", "kafka-server:9092", "-t", f"alerts",
                               "-s", "/tests/schema/elasticc.v0_9_1.alert.avsc",
                               "-r", "sending_alerts_runningfile", "--do" ],
                               cwd="/tom_desc", capture_output=True )
    assert result.returncode == 0

    yield True

    # I'm not going to clean up here.  Ideally, I should delete all of
    # the alerts on the kafka server, but that's hard, and would require
    # me to run something *on* the kafka server, so I'm just not going
    # to do that, and that's a mess.

    # This does mean that if you run tests more than once in the same
    # run of docker compose up, subsequent tests may fail because the
    # number of things in the topic will be too high, as alerts will be
    # issued again on subsequent tests.

@pytest.fixture( scope="session" )
def update_diasource_300days( alerts_300days ):
    result = subprocess.run( [ "python", "manage.py", "update_elasticc2_sources" ],
                             cwd="/tom_desc", capture_output=True )
    assert result.returncode == 0

    yield True

    # Cleaning up... not going to bother.  Yeah, yeah, I'm bad.  But,
    # cleaning up would mean figuring out exactly what sources were
    # added by that manage command, and that's more complicated than
    # it's worth, given that I know this is going to be used only in
    # test_alertcycle.  Also, I didn't clean up the last fixture,
    # so... it's a mess!  Embrace it!


@pytest.fixture( scope="session" )
def alerts_100daysmore( alerts_300days ):
    result = subprocess.run( [ "python", "manage.py", "send_elasticc2_alerts", "-a", "100",
                               "-k", "kafka-server:9092", "-t", f"alerts",
                               "-s", "/tests/schema/elasticc.v0_9_1.alert.avsc",
                               "-r", "sending_alerts_runningfile", "--do" ],
                               cwd="/tom_desc", capture_output=True )
    assert result.returncode == 0

    yield True

    # Same issue as alerts_300days about not cleaning up

@pytest.fixture( scope="session" )
def update_diasource_100daysmore( alerts_100daysmore ):
    result = subprocess.run( [ "python", "manage.py", "update_elasticc2_sources" ],
                             cwd="/tom_desc", capture_output=True )
    assert result.returncode == 0

    yield True

    # Same issue...

@pytest.fixture( scope="session" )
def api_classify_existing_alerts( alerts_100daysmore, apibroker_client ):
    result = subprocess.run( [ "python", "apiclassifier.py", "--source", "kafka-server:9092", "-t", "alerts",
                               "-g", "apibroker", "-u", "apibroker", "-p", "testing", "-s", "2",
                               "-a", "/tests/schema/elasticc.v0_9_1.alert.avsc",
                               "-b", "/tests/schema/elasticc.v0_9_1.brokerClassification.avsc"],
                             cwd="/tests", capture_output=True )
    assert result.returncode == 0

    yield True

@pytest.fixture( scope="module" )
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


