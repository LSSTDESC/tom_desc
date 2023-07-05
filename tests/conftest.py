import sys
import os
import pathlib
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
def alerts_10days( elasticc2_ppdb ):
    result = subprocess.run( [ "python", "manage.py", "send_elasticc2_alerts", "-d", "60280",
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
def update_diasource_10days( alerts_10days ):
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
def alerts_1daymore( alerts_10days ):
    # https://www.youtube.com/watch?v=wNNBrg4u9d0
    result = subprocess.run( [ "python", "manage.py", "send_elasticc2_alerts", "-a", "1",
                               "-k", "kafka-server:9092", "-t", f"alerts",
                               "-s", "/tests/schema/elasticc.v0_9_1.alert.avsc",
                               "-r", "sending_alerts_runningfile", "--do" ],
                               cwd="/tom_desc", capture_output=True )
    assert result.returncode == 0

    yield True

    # Same issue as alerts_10days about not cleaning up

@pytest.fixture( scope="session" )
def update_diasource_1daymore( alerts_1daymore ):
    result = subprocess.run( [ "python", "manage.py", "update_elasticc2_sources" ],
                             cwd="/tom_desc", capture_output=True )
    assert result.returncode == 0

    yield True

    # Same issue...
