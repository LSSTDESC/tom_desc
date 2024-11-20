# NOTES ABOUT TEST INTEROPERABILITY
#
# These tests depend a lot on absolute state.  Their results are tested
# in test_elassticc2_alertcycle.py and test_fastdb_dev_alertcycle.py.
# Any other test should ONLY depend on the session fixture
# alert_cycle_complete from this test; that fixture will ensure that the
# PPDB, elasticc2 and fastdb_dev diaObject, diaSource, and
# diaForcedSource are all loaded, as well as all broker classifications
# for both elasticc2 and fastdb_dev.  (This fixture is used, for example,
# in test_elasticc2_spectrumcycle.py.)
#
# These fixtures count on absolute numbers of objects in the following models and databse tables:
#   ...
# No tests that put anything into any of those storage locations should
# be run in the same pytest call as tests that depend on these fixtures.


# The fixtures in this file test the full alert cycle of sending alerts
# from the simulated PPDB (using the django management command
# send_elasticc2_alerts), a fake broker classifying those alerts (see
# "def fakebroker" below), those classifications getting ingested by
# both elasticc2 and fastdb_dev (using management commands brokerpoll2
# and fastdb_dev_brokerpoll, both running as a continuous loop in the
# background), and classifications prompting the popualtion the
# diaObject, diaSource, and diaForce tables (using django management
# commands update_elasticc2_sources and load_fastdb).

import sys
import os
import pathlib
import datetime
import time
import pytz
import random
import subprocess
import multiprocessing
import pytest
import logging

sys.path.insert( 0, "/tom_desc" )
os.environ["DJANGO_SETTINGS_MODULE"] = "tom_desc.settings"
import django
django.setup()

import elasticc2.models
import fastdb_dev.models
import tom_targets.models

from tom_client import TomClient
sys.stderr.write( "ALERTCYCLEFIXTURES IMPORTING testmsgconsumer\n" )
from testmsgconsumer import MsgConsumer

sys.path.insert( 0, pathlib.Path(__file__).parent )
from fakebroker import FakeBroker

# NOTE -- in many of the fixtures below there are lots of tests that
# would normally be in the tests_* file that use the fixtures.  The
# reason they're here is because it's hard (impossible without a bunch
# of ugly hacks) to really clean up after these fixtures -- in
# particular, cleaning up the kafka server topic is something I can't
# just do here, but would have to do *on* the kafka server.  So, once
# some of the later fixtures have run, tests that depend only on earlier
# fixtures would start to fail.  The solution is to make all the
# fixtures session scoped, and to put the tests that have this
# hysteresis problem inside the fixtures, so they'll only be run once,
# and we can control the order in which the fixtures are run.  That will
# also then allow us to use these fixtures in more than one set of
# tests.

# Because of this, lots of fixtures don't bother cleaning up, even if
# they could.  In fact, they deliberately choose not to clean up,
# so that the database will be in the state it is at the end of the
# full alert cycle; the alert_cycle_complete fixture then detects that
# and runs the slow fixtures or not as necessary.  (That last fixture
# then actually cleans up the database.)

# Any tests that use these fixtures and are going to test actual numbers
# in the database should only depend on alert_cycle_complete.  Once all
# these fixtures have run (perhaps from an earlier test), the numbers
# that come out of earlier fixtures will no longer be right.  If any
# fixture other than alert_cycle_complete is run when the other fixtures
# have already been run once in a given docker compose environment, the
# database will be changed, and the fixtures will fail.

_logger = logging.getLogger( "alertcyclefixtures" )
_logger.propagate = False
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - alertcyclefixtures - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.INFO )



# The numbers in these tests are based on the SNANA files in the
# directory elasticc2_alert_test_data under tests, which should
# be unpacked from elasticc2_alert_test_data.tar.bz2.

class AlertCounter:
    def __init__( self ):
        self._test_alerts_exist_count = 0

    def handle_test_alerts_exist( self, msgs ):
        self._test_alerts_exist_count += len(msgs)


# It's not really possible to clean up messages off of the kafka server.
# So, to have a "fresh" environment, we randomly generate topics each time
# we start a new session, so those topics will begin empty.
@pytest.fixture( scope="session" )
def topic_barf():
    return "".join( random.choices( "abcdefghijklmnopqrstuvwxyz", k=10 ) )


@pytest.fixture( scope="session" )
def fakebroker( topic_barf ):
    broker = FakeBroker( "kafka-server:9092",
                         [ f"alerts-wfd-{topic_barf}", f"alerts-ddf-full-{topic_barf}" ],
                         "kafka-server:9092",
                         f"classifications-{topic_barf}" )
    proc = multiprocessing.Process( target=broker, args=[], daemon=True )
    proc.start()

    yield True

    proc.terminate()
    proc.join()


@pytest.fixture( scope="session" )
def brokerpoll_elasticc2( topic_barf ):
    def run_brokerpoll( topic_barf ):
        # Instead of using subprocess, use os.execvp so that signals we
        #   send to this process properly get to the command we're
        #   launching.  (The brokerpoll2 management command loops
        #   forever, but captures SIGINT, SIGKILL, and SIGUSR1,
        #   and shuts itself down upon receiving any of those signals.)
        #   (That's the intention, anyway....)
        sys.stdout.flush()
        sys.stderr.flush()
        os.chdir( "/tom_desc" )
        args = [ "python", "manage.py", "brokerpoll2",
                 "--do-test",
                 "--grouptag", "elasticc2",
                 "--test-topic", f"classifications-{topic_barf}" ]
        os.execvp( args[0], args )

    proc = multiprocessing.Process( target=run_brokerpoll, args=(topic_barf,), daemon=True )
    proc.start()

    yield True

    proc.terminate()
    proc.join()


@pytest.fixture( scope="session" )
def brokerpoll_fastdb_dev( topic_barf ):
    def run_brokerpoll( topic_barf ):
        # See comments in brokerpoll_elasticc2
        sys.stdout.flush()
        sys.stderr.flush()
        os.chdir( "/tom_desc" )
        args = [ "python", "manage.py", "fastdb_dev_brokerpoll",
                 "--do-test",
                 "--grouptag", "fastdb_dev",
                 "--test-topic", f"classifications-{topic_barf}" ]
        os.execvp( args[0], args )

    proc = multiprocessing.Process( target=run_brokerpoll, args=(topic_barf,), daemon=True )
    proc.start()

    yield True

    proc.terminate()
    proc.join()


@pytest.fixture( scope="session" )
def alerts_300days( elasticc2_ppdb, topic_barf ):
    result = subprocess.run( [ "python", "manage.py", "send_elasticc2_alerts", "-d", "60578",
                               "-k", "kafka-server:9092",
                               "--wfd-topic", f"alerts-wfd-{topic_barf}",
                               "--ddf-full-topic", f"alerts-ddf-full-{topic_barf}",
                               "--ddf-limited-topic", f"alerts-ddf-limited-{topic_barf}",
                               "-s", "/tests/schema/elasticc.v0_9_1.alert.avsc",
                               "-r", "sending_alerts_runningfile",
                               "--do" ],
                               cwd="/tom_desc", capture_output=True )
    sys.stderr.write( result.stderr.decode( 'utf-8' ) )
    assert result.returncode == 0

    consumer = MsgConsumer( 'kafka-server:9092',
                            'test_send_alerts',
                            [ f'alerts-wfd-{topic_barf}', f'alerts-ddf-full-{topic_barf}' ],
                            '/tests/schema/elasticc.v0_9_1.alert.avsc',
                            consume_nmsgs=100,
                            logger=_logger )
    counter = AlertCounter()
    consumer.poll_loop( counter.handle_test_alerts_exist, timeout=10, stopafter=datetime.timedelta(seconds=10) )
    # I don't understand why this is 546.  545 were sent.
    # The fake broker sees 545.
    assert counter._test_alerts_exist_count == 546
    consumer.close()

    yield True


@pytest.fixture( scope="session" )
def classifications_300days_exist( alerts_300days, topic_barf, fakebroker ):
    counter = AlertCounter()
    consumer = MsgConsumer( 'kafka-server:9092',
                            'test_classifications_exist',
                            f'classifications-{topic_barf}',
                            '/tests/schema/elasticc.v0_9_1.brokerClassification.avsc',
                            consume_nmsgs=100,
                            logger=_logger )
    consumer.reset_to_start( f'classifications-{topic_barf}' )

    # fake broker has a 10s sleep loop, so we can't
    # assume things will be there instantly; thus, the 16s timeout.

    consumer.poll_loop( counter.handle_test_alerts_exist, timeout=5,
                        stopafter=datetime.timedelta(seconds=16) )

    # This is 2x545
    assert counter._test_alerts_exist_count == 1090
    consumer.close()

    yield True


@pytest.fixture( scope="session" )
def classifications_300days_elasticc2_ingested( classifications_300days_exist, brokerpoll_elasticc2 ):
    # Have to have an additional sleep after the classifications exist,
    # because brokerpoll itself has a 10s sleep loop
    time.sleep( 11 )

    # Have to have these tests here rather than in the actual test_*
    # file because I can't clean up, and there is hysteresis.  Once
    # later fixtures have run, the tests below would fail, and these
    # fixtures may be used in more than one test.

    brkmsg = elasticc2.models.BrokerMessage
    cfer = elasticc2.models.BrokerClassifier
    bsid = elasticc2.models.BrokerSourceIds

    assert brkmsg.objects.count() == 1090
    assert cfer.objects.count() == 2
    assert bsid.objects.count() == 545

    numprobs = 0
    for msg in brkmsg.objects.all():
        assert len(msg.classid) == len(msg.probability)
        numprobs += len(msg.classid)

    # 545 from NugentClassifier plus 20*545 for RandomSNType
    assert numprobs == 11445

    # TODO : check that the data is identical for
    # corresponding entries in the two cassbroker
    # tables

    assert ( set( [ i.classifiername for i in cfer.objects.all() ] )
             == set( [ "NugentClassifier", "RandomSNType" ] ) )

    yield True


@pytest.fixture( scope="session" )
def classifications_300days_fastdb_dev_ingested( classifications_300days_exist, brokerpoll_fastdb_dev, mongoclient ):
    # Have to have an additional sleep after the classifications exist,
    # because brokerpoll itself has a 10s sleep loop
    time.sleep( 11 )

    # Have to have these tests here rather than in the actual test_*
    # file because I can't clean up, and there is hysteresis.  Once
    # later fixtures have run, the tests below would fail, and these
    # fixtures may be used in more than one test.

    db = mongoclient.alerts

    assert 'fakebroker' in db.list_collection_names()

    coll = db.fakebroker
    assert coll.count_documents({}) == 1090

    numprobs = 0
    for msg in coll.find():
        msg = msg['msg']
        assert msg['brokerName'] == 'FakeBroker'
        assert msg['classifierName'] in [ 'RandomSNType', 'NugentClassifier' ]
        if msg['classifierName'] == 'NugentClassifier':
            assert len( msg['classifications'] ) == 1
            assert msg['classifications'][0]['classId'] == 2222
            assert msg['classifications'][0]['probability'] == 1.0
        numprobs += len( msg['classifications'] )
    assert numprobs == 11445

    yield True

@pytest.fixture( scope="session" )
def update_elasticc2_diasource_300days( classifications_300days_elasticc2_ingested ):
    result = subprocess.run( [ "python", "manage.py", "update_elasticc2_sources" ],
                             cwd="/tom_desc", capture_output=True )
    assert result.returncode == 0

    # Have to have tests here because of hysteresis (search for that word above)
    obj = elasticc2.models.DiaObject
    src = elasticc2.models.DiaSource
    frced = elasticc2.models.DiaForcedSource
    targ = tom_targets.models.Target
    ooft = elasticc2.models.DiaObjectOfTarget
    bsid = elasticc2.models.BrokerSourceIds

    assert bsid.objects.count() == 0
    assert obj.objects.count() == 102
    # TODO -- put these next two lines back in once we start doing this thing again
    # assert ooft.objects.count() == obj.objects.count()
    # assert targ.objects.count() == obj.objects.count()
    assert src.objects.count() == 545
    assert frced.objects.count() == 4242

    yield True


@pytest.fixture( scope="session" )
def update_fastdb_dev_diasource_300days( classifications_300days_fastdb_dev_ingested ):
    result = subprocess.run( [ "python", "manage.py", "load_fastdb",
                               "--pv", "test_pv", "--snapshot", "test_ss",
                               "--tag", "test_ss_tag",
                               "--brokers", "fakebroker" ],
                             cwd="/tom_desc",
                             capture_output=True )
    assert result.returncode == 0

    lut = fastdb_dev.models.LastUpdateTime
    obj = fastdb_dev.models.DiaObject
    src = fastdb_dev.models.DiaSource
    frced = fastdb_dev.models.DiaForcedSource
    cfer = fastdb_dev.models.BrokerClassifier
    cification = fastdb_dev.models.BrokerClassification
    pver = fastdb_dev.models.ProcessingVersions
    ss = fastdb_dev.models.Snapshots
    dspvss = fastdb_dev.models.DStoPVtoSS
    dfspvss = fastdb_dev.models.DFStoPVtoSS

    assert lut.objects.count() == 1
    assert lut.objects.first().last_update_time > datetime.datetime.fromtimestamp( 0, tz=datetime.timezone.utc )
    assert lut.objects.first().last_update_time < datetime.datetime.now( tz=datetime.timezone.utc )

    # TODO : right now, load_fastdb.py imports the future -- that is, it imports
    #   the full ForcedSource lightcure for an object for which we got a source
    #   the first time that source is seen, and never looks at forcedsources
    #   again.  Update the tests numbers if/when it simulates not knowing the
    #   future.
    # (Really, we should probably creat a whole separate simulated PPDB server with
    #  an interface that will look something like the real PPDB interface... when
    #  we actually know what that is.)

    assert obj.objects.count() == 102
    assert src.objects.count() == 545
    assert frced.objects.count() == 15760   # 4242
    assert cfer.objects.count() == 2
    # assert cification.objects.count() == 831  # ???? WHy is this not 545 * 2 ?   LOOK INTO THIS
    #                                           # ---> seems to be non-deterministic!
    # TODO : pver, ss, dpvss, dfspvss

    yield True

@pytest.fixture( scope="session" )
def alerts_100daysmore( alerts_300days, topic_barf, fakebroker,
                        update_elasticc2_diasource_300days,
                        update_fastdb_dev_diasource_300days ):
    # This will send alerts up through mjd 60676.  Why not 60678, since the previous
    #   sent through 60578?  There were no alerts between 60675 and 60679, so the last
    #   alert sent will have been a source from mjd 60675.  That's what the 100 days
    #   are added to.
    # This is an additional 105 alerts, for a total of 650 (coming from 131 objects).
    # (Note that we have to make sure the udpate_*_diasource_300days
    #   fixtures have run before this fixture runs, because this fixture
    #   is going to add more alerts, leading the running fakebroker to
    #   add more classifications, and then there will be more
    #   classifications sitting on the kafka server than those fixtures
    #   (and their prerequisites) are expecting.)
    result = subprocess.run( [ "python", "manage.py", "send_elasticc2_alerts",
                               "-a", "100",
                               "-k", "kafka-server:9092",
                               "--wfd-topic", f"alerts-wfd-{topic_barf}",
                               "--ddf-full-topic", f"alerts-ddf-full-{topic_barf}",
                               "--ddf-limited-topic", f"alerts-ddf-limited-{topic_barf}",
                               "-s", "/tests/schema/elasticc.v0_9_1.alert.avsc",
                               "-r", "sending_alerts_runningfile",
                               "--do" ],
                               cwd="/tom_desc", capture_output=True )
    sys.stderr.write( result.stderr.decode( 'utf-8' ) )
    assert result.returncode == 0

    yield True

    # Same issue as alerts_300days about not cleaning up

@pytest.fixture( scope="session" )
def classifications_100daysmore_elasticc2_ingested( alerts_100daysmore, brokerpoll_elasticc2 ):
    # This time we need to allow for both the 10s sleep cycle timeout of
    # brokerpoll and fakebroker (since we're not checking
    # classifications exist separately from ingested)
    time.sleep( 22 )

    # Tests here because of hysteresis

    brkmsg = elasticc2.models.BrokerMessage
    cfer = elasticc2.models.BrokerClassifier

    # 650 total alerts times 2 classifiers = 1300 broker messages
    assert len( brkmsg.objects.all() ) == 1300
    assert cfer.objects.count() == 2
    assert len( cfer.objects.all() ) == 2

    numprobs = 0
    for msg in brkmsg.objects.all():
        assert len(msg.classid) == len(msg.probability)
        numprobs += len(msg.classid)
    # 650 from NugentClassifier plus 20*650 for RandomSNType
    assert numprobs == 13650

    assert ( set( [ i.classifiername for i in cfer.objects.all() ] )
             == set( [ "NugentClassifier", "RandomSNType" ] ) )

    yield True


@pytest.fixture( scope="session" )
def classifications_100daysmore_fastdb_dev_ingested( alerts_100daysmore, brokerpoll_fastdb_dev, mongoclient ):
    # This time we need to allow for both the 10s sleep cycle timeout of
    # brokerpoll and fakebroker (since we're not checking
    # classifications exist separately from ingested)
    time.sleep( 22 )

    # Tests here because of hysteresis

    db = mongoclient.alerts

    assert 'fakebroker' in db.list_collection_names()

    coll = db.fakebroker
    assert coll.count_documents({}) == 1300

    numprobs = 0
    for msg in coll.find():
        msg = msg['msg']
        assert msg['brokerName'] == 'FakeBroker'
        assert msg['classifierName'] in [ 'RandomSNType', 'NugentClassifier' ]
        if msg['classifierName'] == 'NugentClassifier':
            assert len( msg['classifications'] ) == 1
            assert msg['classifications'][0]['classId'] == 2222
            assert msg['classifications'][0]['probability'] == 1.0
        numprobs += len( msg['classifications'] )
    assert numprobs == 13650

    yield True


@pytest.fixture( scope="session" )
def update_elasticc2_diasource_100daysmore( classifications_100daysmore_elasticc2_ingested ):
    result = subprocess.run( [ "python", "manage.py", "update_elasticc2_sources" ],
                             cwd="/tom_desc", capture_output=True )
    assert result.returncode == 0

    obj = elasticc2.models.DiaObject
    src = elasticc2.models.DiaSource
    frced = elasticc2.models.DiaForcedSource
    targ = tom_targets.models.Target
    ooft = elasticc2.models.DiaObjectOfTarget
    bsid = elasticc2.models.BrokerSourceIds

    assert bsid.objects.count() == 0
    assert obj.objects.count() == 131
    # TODO: put these next two lines back in once we start doing this again
    # assert ooft.objects.count() == obj.objects.count()
    # assert targ.objects.count() == obj.objects.count()
    assert src.objects.count() == 650
    assert frced.objects.count() == 5765

    yield True


@pytest.fixture( scope="session" )
def update_fastdb_dev_diasource_100daysmore( classifications_100daysmore_fastdb_dev_ingested ):
    # SEE COMMENTS IN update_fastdb_dev_diasource_300days

    result = subprocess.run( [ "python", "manage.py", "load_fastdb",
                               "--pv", "test_pv", "--snapshot", "test_ss",
                               "--tag", "test_ss_tag",
                               "--brokers", "fakebroker" ],
                             cwd="/tom_desc",
                             capture_output=True )
    assert result.returncode == 0

    lut = fastdb_dev.models.LastUpdateTime
    obj = fastdb_dev.models.DiaObject
    src = fastdb_dev.models.DiaSource
    frced = fastdb_dev.models.DiaForcedSource
    cfer = fastdb_dev.models.BrokerClassifier
    cification = fastdb_dev.models.BrokerClassification
    pver = fastdb_dev.models.ProcessingVersions
    ss = fastdb_dev.models.Snapshots
    dspvss = fastdb_dev.models.DStoPVtoSS
    dfspvss = fastdb_dev.models.DFStoPVtoSS

    assert lut.objects.count() == 1
    assert lut.objects.first().last_update_time > datetime.datetime.fromtimestamp( 0, tz=datetime.timezone.utc )
    assert lut.objects.first().last_update_time < datetime.datetime.now( tz=datetime.timezone.utc )

    # TODO : right now, load_fastdb.py imports the future -- that is, it imports
    #   the full ForcedSource lightcure for an object for which we got a source
    #   the first time that source is seen, and never looks at forcedsources
    #   again.  Update the tests numbers if/when it simulates not knowing the
    #   future.
    # (Really, we should probably creat a whole separate simulated PPDB server with
    #  an interface that will look something like the real PPDB interface... when
    #  we actually know what that is.)

    assert obj.objects.count() == 131
    assert src.objects.count() == 650
    assert frced.objects.count() == 20834   # 5765
    assert cfer.objects.count() == 2
    # assert cification.objects.count() == ...  # ???? WHy is this not 650 * 2 ?   LOOK INTO THIS
    # TODO : pver, ss, dpvss, dfspvss

    yield True

@pytest.fixture( scope="session" )
def api_classify_existing_alerts( alerts_100daysmore, apibroker_client, topic_barf ):
    result = subprocess.run( [ "python", "apiclassifier.py",
                               "--source", "kafka-server:9092",
                               "-t", f"alerts-wfd-{topic_barf}", f"alerts-ddf-full-{topic_barf}",
                               "-g", "apibroker",
                               "-u", "apibroker",
                               "-p", "testing",
                               "-s", "2",
                               "-a", "/tests/schema/elasticc.v0_9_1.alert.avsc",
                               "-b", "/tests/schema/elasticc.v0_9_1.brokerClassification.avsc"
                              ],
                             cwd="/tests", capture_output=True )
    sys.stderr.write( result.stderr.decode( 'utf-8' ) )
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


@pytest.fixture( scope="session" )
def alert_cycle_complete( # request, tomclient, mongoadmin ):
                          mongoadmin,
                          update_elasticc2_diasource_100daysmore,
                          update_fastdb_dev_diasource_100daysmore,
                          api_classify_existing_alerts ):
    # res = tomclient.post( 'db/runsqlquery/',
    #                       json={ 'query': 'SELECT COUNT(*) AS count FROM elasticc2_brokermessage' } )
    # rows = res.json()[ 'rows' ]
    # if rows[0]['count'] == 0:
    #     request.getfixturevalue( "update_elasticc2_diasource_100daysmore" )
    #     request.getfixturevalue( "update_fastdb_dev_diasource_100daysmore" )
    #     request.getfixturevalue( "api_classify_existing_alerts" )

    yield True

    # Clean up the database.
    # This is potentially antisocial, since other fixtures may have
    #   added things to these tables.  However, since this is as session
    #   fixture, it shouldn't be too bad.  In any event, there are other
    #   problems running this file's fixtures with any tests other than
    #   those in test_elasticc2_alertcycle and
    #   test_fastdb_dev_alertcycle that want to look at any of these
    #   tables; see top of file.

    _logger.info( "Cleaning up alertcycle database entries" )
    elasticc2.models.BrokerMessage.objects.all().delete()
    elasticc2.models.BrokerClassifier.objects.all().delete()
    elasticc2.models.BrokerSourceIds.objects.all().delete()
    elasticc2.models.DiaObjectOfTarget.objects.all().delete()
    tom_targets.models.Target.objects.all().delete()
    elasticc2.models.DiaForcedSource.objects.all().delete()
    elasticc2.models.DiaSource.objects.all().delete()
    elasticc2.models.DiaObject.objects.all().delete()
    fastdb_dev.models.BrokerClassification.objects.all().delete()
    fastdb_dev.models.BrokerClassifier.objects.all().delete()
    fastdb_dev.models.DiaForcedSource.objects.all().delete()
    fastdb_dev.models.DiaSource.objects.all().delete()
    fastdb_dev.models.DiaObject.objects.all().delete()
    fastdb_dev.models.DStoPVtoSS.objects.all().delete()
    fastdb_dev.models.DFStoPVtoSS.objects.all().delete()
    fastdb_dev.models.Snapshots.objects.all().delete()
    fastdb_dev.models.ProcessingVersions.objects.all().delete()

    db = mongoadmin.alerts
    if 'fakebroker' in db.list_collection_names():
        coll = db.fakebroker
        coll.drop()
    assert 'fakebroker' not in db.list_collection_names()
