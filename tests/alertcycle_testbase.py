# A base class for any class that tests the alert cycle.  Right now there are two, in
#  test_elasticc2_alertcycle.py and test_fastdb_dev_alertcycle.py.

# Inefficiency note: the fixutres do everything needed for both the
# elasticc2 and fastdb_dev tests, but because those tests are separate,
# it all gets run twice. ¯\_(ツ)_/¯

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
import django.db

import elasticc2.models
import fastdb_dev.models
import tom_targets.models

from tom_client import TomClient
sys.stderr.write( "ALERTCYCLEFIXTURES IMPORTING testmsgconsumer\n" )
from testmsgconsumer import MsgConsumer

sys.path.insert( 0, pathlib.Path(__file__).parent )
from fakebroker import FakeBroker

class _AlertCounter:
    def __init__( self ):
        self._test_alerts_exist_count = 0

    def handle_test_alerts_exist( self, msgs ):
        self._test_alerts_exist_count += len(msgs)


# The numbers in these tests are based on the SNANA files in the
# directory elasticc2_alert_test_data under tests, which should
# be unpacked from elasticc2_alert_test_data.tar.bz2.


class AlertCycleTestBase:

    # ....oh, pytest.  Too much magic.  I had this logger
    #  in an __init__ function, but it turns out you can't
    #  use __init__ functions in classes that are
    #  pytest classes.

    logger = logging.getLogger( "alertcyclefixtures" )
    logger.propagate = False
    _logout = logging.StreamHandler( sys.stderr )
    logger.addHandler( _logout )
    _formatter = logging.Formatter( f'[%(asctime)s - alertcyclefixtures - %(levelname)s] - %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S' )
    _logout.setFormatter( _formatter )
    logger.setLevel( logging.INFO )

    @pytest.fixture( scope="class", autouse=True )
    def alertcycletestbase_cleanup( self, elasticc2_ppdb_class ):
        # The loaded database will have some of the alerts tagged as
        #   having been sent (as the database dump was done for all of
        #   these fixtures having been run!), but we want them all to be
        #   null for these tests, so fix that.
        with django.db.connection.cursor() as cursor:
            cursor.execute( "UPDATE elasticc2_ppdbalert SET alertsenttimestamp=NULL" )

        yield True

        # Clean up the database.  This assumes that there aren't any
        # session fixtures that have left behind things in any of the
        # tables in self._models_to_cleanup....  Perhaps we should be
        # better about keeping track of what we've created.

        # (The PPDB and DiaObjectTruth tables are filled by a fixture
        # in confetest.py that *is* a session-scope fixture.)

        self.logger.info( "Cleaning up alertcycle database entries" )
        for model in self._models_to_cleanup:
            model.objects.all().delete()

        self._cleanup()



    # It's not really possible to clean up messages off of the kafka server.
    # So, to have a "fresh" environment, we randomly generate topics each time
    # we run this fixture, so those topics will begin empty.
    @pytest.fixture( scope="class" )
    def topic_barf( self ):
        return "".join( random.choices( "abcdefghijklmnopqrstuvwxyz", k=10 ) )


    @pytest.fixture( scope="class" )
    def fakebroker( self, topic_barf ):
        broker = FakeBroker( "kafka-server:9092",
                             [ f"alerts-wfd-{topic_barf}", f"alerts-ddf-full-{topic_barf}" ],
                             "kafka-server:9092",
                             f"classifications-{topic_barf}" )
        proc = multiprocessing.Process( target=broker, args=[], daemon=True )
        proc.start()

        yield True

        proc.terminate()
        proc.join()


    @pytest.fixture( scope="class" )
    def brokerpoll_elasticc2( self, topic_barf ):
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


    @pytest.fixture( scope="class" )
    def brokerpoll_fastdb_dev( self, topic_barf ):
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


    @pytest.fixture( scope="class" )
    def alerts_300days( self, topic_barf ):
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
                                logger=self.logger )
        counter = _AlertCounter()
        consumer.poll_loop( counter.handle_test_alerts_exist, timeout=10, stopafter=datetime.timedelta(seconds=10) )
        # I don't understand why this is 546.  545 were sent.
        # The fake broker sees 545.
        assert counter._test_alerts_exist_count == 546
        consumer.close()

        yield True


    @pytest.fixture( scope="class" )
    def classifications_300days_exist( self, alerts_300days, topic_barf, fakebroker ):
        counter = _AlertCounter()
        consumer = MsgConsumer( 'kafka-server:9092',
                                'test_classifications_exist',
                                f'classifications-{topic_barf}',
                                '/tests/schema/elasticc.v0_9_1.brokerClassification.avsc',
                                consume_nmsgs=100,
                                logger=self.logger )
        consumer.reset_to_start( f'classifications-{topic_barf}' )

        # fake broker has a 10s sleep loop, so we can't
        # assume things will be there instantly; thus, the 16s timeout.

        consumer.poll_loop( counter.handle_test_alerts_exist, timeout=5,
                            stopafter=datetime.timedelta(seconds=16) )

        # This is 2x545
        assert counter._test_alerts_exist_count == 1090
        consumer.close()

        yield True


    @pytest.fixture( scope="class" )
    def classifications_300days_elasticc2_ingested( self, classifications_300days_exist, mongoclient,
                                                    brokerpoll_elasticc2 ):
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

        assert ( set( [ i.classifiername for i in cfer.objects.all() ] )
                 == set( [ "NugentClassifier", "RandomSNType" ] ) )

        yield True


    @pytest.fixture( scope="class" )
    def classifications_300days_fastdb_dev_ingested( self, classifications_300days_exist, mongoclient,
                                                     brokerpoll_fastdb_dev ):
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


    @pytest.fixture( scope="class" )
    def update_elasticc2_diasource_300days( self, classifications_300days_elasticc2_ingested ):
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


    @pytest.fixture( scope="class" )
    def update_fastdb_dev_diasource_300days( self, classifications_300days_fastdb_dev_ingested ):
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

    # WORRY.  This will screw things up if this fixture is run
    #   before something the update_300days_*_ingested fixtures.
    #   But, we don't want to make those prerequisites for this one,
    #   because we don't want to have to run (in particular) the fastdb_dev
    #   ingestion fixtures when we don't need to, because it's slow.  So,
    #   Just figure that right now, the classes that are using these fixtures
    #   won't do things wrong, and leave the dependency out.
    @pytest.fixture( scope="class" )
    def alerts_100daysmore( self, alerts_300days, topic_barf, fakebroker ):
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


    @pytest.fixture( scope="class" )
    def classifications_100daysmore_elasticc2_ingested( self, alerts_100daysmore, mongoclient,
                                                        brokerpoll_elasticc2 ):
        # This time we need to allow for both the 10s sleep cycle timeout of
        # brokerpoll and fakebroker (since we're not checking
        # classifications exist separately from ingested)
        time.sleep( 22 )

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


    @pytest.fixture( scope="class" )
    def classifications_100daysmore_fastdb_dev_ingested( self, alerts_100daysmore, mongoclient,
                                                         brokerpoll_fastdb_dev ):
        # This time we need to allow for both the 10s sleep cycle timeout of
        # brokerpoll and fakebroker (since we're not checking
        # classifications exist separately from ingested)
        time.sleep( 22 )

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


    @pytest.fixture( scope="class" )
    def update_elasticc2_diasource_100daysmore( self, classifications_100daysmore_elasticc2_ingested ):
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


    @pytest.fixture( scope="class" )
    def update_fastdb_dev_diasource_100daysmore( self, classifications_100daysmore_fastdb_dev_ingested ):
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

    @pytest.fixture( scope="class" )
    def api_classify_existing_alerts( self, alerts_100daysmore, apibroker_client, topic_barf ):
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
