# The tests in this file depend on a completely fresh environment, just
# started with docker compose.  Because they are testing the result of
# other services started in the docker compose file, they don't restore
# the state of everything.  As such, they will fail if run a second time
# without completely restarting the docker compose environment.  (There
# will already be pre-existing alerts on the kafka server, there will
# already be existing database records.)

import os
import sys
import datetime
import time

sys.path.insert( 0, "/tom_desc" )

import elasticc2.models
import tom_targets.models

from msgconsumer import MsgConsumer

# The numbers in these tests are based on the SNANA files in the
# directory /data/raknop/elasticc2_train_1pct_3models on my desktop.
# that needs to be in the ELASTICC2_TEST_DATA env var when running
# docker compose.
#
# HARDCORE TODO: get the test data set integrated into the archive
# somehow!  Or, at the very least, somewhere it can be downloaded.

class TestAlertCycle:

    def test_hello_world( self ):
        # This is just here so I can get a timestamp to see how long the next test took
        assert True
    
    def test_ppdb_loaded( self, elasticc2_ppdb ):
        # I should probably have some better tests than just object counts....
        assert elasticc2.models.PPDBDiaObject.objects.count() == 346
        assert elasticc2.models.PPDBDiaSource.objects.count() == 1862
        assert elasticc2.models.PPDBAlert.objects.count() == elasticc2.models.PPDBDiaSource.objects.count()
        assert elasticc2.models.PPDBDiaForcedSource.objects.count() == 52172
        assert elasticc2.models.DiaObjectTruth.objects.count() == elasticc2.models.PPDBDiaObject.objects.count()

        
    def handle_test_send_alerts( self, msgs ):
        self._test_send_alerts_count += len(msgs)
        
    def test_send_alerts( self, alerts_300days ):
        self._test_send_alerts_count = 0
        consumer = MsgConsumer( 'kafka-server:9092', 'test_send_alerts', 'alerts',
                                '/tests/schema/elasticc.v0_9_1.alert.avsc',
                                consume_nmsgs=100 )
        consumer.poll_loop( self.handle_test_send_alerts, timeout=10, stopafter=datetime.timedelta(seconds=10) )
        assert self._test_send_alerts_count == 545
        consumer.close()

        
    def handle_test_classifications_exist( self, msgs ):
        self._test_classifications_exist_count += len(msgs)
        
    def test_classifications_exist( self, alerts_300days ):
        # We want to make sure that the fake broker has classified all the alerts
        # The fake broker has a 10s sleep loop for waiting for topic to
        # exist.  Once it does, it should be pretty fast for it to
        # classify everything.  But, to be safe, give it 20s
        time.sleep( 20 )

        self._test_classifications_exist_count = 0
        consumer = MsgConsumer( 'kafka-server:9092', 'test_classifications_exist', 'classifications',
                                '/tests/schema/elasticc.v0_9_1.brokerClassification.avsc',
                                consume_nmsgs=100 )
        consumer.poll_loop( self.handle_test_classifications_exist, timeout=10,
                            stopafter=datetime.timedelta(seconds=10) )

        # Number of classifications should be:
        # 545 alerts * ( 2 classifiers ) = 1090
        assert self._test_classifications_exist_count == 1090
        consumer.close()

    def test_classifications_ingested( self, alerts_300days ):
        # This test effectively includes the previous one, but it's going
        # to look at a different thing.  Make sure that the broker classifications
        # ended up in the database.
        # Sleep another 10 seconds; the broker poller itself has a 10s loop for
        # looking for topics on the kafka server, so potentially that could be a full
        # 10s later than when the fakebrokers 10s timeout finished.  (Give it
        # another second to actually run, and/or to deal with roundoff error.)
        time.sleep( 11 )

        brkmsg = elasticc2.models.CassBrokerMessage
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

    def test_sources_updated( self, update_diasource_300days ):
        obj = elasticc2.models.DiaObject
        src = elasticc2.models.DiaSource
        frced = elasticc2.models.DiaForcedSource
        targ = tom_targets.models.Target
        ooft = elasticc2.models.DiaObjectOfTarget
        bsid = elasticc2.models.BrokerSourceIds

        assert bsid.objects.count() == 0
        assert obj.objects.count() == 102
        assert ooft.objects.count() == obj.objects.count()
        assert targ.objects.count() == obj.objects.count()
        assert src.objects.count() == 545
        assert frced.objects.count() == 4242
        
    def test_100moredays_classifications_ingested( self, alerts_100daysmore ):
        time.sleep( 21 )

        brkmsg = elasticc2.models.CassBrokerMessage
        cfer = elasticc2.models.BrokerClassifier

        # THIS DOES NOT WORK
        # This is something that's broken about the cassandra django
        # engine.  It doesn't seem to go back to the database to
        # recount, but it's remembering what it has cached.  It may
        # really be a mismatch between how you're supposed to use
        # Cassandra and the built-in relational-database assumptions of
        # django.
        # assert brkmsg.objects.count() == 1300

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
        

    def test_100moredays_sources_updated( self, update_diasource_100daysmore ):
        obj = elasticc2.models.DiaObject
        src = elasticc2.models.DiaSource
        frced = elasticc2.models.DiaForcedSource
        targ = tom_targets.models.Target
        ooft = elasticc2.models.DiaObjectOfTarget
        bsid = elasticc2.models.BrokerSourceIds

        assert bsid.objects.count() == 0
        assert obj.objects.count() == 131
        assert ooft.objects.count() == obj.objects.count()
        assert targ.objects.count() == obj.objects.count()
        assert src.objects.count() == 650
        assert frced.objects.count() == 5765

    def test_apibroker_existingsources( self, api_classify_existing_alerts ):
        cfer = elasticc2.models.BrokerClassifier
        brkmsg = elasticc2.models.CassBrokerMessage
        
        assert cfer.objects.count() == 3
        apibroker = cfer.objects.filter( brokername="apiclassifier" )[0]
        assert apibroker.brokerversion == "1.0"
        assert apibroker.classifiername == "AlwaysTheSame"
        assert apibroker.classifierparams == "0.5 111, 0.75 112"

        numprobs = 0
        for msg in brkmsg.objects.all():
            assert len(msg.classid) == len(msg.probability)
            numprobs += len(msg.classid)
        # 13650 from before, plus 2*650 for the new classifier
        assert numprobs == 14950

        apiclassmsgs = brkmsg.objects.filter( classifier_id=apibroker.classifier_id )

        onemsg = apiclassmsgs[0]
        assert onemsg.classid == [ 111, 112 ]
        assert onemsg.probability == [ 0.25, 0.75 ]
        assert onemsg.msghdrtimestamp >= onemsg.brokeringesttimestamp
        assert onemsg.msghdrtimestamp - onemsg.brokeringesttimestamp < datetime.timedelta(seconds=5)
        assert onemsg.descingesttimestamp - onemsg.msghdrtimestamp < datetime.timedelta(seconds=5)
