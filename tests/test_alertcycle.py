import os
import sys
import datetime
import time

sys.path.insert( 0, "/tom_desc" )

import elasticc2.models

from msgconsumer import MsgConsumer

class TestAlertCycle:

    def test_ppdb_loaded( self, elasticc2_ppdb ):
        # I should probably have some better tests than just object counts....
        assert elasticc2.models.PPDBDiaObject.objects.count() == 14579
        assert elasticc2.models.PPDBDiaSource.objects.count() == 365300
        assert elasticc2.models.PPDBAlert.objects.count() == elasticc2.models.PPDBDiaSource.objects.count()
        assert elasticc2.models.PPDBDiaForcedSource.objects.count() == 2497031
        assert elasticc2.models.DiaObjectTruth.objects.count() == elasticc2.models.PPDBDiaObject.objects.count()

        
    def handle_test_send_alerts( self, msgs ):
        self._test_send_alerts_count += len(msgs)
        
    def test_send_alerts( self, alerts_10days ):
        self._test_send_alerts_count = 0
        consumer = MsgConsumer( 'kafka-server:9092', 'test_send_alerts', 'alerts',
                                '/tests/schema/elasticc.v0_9_1.alert.avsc',
                                consume_nmsgs=100 )
        consumer.poll_loop( self.handle_test_send_alerts, timeout=10, stopafter=datetime.timedelta(seconds=10) )
        assert self._test_send_alerts_count == 990
        consumer.close()

        
    def handle_test_classifications_exist( self, msgs ):
        self._test_classifications_exist_count += len(msgs)
        
    def test_classifications_exist( self, alerts_10days ):
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
        assert self._test_classifications_exist_count == 1980
        consumer.close()

    def test_classifications_ingested( self, alerts_10days ):
        # This test effectively includes the previous one, but it's going
        # to look at a different thing.  Make sure that the broker classifications
        # ended up in the database.
        # Sleep another 10 seconds; the broker poller itself has a 10s loop for
        # looking for topics on the kafka server, so potentially that could be a full
        # 10s later than when the fakebrokers 10s timeout finished.
        time.sleep( 10 )
        
        msg  = elasticc2.models.BrokerMessage
        cfer = elasticc2.models.BrokerClassifier
        cify = elasticc2.models.BrokerClassification

        assert msg.objects.count() == 1980
        assert cfer.objects.count() == 2
        # 990 from NugentClassifier plus 7*990 for RandomSNType
        assert cify.objects.count() == 7920

        assert ( set( [ i.classifiername for i in cfer.objects.all() ] )
                 == set( [ "NugentClassifier", "RandomSNType" ] ) )
        
    def test_1moreday_classifications_ingested( self, alerts_1daymore ):
        time.sleep( 20 )

        msg  = elasticc2.models.BrokerMessage
        cfer = elasticc2.models.BrokerClassifier
        cify = elasticc2.models.BrokerClassification

        assert msg.objects.count() == 2392
        assert cfer.objects.count() == 2
        # 1196 from NugentClassifier plus 7*990 for RandomSNType
        assert cify.objects.count() == 9568

        assert ( set( [ i.classifiername for i in cfer.objects.all() ] )
                 == set( [ "NugentClassifier", "RandomSNType" ] ) )
        
