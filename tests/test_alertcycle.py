import os
import sys
import datetime

sys.path.insert( 0, "/tom_desc" )

import elasticc2.models as m

from msgconsumer import MsgConsumer

class TestAlertCycle:

    def test_ppdb_loaded( self, elasticc_ppdb ):
        # I should probably have some better tests than just object counts....
        assert m.PPDBDiaObject.objects.count() == 14579
        assert m.PPDBDiaSource.objects.count() == 365300
        assert m.PPDBAlert.objects.count() == m.PPDBDiaSource.objects.count()
        assert m.PPDBDiaForcedSource.objects.count() == 2497031
        assert m.DiaObjectTruth.objects.count() == m.PPDBDiaObject.objects.count()

    def handle_test_send_alerts( self, msgs ):
        self._test_send_alerts_count += len(msgs)
        
    def test_send_alerts( self, alerts_10days ):
        self._test_send_alerts_count = 0
        consumer = MsgConsumer( 'kafka-server:9092', 'test_send_alerts', 'alerts',
                                'schema/elasticc.v0_9_1.alert.avsc',
                                consume_nmsgs=100 )
        consumer.poll_loop( self.handle_test_send_alerts, timeout=10, stopafter=datetime.timedelta(seconds=10) )
        import pdb; pdb.set_trace()
        assert self._test_send_alerts_count == 990
        
