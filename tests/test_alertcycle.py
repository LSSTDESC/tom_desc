import os
import sys
import datetime
import time

sys.path.insert( 0, "/tom_desc" )

import elasticc2.models

from msgconsumer import MsgConsumer

# pytest is mysterious.  I tried importing just the fixtures I was using
# form alertcyclefixtures, but the a fixture there that used another
# fixture from alertcyclefixtures that I did *not* import here couldn't
# find that other fixture.  So, I import *, and use an __all__ in
# alertcyclefixtures.
from alertcyclefixtures import *

# NOTE -- many of the actual tests are run in the fixtures rather than
#   the tests below.  See comments in conftest.py for the reason for
#   this.

class TestAlertCycle:
    def test_hello_world( self ):
        # This is just here so I can get a timestamp to see how long the next test took
        assert True

    def test_ppdb_loaded( self, elasticc2_ppdb ):
        # I should probably have some better tests than just object counts....
        assert elasticc2.models.PPDBDiaObject.objects.count() == 138
        assert elasticc2.models.PPDBDiaSource.objects.count() == 429
        assert elasticc2.models.PPDBAlert.objects.count() == elasticc2.models.PPDBDiaSource.objects.count()
        assert elasticc2.models.PPDBDiaForcedSource.objects.count() == 34284
        assert elasticc2.models.DiaObjectTruth.objects.count() == elasticc2.models.PPDBDiaObject.objects.count()


    def handle_test_send_alerts( self, msgs ):
        self._test_send_alerts_count += len(msgs)


    def test_send_alerts( self, alerts_300days ):
        assert alerts_300days


    def test_classifications_exist( self, classifications_300days_exist ):
        assert classifications_300days_exist


    def test_classifications_ingested( self, classifications_300days_ingested ):
        assert classifications_300days_ingested


    def test_sources_updated( self, update_diasource_300days ):
        assert update_diasource_300days


    def test_100moredays_classifications_ingested( self, classifications_100daysmore_ingested ):
        assert classifications_100daysmore_ingested


    def test_100moredays_sources_updated( self, update_diasource_100daysmore ):
        assert update_diasource_100daysmore


    def test_apibroker_existingsources( self, api_classify_existing_alerts ):
        cfer = elasticc2.models.BrokerClassifier
        # brkmsgsrc = elasticc2.models.CassBrokerMessageBySource
        # brkmsgtim = elasticc2.models.CassBrokerMessageByTime
        brkmsg = elasticc2.models.BrokerMessage

        assert cfer.objects.count() == 3
        apibroker = cfer.objects.filter( brokername="apiclassifier" )[0]
        assert apibroker.brokerversion == "1.0"
        assert apibroker.classifiername == "AlwaysTheSame"
        assert apibroker.classifierparams == "0.5 111, 0.75 112"

        # numprobssrc = 0
        # numprobstim = 0
        numprobs = 0
        # for msg in brkmsgsrc.objects.all():
        #     assert len(msg.classid) == len(msg.probability)
        #     numprobssrc += len(msg.classid)
        # for msg in brkmsgtim.objects.all():
        #     assert len(msg.classid) == len(msg.probability)
        #     numprobstim += len(msg.classid)
        for msg in brkmsg.objects.all():
            assert len(msg.classid) == len(msg.probability)
            numprobs += len(msg.classid)
        # 5460 from before, plus 2*260 for the new classifier
        # assert numprobssrc == 5980
        # assert numprobssrc == numprobstim
        assert numprobs == 5980

        # apiclassmsgssrc = brkmsgsrc.objects.filter( classifier_id=apibroker.classifier_id )
        # apiclassmsgstim = brkmsgtim.objects.filter( classifier_id=apibroker.classifier_id )
        # assert len( apiclassmsgssrc ) == len( apiclassmsgstim )

        apiclassmsg = brkmsg.objects.filter( classifier_id=apibroker.classifier_id )
        assert len( apiclassmsg ) == 260

        onemsg = apiclassmsg[0]
        assert onemsg.classid == [ 111, 112 ]
        assert onemsg.probability == [ 0.25, 0.75 ]
        assert onemsg.msghdrtimestamp >= onemsg.brokeringesttimestamp
        assert onemsg.msghdrtimestamp - onemsg.brokeringesttimestamp < datetime.timedelta(seconds=5)
        assert onemsg.descingesttimestamp - onemsg.msghdrtimestamp < datetime.timedelta(seconds=5)

