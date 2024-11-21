import os
import sys
import datetime
import time

sys.path.insert( 0, "/tom_desc" )

import elasticc2.models
import tom_targets.models

from alertcycle_testbase import AlertCycleTestBase

# NOTE -- many of the actual tests are run in the fixtures rather than
#   the tests below.  See comments in alertcycle_testbase.py for the reason for
#   this.

class TestElasticc2AlertCycle( AlertCycleTestBase ):
    _models_to_cleanup = [ elasticc2.models.BrokerMessage,
                           elasticc2.models.BrokerClassifier,
                           elasticc2.models.BrokerSourceIds,
                           elasticc2.models.DiaObjectOfTarget,
                           tom_targets.models.Target,
                           elasticc2.models.DiaForcedSource,
                           elasticc2.models.DiaSource,
                           elasticc2.models.DiaObject ]

    def _cleanup( self ):
        pass

    def test_ppdb_loaded( self, elasticc2_ppdb ):
        # I should probably have some better tests than just object counts....
        assert elasticc2.models.PPDBDiaObject.objects.count() == 346
        assert elasticc2.models.PPDBDiaSource.objects.count() == 1862
        assert elasticc2.models.PPDBAlert.objects.count() == elasticc2.models.PPDBDiaSource.objects.count()
        assert elasticc2.models.PPDBDiaForcedSource.objects.count() == 52172
        assert elasticc2.models.DiaObjectTruth.objects.count() == elasticc2.models.PPDBDiaObject.objects.count()


    def test_send_alerts( self, alerts_300days ):
        assert alerts_300days


    def test_classifications_exist( self, classifications_300days_exist ):
        assert classifications_300days_exist


    def test_classifications_ingested( self, classifications_300days_elasticc2_ingested ):
        assert classifications_300days_elasticc2_ingested


    def test_sources_updated( self, update_elasticc2_diasource_300days ):
        assert update_elasticc2_diasource_300days


    def test_100moredays_classifications_ingested( self, classifications_100daysmore_elasticc2_ingested ):
        assert classifications_100daysmore_elasticc2_ingested


    def test_100moredays_sources_updated( self, update_elasticc2_diasource_100daysmore ):
        assert update_elasticc2_diasource_100daysmore


    def test_apibroker_existingsources( self, api_classify_existing_alerts ):
        cfer = elasticc2.models.BrokerClassifier
        brkmsg = elasticc2.models.BrokerMessage

        assert cfer.objects.count() == 3
        apibroker = cfer.objects.filter( brokername="apiclassifier" )[0]
        assert apibroker.brokerversion == "1.0"
        assert apibroker.classifiername == "AlwaysTheSame"
        assert apibroker.classifierparams == "0.5 111, 0.75 112"

        numprobs = 0
        # There are 650 alerts
        # The test api broker will add 1300 probabilities
        #   (since it assignes probabilities to two classes).
        # Add that to the 13650 probabilities that
        #   are in fixture classifications_100daysmore_elasticc2_ingested,
        #   and you get 14950
        for msg in brkmsg.objects.all():
            assert len(msg.classid) == len(msg.probability)
            numprobs += len(msg.classid)
        assert numprobs == 14950

        # apiclassmsgssrc = brkmsgsrc.objects.filter( classifier_id=apibroker.classifier_id )
        # apiclassmsgstim = brkmsgtim.objects.filter( classifier_id=apibroker.classifier_id )
        # assert len( apiclassmsgssrc ) == len( apiclassmsgstim )

        apiclassmsg = brkmsg.objects.filter( classifier_id=apibroker.classifier_id )
        # There are 650 alerts, and the api broker should have classified all of them
        assert len( apiclassmsg ) == 650

        onemsg = apiclassmsg[0]
        assert onemsg.classid == [ 111, 112 ]
        assert onemsg.probability == [ 0.25, 0.75 ]
        assert onemsg.msghdrtimestamp >= onemsg.brokeringesttimestamp
        assert onemsg.msghdrtimestamp - onemsg.brokeringesttimestamp < datetime.timedelta(seconds=5)
        assert onemsg.descingesttimestamp - onemsg.msghdrtimestamp < datetime.timedelta(seconds=5)
