import os
import sys
import datetime
import time

sys.path.insert( 0, "/tom_desc" )

import elasticc2.models

# NOTE -- many of the actual tests are run in the fixtures rather than
#   the tests below.  See comments in alercyclefixtures.py for the reason for
#   this.

class TestFastDBDevAlertCycle:
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


    def test_classifications_ingested( self, classifications_300days_ingested ):
        assert classifications_300days_ingested


    def test_sources_updated( self, update_fastdb_dev_diasource_300days ):
        assert update_fastdb_dev_diasource_300days


    def test_100moredays_classifications_ingested( self, classifications_100daysmore_ingested ):
        assert classifications_100daysmore_ingested


    def test_100moredays_sources_updated( self, update_fastdb_dev_diasource_100daysmore ):
        assert update_fastdb_dev_diasource_100daysmore


    def test_cleanup( self, alert_cycle_complete ):
        # This is just here to make sure that the cleanup in the
        #   alert_cycle_complete session fixture gets run.
        pass
