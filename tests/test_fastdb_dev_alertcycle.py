# WARNING -- if you run both this test and test_elasticc2_alertcycle
#   within the same docker compose session, but different pytest
#   sessions, one will fail.  For the reason, see the comments in
#   alertcyclefixtures.py.  (Basically, the first one you run will load
#   up both databases, so early tests that expect not-fully-loaded
#   databases will fail.)
#
# Both should all pass if you run them both at once, i.e.
#
# pytest -v test_elasticc2_alertcycle.py test_fastdb_dev_alertcycle.py

import os
import sys
import datetime
import time

sys.path.insert( 0, "/tom_desc" )

import elasticc2.models

from testmsgconsumer import MsgConsumer

# pytest is mysterious.  I tried importing just the fixtures I was using
# form alertcyclefixtures, but the a fixture there that used another
# fixture from alertcyclefixtures that I did *not* import here couldn't
# find that other fixture.  So, I import *, and use an __all__ in
# alertcyclefixtures.
from alertcyclefixtures import *

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


    def handle_test_send_alerts( self, msgs ):
        self._test_send_alerts_count += len(msgs)


    def test_send_alerts( self, alerts_300days ):
        assert alerts_300days


    def test_classifications_exist( self, classifications_300days_exist ):
        assert classifications_300days_exist


    def test_classifications_ingested( self, classifications_300days_fastdb_dev_ingested ):
        assert classifications_300days_fastdb_dev_ingested


    def test_sources_updated( self, update_fastdb_dev_diasource_300days ):
        assert update_fastdb_dev_diasource_300days


    def test_100moredays_classifications_ingested( self, classifications_100daysmore_fastdb_dev_ingested ):
        assert classifications_100daysmore_fastdb_dev_ingested


    def test_100moredays_sources_updated( self, update_fastdb_dev_diasource_100daysmore ):
        assert update_fastdb_dev_diasource_100daysmore
