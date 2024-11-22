import os
import sys
import datetime
import time

from pymongo import MongoClient

sys.path.insert( 0, "/tom_desc" )

import fastdb_dev.models
import elasticc2.models

from alertcycle_testbase import AlertCycleTestBase

# NOTE -- many of the actual tests are run in the fixtures rather than
#   the tests below.  See comments in alertcycle_testbase.py for the reason for
#   this.

class TestFastDBDevAlertCycle( AlertCycleTestBase ):
    _models_to_cleanup = [ fastdb_dev.models.BrokerClassification,
                           fastdb_dev.models.BrokerClassifier,
                           fastdb_dev.models.DiaForcedSource,
                           fastdb_dev.models.DiaSource,
                           fastdb_dev.models.DiaObject,
                           fastdb_dev.models.DStoPVtoSS,
                           fastdb_dev.models.DFStoPVtoSS,
                           fastdb_dev.models.Snapshots,
                           fastdb_dev.models.ProcessingVersions ]

    def _cleanup( self ):
        host = os.getenv( 'MONGOHOST' )
        username = os.getenv( 'MONGODB_ADMIN' )
        password = os.getenv( 'MONGODB_ADMIN_PASSWORD' )
        client = MongoClient( f"mongodb://{username}:{password}@{host}:27017/" )
        db = client.alerts
        if 'fakebroker' in db.list_collection_names():
            coll = db.fakebroker
            coll.drop()
        assert 'fakebroker' not in db.list_collection_names()


    def test_ppdb_loaded( self, elasticc2_ppdb_class ):
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


    def test_classifications_ingested( self, classifications_300days_fastdb_dev_ingested ):
        assert classifications_300days_fastdb_dev_ingested


    def test_sources_updated( self, update_fastdb_dev_diasource_300days ):
        assert update_fastdb_dev_diasource_300days


    def test_100moredays_classifications_ingested( self, classifications_100daysmore_fastdb_dev_ingested ):
        assert classifications_100daysmore_fastdb_dev_ingested


    def test_100moredays_sources_updated( self, update_fastdb_dev_diasource_100daysmore ):
        assert update_fastdb_dev_diasource_100daysmore
