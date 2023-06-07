import os
import sys

sys.path.insert( 0, "/tom_desc" )
os.environ["DJANGO_SETTINGS_MODULE"] = "tom_desc.settings"

import django
django.setup()

import elasticc2.models as m

class TestLoadPPDB:

    def test_ppdb_loaded( self, elasticc_ppdb ):
        # I should probably have some better tests than just object counts....
        assert m.PPDBDiaObject.objects.count() == 14579
        assert m.PPDBDiaSource.objects.count() == 365300
        assert m.PPDBDiaForcedSoruce.objects.count() == 2479031
        assert m.DiaObjectTruth.objects.count() == m.PPDBDiaObject.objects.count()
        
