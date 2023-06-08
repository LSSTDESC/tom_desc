import os
import sys

# The next 4 lines are necessary to get access to
#  django models without doing python manage.py shell
sys.path.insert( 0, "/tom_desc" )
os.environ["DJANGO_SETTINGS_MODULE"] = "tom_desc.settings"
import django
django.setup()

import elasticc2.models as m

class TestPPDBLoaded:

    def test_ppdb_loaded( self, elasticc_ppdb ):
        # I should probably have some better tests than just object counts....
        assert m.PPDBDiaObject.objects.count() == 14579
        assert m.PPDBDiaSource.objects.count() == 365300
        assert m.PPDBAlert.objects.count() == m.PPDBDiaSource.objects.count()
        assert m.PPDBDiaForcedSource.objects.count() == 2497031
        assert m.DiaObjectTruth.objects.count() == m.PPDBDiaObject.objects.count()
        
