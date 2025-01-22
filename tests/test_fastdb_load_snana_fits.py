import pytest
import os
import sys
import pathlib
import subprocess

# The next 4 lines are necessary to get access to
#  django models without doing python manage.py shell
sys.path.insert( 0, "/tom_desc" )
os.environ["DJANGO_SETTINGS_MODULE"] = "tom_desc.settings"
import django
django.setup()

import fastdb_dev.models as m

class TestLoadFastDBDevSnanaFits:

    @pytest.fixture( scope='class' )
    def snana_loaded( tomclient ):
        basedir = pathlib.Path( "/elasticc2data" )
        for subdir in basedir.glob( '*' ):
            if subdir.is_dir():
                result = subprocess.run( [ "python", "manage.py", "load_fastdb_dev_snana_fits",
                                           "-d", str(subdir), "--pv", "v1_1", "-s", "test",
                                           "-n", "5", "--do" ],
                                         cwd="/tom_desc", capture_output=True )
                assert result.returncode == 0

        yield True

        m.DFStoPVtoSS.objects.all().delete()
        m.DStoPVtoSS.objects.all().delete()
        m.DiaSource.objects.all().delete()
        m.DiaForcedSource.objects.all().delete()
        m.DiaObject.objects.all().delete()

    def test_loaded( self, snana_loaded ):
        # TODO: better tests than just object counts
        assert m.DiaObject.objects.count() == 346
        assert m.DiaSource.objects.count() == 1862
        assert m.DiaForcedSource.objects.count() == 52172
        assert m.DStoPVtoSS.objects.count() == m.DiaSource.objects.count()
        assert m.DFStoPVtoSS.objects.count() == m.DiaForcedSource.objects.count()
