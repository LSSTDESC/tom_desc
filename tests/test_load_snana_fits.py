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

import elasticc2.models as m

class TestLoadSnanaFits:

    def test_ppdb_loaded( self, elasticc2_ppdb ):
        # I should probably have some better tests than just object counts....
        assert m.PPDBDiaObject.objects.count() == 138
        assert m.PPDBDiaSource.objects.count() == 429
        assert m.PPDBAlert.objects.count() == m.PPDBDiaSource.objects.count()
        assert m.PPDBDiaForcedSource.objects.count() == 34284
        assert m.DiaObjectTruth.objects.count() == m.PPDBDiaObject.objects.count()
        

    @pytest.fixture( scope="class" )
    def count_ppdb( self ):
        # IRRITATION.  When the fixture is called, it's not called with
        #  the same instance as the test will be called with.  So,
        #  we have to store class variables instead of instance variables.
        self.__class__._ppdbdiaobjects = m.PPDBDiaObject.objects.count()
        self.__class__._ppdbdiasources = m.PPDBDiaSource.objects.count()
        self.__class__._ppdbdiaforcedsources = m.PPDBDiaForcedSource.objects.count()
        self.__class__._ppdbalerts = m.PPDBAlert.objects.count()
        self.__class__._ppdbdiaobjecttruths = m.DiaObjectTruth.objects.count()
        
    @pytest.fixture( scope="class" )
    def elasticc2_training( self, count_ppdb ):
        basedir = pathlib.Path( "/elasticc2data" )
        dirs = []
        for subdir in basedir.glob( '*' ):
            if subdir.is_dir():
                result = subprocess.run( [ "python", "manage.py", "load_snana_fits",
                                           "-d", str(subdir), "--train", "--do" ],
                                         cwd="/tom_desc", capture_output=True )
                assert result.returncode == 0

        yield True

        m.TrainingDiaObjectTruth.objects.all().delete()
        m.TrainingAlert.objects.all().delete()
        m.TrainingDiaForcedSource.objects.all().delete()
        m.TrainingDiaSource.objects.all().delete()
        m.TrainingDiaObject.objects.all().delete()

    def test_training_tables_loaded( self, elasticc2_training ):
        assert m.PPDBDiaObject.objects.count() == self.__class__._ppdbdiaobjects
        assert m.PPDBDiaSource.objects.count() == self.__class__._ppdbdiasources
        assert m.PPDBAlert.objects.count() == self.__class__._ppdbalerts
        assert m.PPDBDiaForcedSource.objects.count() == self.__class__._ppdbdiaforcedsources
        assert m.DiaObjectTruth.objects.count() == self.__class__._ppdbdiaobjecttruths

        assert m.TrainingDiaObject.objects.count() == 138
        assert m.TrainingDiaSource.objects.count() == 429
        assert m.TrainingDiaForcedSource.objects.count() == 34284
        assert m.TrainingAlert.objects.count() == m.TrainingDiaSource.objects.count()
        assert ( m.TrainingDiaObjectTruth.objects.count()
                 == m.TrainingDiaObject.objects.count() )
        
