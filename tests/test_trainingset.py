import sys
import pathlib
import subprocess
import pytest

sys.path.insert( 0, "/tom_desc" )

import elasticc2.models

from msgconsumer import MsgConsumer

class TestTrainingSet:

    @pytest.fixture( scope="class" )
    def count_ppdb( self ):
        # IRRITATION.  When the fixture is called, it's not called with
        #  the same instance as the test will be called with.  So,
        #  we have to store class variables instead of instance variables.
        self.__class__._ppdbdiaobjects = elasticc2.models.PPDBDiaObject.objects.count()
        self.__class__._ppdbdiasources = elasticc2.models.PPDBDiaSource.objects.count()
        self.__class__._ppdbdiaforcedsources = elasticc2.models.PPDBDiaForcedSource.objects.count()
        self.__class__._ppdbalerts = elasticc2.models.PPDBAlert.objects.count()
        self.__class__._ppdbdiaobjecttruths = elasticc2.models.DiaObjectTruth.objects.count()
        
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

        elasticc2.models.TrainingDiaObjectTruth.objects.all().delete()
        elasticc2.models.TrainingAlert.objects.all().delete()
        elasticc2.models.TrainingDiaForcedSource.objects.all().delete()
        elasticc2.models.TrainingDiaSource.objects.all().delete()
        elasticc2.models.TrainingDiaObject.objects.all().delete()

    def test_tables_loaded( self, elasticc2_training ):
        assert elasticc2.models.PPDBDiaObject.objects.count() == self.__class__._ppdbdiaobjects
        assert elasticc2.models.PPDBDiaSource.objects.count() == self.__class__._ppdbdiasources
        assert elasticc2.models.PPDBAlert.objects.count() == self.__class__._ppdbalerts
        assert elasticc2.models.PPDBDiaForcedSource.objects.count() == self.__class__._ppdbdiaforcedsources
        assert elasticc2.models.DiaObjectTruth.objects.count() == self.__class__._ppdbdiaobjecttruths

        assert elasticc2.models.TrainingDiaObject.objects.count() == 14579
        assert elasticc2.models.TrainingDiaSource.objects.count() == 365300
        assert elasticc2.models.TrainingDiaForcedSource.objects.count() == 2497031
        assert elasticc2.models.TrainingAlert.objects.count() == elasticc2.models.TrainingDiaSource.objects.count()
        assert ( elasticc2.models.TrainingDiaObjectTruth.objects.count()
                 == elasticc2.models.TrainingDiaObject.objects.count() )
        
