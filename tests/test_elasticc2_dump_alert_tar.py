import pytest
import sys
import pathlib
import subprocess
import tarfile
import fastavro

sys.path.insert( 0, "/tom_desc" )
import elasticc2.models

from alertcyclefixtures import *

class TestDumpAlertTar:
    def test_dump_tar( self, elasticc2_ppdb_class ):
        try:
            # Just make sure things are as expected
            assert elasticc2.models.PPDBDiaObject.objects.count() == 346
            assert elasticc2.models.PPDBDiaSource.objects.count() == 1862
            assert elasticc2.models.PPDBAlert.objects.count() == elasticc2.models.PPDBDiaSource.objects.count()
            assert elasticc2.models.PPDBDiaForcedSource.objects.count() == 52172
            assert elasticc2.models.DiaObjectTruth.objects.count() == elasticc2.models.PPDBDiaObject.objects.count()

            # The test set ppdb has mjds between 60278.029 and 61378.1214

            res = subprocess.run( [ "python", "manage.py", "dump_elasticc2_alert_tars",
                                    "--start-day", "60278", "-d", "60286",
                                    "-s", "/tests/schema/elasticc.v0_9_1.alert.avsc",
                                    "-o", "/tests", "-c", "--do" ],
                                  cwd="/tom_desc", capture_output=True )
            assert res.returncode == 0

            # Check that the expected tar files exist, and spot-check one
            assert all( pathlib.Path( f"/tests/{i}.tar" ).is_file()
                        for i in [ 60278, 60279, 60281, 60282, 60283, 60286 ] )
            with tarfile.TarFile( "/tests/60281.tar" ) as tf:
                assert set( tf.getnames() ) ==  { '102879800005.avro', '155218500008.avro', '155218500009.avro',
                                                  '155218500010.avro', '155218500011.avro', '155218500012.avro',
                                                  '155218500013.avro'}
                schema = fastavro.schema.load_schema( '/tests/schema/elasticc.v0_9_1.alert.avsc' )
                alert = fastavro.schemaless_reader( tf.extractfile( '155218500011.avro' ), schema )
                assert alert['alertId'] == 155218500011
                assert alert['diaSource']['diaSourceId'] == 155218500011
                assert alert['diaSource']['midPointTai'] == pytest.approx( 60281.0959, abs=0.0001 )
                assert alert['diaSource']['psFlux'] == pytest.approx( 4440.86, abs=0.01 )
                assert alert['diaSource']['psFluxErr'] == pytest.approx( 256.23, abs=0.01 )

        finally:
            # Clean up
            for i in range( 60278, 60287 ):
                tarp = pathlib.Path( f"/tests/{i}.tar" )
                tarp.unlink( missing_ok=True )
