import pathlib
import subprocess
import pytest

from tom_client import TomClient

@pytest.fixture( scope="session" )
def tomclient():
    return TomClient( "http://tom:8080", username="root", password="testing" )

@pytest.fixture( scope="session" )
def elasticc_ppdb( tomclient ):
    import pdb; pdb.set_trace()
    basedir = pathlib.Path( "/elasticc2data" )
    dirs = []
    for subdir in basedir.glob( '*' ):
        if subdir.is_dir():
            result = subprocess.run( [ "python", "manage.py", "load_snana_fits", "-d", str(subdir), "--ppdb", "--do" ],
                                     cwd="/tom_desc", capture_output=True )
            assert result.returncode == 0

    yield True

    #TODO : clean out database.  Not worth it, since
    # this is a session-scope fixture, and the database
    # is created just for the tests in the docker compose file.
    


    
