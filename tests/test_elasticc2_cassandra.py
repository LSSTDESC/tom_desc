import os
import sys

# The next 4 lines are necessary to get access to
#  django models without doing python manage.py shell
sys.path.insert( 0, "/tom_desc" )
os.environ["DJANGO_SETTINGS_MODULE"] = "tom_desc.settings"
import django
django.setup()

import elastdicc2.models.as m

class TestElasticc2Cassandra:

    @pytest.fixture(scope='class')
    def load_batch( self ):
        
