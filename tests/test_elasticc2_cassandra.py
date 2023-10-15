import pytest
import os
import sys
import random
import datetime
import pytz

# The next 4 lines are necessary to get access to
#  django models without doing python manage.py shell
sys.path.insert( 0, "/tom_desc" )
os.environ["DJANGO_SETTINGS_MODULE"] = "tom_desc.settings"
import django
django.setup()

import django.db

import elasticc2.models as m

class TestElasticc2Cassandra:

    #TODO lots more

    @pytest.fixture(scope='class')
    def load_some( self ):
        brokers = {
            'test1': {
                '1.0': {
                    'classifiertest1': [ '1.0' ],
                    'classifiertest2': [ '1.0' ]
                }
            },
            'test2': {
                '3.5': {
                    'testing1': [ '42' ],
                    'testing2': [ '23' ]
                }
            }
        }

        minsrc = 10
        maxsrc = 20
        mincls = 1
        maxcls = 20

        msgs = []
        classifier_id = 0
        for brokername, brokerspec in brokers.items():
            for brokerversion, versionspec in brokerspec.items():
                for classifiername, clsspec in versionspec.items():
                    for classifierparams in clsspec:
                        nsrcs = random.randint( minsrc, maxsrc )
                        for src in range(nsrcs):
                            ncls = random.randint( mincls, maxcls )
                            probleft = 1.0
                            classes = []
                            probs = []
                            for cls in range( ncls ):
                                classes.append( cls )
                                prob = random.random() * probleft
                                probleft -= prob
                                probs.append( prob )
                            classes.append( ncls )
                            probs.append( probleft )

                        msg = m.CassBrokerMessage( sourceid=src,
                                                   classifier_id=classifier_id,
                                                   alertid=src,
                                                   elasticcpublishtimestmp=datetime.datetime.now( tz=pytz.utc ),
                                                   brokeringesttimestamp=datetime.datetime.now( tz=pytz.utc ),
                                                   classid=classes,
                                                   probability=probs )
                        msgs.append( msg )

                        classifier_id += 1

        m.CassBrokerMessage.objects.bulk_create( msgs )

        yield true

        conn = django.db.connections['cassandra'].cursor().connection
        conn.execute( "TRUNCATE TABLE tom_desc.cass_broker_message" )

    @pytest.mark.xfail( reason="Fix fixture to not use no-longer-existing bulk_create" )
    def test_some_loaded( self, load_some ):
        import pdb; pdb.set_trace()
        assert m.CassBrokerMessage.objects.count() > 0
