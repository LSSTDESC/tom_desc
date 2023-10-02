import pytest
import io
import datetime
import pytz

from elasticc2.models import CassBrokerMessage, BrokerSourceIds

#TODO : lots more

class TestElasticc2Models:

    @pytest.fixture( scope='class' )
    def loaded_broker_classifications( self, random_broker_classifications ):
        # Some of the hardcodes here depend on details of what's inside
        # random_broker_classifications in conftest.py

        alerts = []
        sourceids = []
        for i, msg in enumerate( random_broker_classifications ):
            msgio = io.BytesIO()
            alertdict = { 'alertId': msg['sourceid'],
                          'diaSourceId': msg['sourceid'],
                          'elasticcPublishTimestamp': msg['elasticcpublishtimestamp'],
                          'brokerIngestTimestamp': msg['brokeringesttimestamp'],
                          'brokerName': msg['brokername'],
                          'brokerVersion': msg['brokerversion'],
                          'classifierName': msg['classifiername'],
                          'classifierParams': msg['classifierparams'],
                          'classifications': [ { 'classId': c, 'probability': p }
                                               for c, p in zip( msg['classid'], msg['probability'] ) ]
                         }
            sourceids.append( msg['sourceid'] )
            alerts.append( { 'topic': 'testing',
                             'msgoiffset': i,
                             'timestamp': datetime.datetime.now( tz=pytz.utc ),
                             'msg': alertdict
                            } )

        yield CassBrokerMessage.load_batch( alerts )

        # This doesn't work; Cassandra is picky about deleting stuff in bulk
        # CassBrokerMessage.objects.filter( brokername__in=[ 'rbc_test1', 'rbc_test2' ] ).delete()
        # So we do the slow thing, which will be OK given the small number of messages
        msgs = CassBrokerMessage.objects.filter( brokername__in=[ 'rbc_test1', 'rbc_test2'] )
        for msg in msgs:
            msg.delete()

        # This IN will be slow if the number of messages is too big (which it won't be)
        BrokerSourceIds.objects.filter( diasource_id__in=sourceids ).delete()

    
    def test_hello_world( self ):
        # This is just here so I can get a timestamp to see how long the next test took
        assert True
    

    def test_cassbrokermessage_bulk( self, loaded_broker_classifications ):
        assert CassBrokerMessage.objects.count() >= loaded_broker_classifications[ 'addedmsgs' ]
        msgs = CassBrokerMessage.objects.filter( brokername__in=[ 'rbc_test1', 'rbc_test2' ] )
        assert msgs.count() == loaded_broker_classifications[ 'addedmsgs' ]
        sources = set()
        for msg in msgs.all():
            sources.add( msg.sourceid )
        assert sources.issubset( set( [ b.diasource_id for b in BrokerSourceIds.objects.all() ] ) )
