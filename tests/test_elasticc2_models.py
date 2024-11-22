import pytest
import io
import datetime
import dateutil.parser
import pytz

from elasticc2.models import ( BrokerSourceIds,
                               BrokerClassifier,
                               BrokerMessage )

#TODO : lots more

class TestElasticc2Models:

    @pytest.fixture( scope='class' )
    def alerts( self, random_broker_classifications ):
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
                             'msgoffset': i,
                             'timestamp': datetime.datetime.now( tz=pytz.utc ),
                             'msg': alertdict
                            } )
        yield alerts

        # Hardcoded from what I know is in random_broker_classifications 
        cfers = BrokerClassifier.objects.filter( brokername__in=[ 'rbc_test1', 'rbc_test2'] )
        
        # This IN will be slow if the number of messages is too big (which it won't be)
        BrokerSourceIds.objects.filter( diasource_id__in=sourceids ).delete()

        BrokerClassifier.objects.filter( classifier_id__in=[ i.classifier_id for i in cfers ] ).delete()


        
    
    @pytest.fixture( scope='class' )
    def loaded_broker_classifications( self, alerts ):
        yield BrokerMessage.load_batch( alerts )

        # Hardcoded from what I know is in random_broker_classifications 
        cfers = BrokerClassifier.objects.filter( brokername__in=[ 'rbc_test1', 'rbc_test2'] )

        BrokerMessage.objects.filter( classifier_id__in=[ i.classifier_id for i in cfers ] ).delete()
    
    def test_hello_world( self ):
        # This is just here so I can get a timestamp to see how long the next test took
        assert True

    def test_brokermessage_bulk( self, loaded_broker_classifications ):
        assert BrokerMessage.objects.count() >= loaded_broker_classifications[ 'addedmsgs' ]
        cfers = BrokerClassifier.objects.filter( brokername__in=[ 'rbc_test1', 'rbc_test2' ] )
        msgs = BrokerMessage.objects.filter( classifier_id__in=[ i.classifier_id for i in cfers ] )
        assert msgs.count() == loaded_broker_classifications[ 'addedmsgs' ]
        sources = set()
        for msg in msgs.all():
            sources.add( msg.diasource_id )
        assert sources.issubset( set( [ b.diasource_id for b in BrokerSourceIds.objects.all() ] ) )
