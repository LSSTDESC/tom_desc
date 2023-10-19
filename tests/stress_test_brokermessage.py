# Intended to be run with pytest, but not automatically

import sys
import time
import datetime
import pytz
import random
import pytest
import logging

import django.db

from elasticc2.models import ( CassBrokerMessageBySource,
                               CassBrokerMessageByTime,
                               BrokerSourceIds,
                               BrokerClassifier,
                               BrokerMessage )


_logger = logging.getLogger("main")
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_logout.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                         datefmt='%Y-%m-%d %H:%M:%S' ) )
_logger.propagate = False
_logger.setLevel( logging.INFO )

@pytest.fixture(scope='module')
def lots_of_alerts():
    # Goal is to get a million classifications
    # 2 brokers * 2 ( classifiers / broker ) * ( 250,000 sources ) * ( 1 alert / (classifier*source )
    
    brokers = {
        'lots_test1': {
            '0.1': {
                'someclassifier': [ '1.0' ],
                'anotherclassifier': [ '1.0' ]
            }
        },
        'lots_test2': {
            '3.141': {
                'twistandshout': [ '42' ],
                'duckandcover': [ '23' ]
            }
        }
    }

    nsrc = 25000
    mincls = 1
    maxcls = 20

    nalerts = 2 * 2 * nsrc
    _logger.info( f"Generating {nalerts} random alerts..." )

    msgs = []
    for brokername, brokerspec in brokers.items():
        for brokerversion, versionspec in brokerspec.items():
            for classifiername, clsspec in versionspec.items():
                for classifierparams in clsspec:
                    for src in range(nsrc):
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

                        msgs.append( { 'alertId': src,
                                       'diaSourceId': src,
                                       'elasticcPublishTimestamp': datetime.datetime.now( tz=pytz.utc ),
                                       'brokerIngestTimestamp': datetime.datetime.now( tz=pytz.utc ),
                                       'brokerName': brokername,
                                       'brokerVersion': brokerversion,
                                       'classifierName': classifiername,
                                       'classifierParams': classifierparams,
                                       'classifications': [ { 'classId': c, 'probability': p }
                                                            for c, p, in zip( classes, probs ) ]
                                      } )

    alerts = []
    for i, msg in enumerate( msgs ):
        alerts.append( { 'topic': 'testing',
                         'msgoffset': i,
                         'timestamp': datetime.datetime.now( tz=pytz.utc ),
                         'msg': msg } )
    
                        
    _logger.info( f"...done generating {len(alerts)} random alerts" )

    yield alerts
    
@pytest.fixture
def load_cass( lots_of_alerts ):
    tinit = time.perf_counter()
    t0 = tinit
    n = 0
    delta = 2000
    printevery = 20000
    nextprint = printevery

    while n < len(lots_of_alerts):
        n1 = n + delta if n + delta < len(lots_of_alerts) else len(lots_of_alerts)
        CassBrokerMessageBySource.load_batch( lots_of_alerts[n:n1] )
        n = n1

        if n >= nextprint:
            t1 = time.perf_counter()
            _logger.info( f"Cass: loaded {n} of {len(lots_of_alerts)} alerts "
                          f"in {t1-tinit:.2f} sec (current rate {printevery/(t1-t0):.0f} s⁻¹)" )
            nextprint += printevery
            t0 = t1

    t1 = time.perf_counter()
    _logger.info( f"Cass: done loading {len(lots_of_alerts)} alerts in {t1-tinit:.2f} sec "
                  f"({len(lots_of_alerts)/(t1-tinit):.0f} s⁻¹)" )
        
    yield True

    cur = django.db.connection.cursor()
    cur.execute( "TRUNCATE TABLE elasticc2_brokersourceids" )
    cur.execute( "TRUNCATE TABLE elasticc2_brokerclassifier" )
    casscur = django.db.connections['cassandra'].connection.cursor()
    casscur.execute( "TRUNCATE TABLE tom_desc.cass_broker_message_by_time" )
    casscur.execute( "TRUNCATE TABLE tom_desc.cass_broker_message_by_source" )

    
@pytest.fixture(scope='module')
def load_pg( lots_of_alerts ):
    tinit = time.perf_counter()
    t0 = tinit
    n = 0
    delta = 2000
    printevery = 20000
    nextprint = printevery

    while n < len(lots_of_alerts):
        n1 = n + delta if n + delta < len(lots_of_alerts) else len(lots_of_alerts)
        BrokerMessage.load_batch( lots_of_alerts[n:n1] )
        n = n1

        if n >= nextprint:
            t1 = time.perf_counter()
            _logger.info( f"PG: loaded {n} of {len(lots_of_alerts)} alerts "
                          f"in {t1-tinit:.2f} sec (current rate {printevery/(t1-t0)} s⁻¹)" )
            nextprint += printevery
            t0 = t1
            
    t1 = time.perf_counter()
    _logger.info( f"PG: done loading {len(lots_of_alerts)} alerts in {t1-tinit:.2f} sec "
                  f"({len(lots_of_alerts)/(t1-tinit):.0f} s⁻¹)" )
        
    yield True

    cur = django.db.connection.cursor()
    cur.execute( "TRUNCATE TABLE elasticc2_brokersourceids" )
    cur.execute( "TRUNCATE TABLE elasticc2_brokerclassifier" )
    cur.execute( "TRUNCATE TABLE elasticc2_brokermessage" )

def test_cass( load_cass ):
    pass

def test_pg( load_pg ):
    pass
