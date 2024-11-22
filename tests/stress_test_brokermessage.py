# Intended to be run with pytest, but not automatically

import sys
import time
import datetime
import pytz
import random
import pytest
import logging
import threading
import subprocess

import django.db

from elasticc2.models import ( BrokerSourceIds,
                               BrokerClassifier,
                               BrokerMessage,
                               PPDBDiaObject,
                               PPDBDiaSource,
                               PPDBAlert )


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

    alertsenttime = datetime.datetime.now( tz=pytz.utc ) - datetime.timedelta( hours=1 )

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

    nsrc = 100000
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

    # Load fake alerts into the database

    obj = PPDBDiaObject( diaobject_id=1, isddf=False, ra=2., decl=2. )
    obj.save()
    srcs = []
    for msg in msgs:
        srcs.append( { 'diasource_id': msg['diaSourceId'],
                       'midpointtai': 60000.,
                       'filtername': 'r',
                       'ra': 2.,
                       'decl': 2.,
                       'psflux': 1000.,
                       'psfluxerr': 10.,
                       'snr': 100.,
                       'diaobject_id': obj.diaobject_id } )
    PPDBDiaSource.bulk_load_or_create( srcs )
    alobjs = []
    for msg in msgs:
        alobjs.append( { 'alert_id': msg['alertId'],
                         'alertsenttimestamp': alertsenttime,
                         'diasource_id': msg['diaSourceId'],
                         'diaobject_id': obj.diaobject_id } )
    PPDBAlert.bulk_load_or_create( alobjs )
    # bulk_load_or_create wont actually set alertsenttimestamp
    cur = django.db.connection.cursor()
    cur.execute( "UPDATE elasticc2_ppdbalert SET alertsenttimestamp=%(t)s", { 't': alertsenttime } )

    _logger.info( f"...done loading fake alerts into database" )

    yield alerts

    cur = django.db.connection.cursor()
    cur.execute( "TRUNCATE TABLE elasticc2_ppdbdiaobject CASCADE" )
    cur.execute( "TRUNCATE TABLE elasticc2_ppdbdiasource CASCADE" )
    cur.execute( "TRUNCATE TABLE elasticc2_ppdbalert" )


class TestPostgres:

    @pytest.fixture(scope='class')
    def load_pg( self, lots_of_alerts ):
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
                              f"in {t1-tinit:.2f} sec (current rate {printevery/(t1-t0):.0f} s⁻¹)" )
                nextprint += printevery
                t0 = t1

        t1 = time.perf_counter()
        _logger.info( f"PG: done loading {len(lots_of_alerts)} alerts in {t1-tinit:.2f} sec "
                      f"({len(lots_of_alerts)/(t1-tinit):.0f} s⁻¹)" )

        yield lots_of_alerts

        cur = django.db.connection.cursor()
        cur.execute( "TRUNCATE TABLE elasticc2_brokersourceids" )
        cur.execute( "TRUNCATE TABLE elasticc2_brokerclassifier" )
        cur.execute( "TRUNCATE TABLE elasticc2_brokermessage" )

    def test_pg( self, load_pg ):
        bms = BrokerMessage.objects.all()
        assert len(bms) == len(load_pg)

    def test_gen_brokerdelay( self, load_pg ):
        t0 = ( datetime.datetime.now( tz=pytz.utc ) + datetime.timedelta( days=-1 ) ).date().isoformat()
        t1 = ( datetime.datetime.now( tz=pytz.utc ) + datetime.timedelta( days=2 ) ).date().isoformat()
        dt = 1

        tstart = time.perf_counter()
        res = subprocess.run( [ "python", "manage.py", "gen_elasticc2_brokerdelaygraphs_pg",
                                "--t0", t0, "--t1", t1, "--dt", str(dt) ],
                              cwd="/tom_desc", capture_output=True )
        assert res.returncode == 0
        tend = time.perf_counter()
        _logger.info( f"gen_elasticc2_brokerdelaygraphs took {tend-tstart:.1f} sec" )


