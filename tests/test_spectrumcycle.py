# Tests depend on a completely fresh environment, just started with
# docker compose.  See test_alertcycle.py.

import sys
import os
import random
import pytest

sys.path.insert( 0, "/tom_desc" )
os.environ["DJANGO_SETTINGS_MODULE"] = "tom_desc.settings"
import django
django.setup()

import elasticc2.models
from tom_client import TomClient

from alertcyclefixtures import *

class TestSpectrumCycle:

    @pytest.fixture( scope='class' )
    def ask_for_spectra( self, update_diasource_100daysmore, tomclient ):
        objs = elasticc2.models.DiaObject.objects.all().order_by("diaobject_id")
        objs = list( objs )

        assert len( objs ) == 72
        objs = [ o.diaobject_id for o in objs[ 0:25 ] ]

        random.seed( 42 )
        prios = [ random.randint( 1, 5 ) for i in range( len( objs ) ) ]

        req = { 'requester': 'tests',
                'objectids': objs,
                'priorities': prios }
        res = tomclient.request( 'POST', 'elasticc2/askforspectrum', json=req )

        assert res.status_code == 200

        yield objs, prios

        elasticc2.models.WantedSpectra.objects.all().delete()

    def test_ask_for_spectra( self, ask_for_spectra, tomclient ):
        objs, prios = ask_for_spectra
        wnts = list( elasticc2.models.WantedSpectra.objects.all().order_by("diaobject_id") )
        for wnt, obj, prio in zip( wnts, objs, prios ) :
            assert wnt.wantspec_id == f"{obj} ; tests"
            assert wnt.diaobject_id == obj
            assert wnt.requester == "tests"
            assert wnt.priority == prio

    def test_what_are_wanted_initial( self, ask_for_spectra, tomclient ):
        objs, prios = ask_for_spectra

        # Ask for no limiting magnitude, and anything with any detection
        res = tomclient.request( 'POST', 'elasticc2/spectrawanted', json={ 'detected_since': 0 } )

        specinfo_strkey = res.json()
        specinfo = { int(k): v for k, v in specinfo_strkey.items() }
        wantedobjids = list( specinfo.keys() )
        wantedobjids.sort()

        # I should do better than just check the first one...
        assert len( wantedobjids ) == 25
        assert wantedobjids[0] == 2612942
        assert specinfo[wantedobjids[0]]['prio'] == 1
        assert specinfo[wantedobjids[0]]['req'] == 'tests'
        assert specinfo[wantedobjids[0]]['latest']['i']['mjd'] == pytest.approx( 61114.348, abs=0.01 )
        assert specinfo[wantedobjids[0]]['latest']['i']['mag'] == pytest.approx( 18.259, abs=0.01 )

        # Now test that we only get things that have been detected since mjd 60910
        # First, figure out what we expect:
        objnewenough = set()
        for objid, objinfo in specinfo.items():
            for band, bandinfo in objinfo['latest'].items():
                if bandinfo['mjd'] >= 60910:
                    objnewenough.add( objid )
                    break

        # Then, see if we get those when we ask for it
        res = tomclient.request( 'POST', 'elasticc2/spectrawanted', json={ 'detected_since': 60910 } )
        detsince_specinfo_strkey = res.json()
        detsince_specinfo = { int(k): v for k, v in detsince_specinfo_strkey.items() }
        assert set( detsince_specinfo.keys() ) == objnewenough
