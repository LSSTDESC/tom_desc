# Tests depend on a completely fresh environment, just started with
# docker compose.  See test_alertcycle.py.

import sys
import os
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
        objs = elasticc2.models.DiaObject.objects.all().order_by("diaobject_id")[0:10]

        expected_objids = [ 2612942, 5855504, 10898484, 11440362, 16410870,
                            16768849, 16828729, 19452966, 22936696, 25775369 ]
        objids = [ o.diaobject_id for o in objs ]
        assert objids == expected_objids

        import pdb; pdb.set_trace()

        req = { 'requester': 'tests',
                'objectids': [ objs[0].diaobject_id, objs[4].diaobject_id, objs[9].diaobject_id ],
                'priorities': [ 5, 1, 3 ] }
        res = tomclient.request( 'POST', 'elasticc2/askforspectrum', json=req )

        assert res.status_code == 200

        yield True

        elasticc2.models.WantedSpectra.filter( wantspec_id__in( [ '2612942 ; tests',
                                                                  '16410870 ; tests',
                                                                  '25775369 ; tests' ] ) ).delete()

    def test_ask_for_spectra( self, ask_for_spectra, tomclient ):
        wnts = list( elasticc2.models.WantedSpectra.objects.all().order_by("wantspec_id") )
        oids = [ 2612942, 16410870, 25775369 ]
        prios = [ 5, 1, 3 ]
        for i in range(len(oids)):
            assert wnts[i].wantspec_id == f"{oids[i]} ; tests"
            assert wnts[i].diaobject_id == oids[i]
            assert wnts[i].requester == 'tests'
            assert wnts[i].priority == prios[i]



