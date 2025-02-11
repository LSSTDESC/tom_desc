# Tests depend on a completely fresh environment, just started with
# docker compose.  See test_alertcycle.py.

import sys
import os
import random
import time
import datetime
import dateutil.parser
import pytest

import numpy
import pandas
import astropy.time

sys.path.insert( 0, "/tom_desc" )
os.environ["DJANGO_SETTINGS_MODULE"] = "tom_desc.settings"
import django
django.setup()

import elasticc2.models
from tom_client import TomClient

# I'm abusing pytest here by having tests depend on previous
#  tests, rather than making all dependencies fixtures.
# I may fix that at some point.
# (But, truthfully, pytest abuses python so badly that
# you might as well just embrace it.)

class TestSpectrumCycle:

    @pytest.fixture( scope='class' )
    def ask_for_spectra( self, elasticc2_database_snapshot_class, tomclient ):
        objs = elasticc2.models.DiaObject.objects.all().order_by("diaobject_id")
        objs = list( objs )

        assert len( objs ) == 131
        objs = [ o.diaobject_id for o in objs[ 0:25 ] ]

        random.seed( 42 )
        prios = [ random.randint( 1, 5 ) for i in range( len( objs ) ) ]

        req = { 'requester': 'tests',
                'objectids': objs,
                'priorities': prios }
        res = tomclient.post( 'elasticc2/askforspectrum', json=req )

        assert res.status_code == 200

        yield objs, prios

        elasticc2.models.WantedSpectra.objects.all().delete()


    # TODO : test things other than detected_since_mjd sent to gethottransients
    def test_hot_sne( self, elasticc2_database_snapshot_class, tomclient ):
        # Make sure it rejects bad keywords
        res = tomclient.post( 'elasticc2/gethottransients', json={ 'foo': 0 } )
        assert res.status_code == 500
        assert res.text == "Error, unknown parameters passed in request body: ['foo']"

        # Testing detected_in_last_days is fraught because
        #   the mjds in elasticc2 are what they are, are
        #   in the future (as of this comment writing).
        # So, go old school and just not test it.
        # (Should test with mjd_now...)

        res = tomclient.post( 'elasticc2/gethottransients', json={ 'detected_since_mjd': 60660 } )
        assert res.status_code == 200
        sne = res.json()['diaobject']
        assert len(sne) == 8

        snids = { s['objectid'] for s in sne }
        assert snids == { 15232, 1913410, 2110476, 416626, 1286131, 1684659, 1045654, 1263066 }

        # Should probably check more than this...
        assert set( sne[0].keys() ) == { 'objectid', 'ra', 'dec', 'photometry', 'zp', 'redshift', 'sncode' }
        assert set( sne[0]['photometry'].keys() ) == { 'mjd', 'band', 'flux', 'fluxerr' }

        # Make sure the include_hostinfo parameter works
        res = tomclient.post( 'elasticc2/gethottransients', json={ 'detected_since_mjd': 60660,
                                                                   'include_hostinfo': 1 } )
        assert res.status_code == 200
        sne = res.json()['diaobject']
        assert len(sne) == 8
        snids = { s['objectid'] for s in sne }
        assert snids == { 15232, 1913410, 2110476, 416626, 1286131, 1684659, 1045654, 1263066 }
        assert set( sne[0].keys() ) == { 'objectid', 'ra', 'dec', 'photometry', 'zp', 'redshift', 'sncode',
                                         'hostgal_mag_u','hostgal_magerr_u',
                                         'hostgal_mag_g','hostgal_magerr_g',
                                         'hostgal_mag_r','hostgal_magerr_r',
                                         'hostgal_mag_i','hostgal_magerr_i',
                                         'hostgal_mag_z','hostgal_magerr_z',
                                         'hostgal_mag_y','hostgal_magerr_y',
                                         'hostgal_ellipticity', 'hostgal_sqradius', 'hostgal_snsep'
                                        }
        assert set( sne[0]['photometry'].keys() ) == { 'mjd', 'band', 'flux', 'fluxerr' }

        # Try return format 2
        res = tomclient.post( 'elasticc2/gethottransients', json={ 'detected_since_mjd': 60660,
                                                                   'return_format': 2 } )
        assert res.status_code == 200
        df = pandas.DataFrame( res.json()['diaobject'] )
        assert len(df) == 8
        assert set( df.objectid.values ) == { 15232, 1913410, 2110476, 416626, 1286131, 1684659, 1045654, 1263066 }
        assert set( df.columns ) == { 'objectid', 'ra', 'dec', 'mjd', 'band', 'flux', 'fluxerr',
                                      'zp', 'redshift', 'sncode' }
        assert df.mjd.dtype == numpy.dtype('O')
        assert len( df.mjd[0] ) > 1

        res = tomclient.post( 'elasticc2/gethottransients', json={ 'detected_since_mjd': 60660,
                                                                   'include_hostinfo': 1,
                                                                   'return_format': 2 } )
        assert res.status_code == 200
        df = pandas.DataFrame( res.json()['diaobject'] )
        assert len(df) == 8
        assert set( df.objectid.values ) == { 15232, 1913410, 2110476, 416626, 1286131, 1684659, 1045654, 1263066 }
        assert set( df.columns ) == { 'objectid', 'ra', 'dec', 'mjd', 'band', 'flux', 'fluxerr',
                                      'zp', 'redshift', 'sncode',
                                      'hostgal_mag_u','hostgal_magerr_u',
                                      'hostgal_mag_g','hostgal_magerr_g',
                                      'hostgal_mag_r','hostgal_magerr_r',
                                      'hostgal_mag_i','hostgal_magerr_i',
                                      'hostgal_mag_z','hostgal_magerr_z',
                                      'hostgal_mag_y','hostgal_magerr_y',
                                      'hostgal_ellipticity', 'hostgal_sqradius', 'hostgal_snsep'
                                     }
        assert df.mjd.dtype == numpy.dtype('O')
        assert len( df.mjd[0] ) > 1



    def test_ask_for_spectra( self, ask_for_spectra, tomclient ):
        objs, prios = ask_for_spectra
        wnts = list( elasticc2.models.WantedSpectra.objects.all().order_by("diaobject_id") )
        for wnt, obj, prio in zip( wnts, objs, prios ) :
            assert wnt.wantspec_id == f"{obj} ; tests"
            assert wnt.diaobject_id == obj
            assert wnt.requester == "tests"
            assert wnt.priority == prio

        # Verify that if we ask again for a spectrum, it overwrites the previous request
        lstobj = objs[-1]
        lstprio = prios[-1]
        newprio = lstprio + 1 if lstprio < 5 else 1

        oldn = elasticc2.models.WantedSpectra.objects.count()
        old_dbobj = elasticc2.models.WantedSpectra.objects.filter( requester='tests',
                                                                   diaobject_id=lstobj )[0]
        assert old_dbobj.priority == lstprio

        res = tomclient.post( 'elasticc2/askforspectrum', json={ 'requester': 'tests',
                                                                 'objectids': [ lstobj ],
                                                                 'priorities': [ newprio ] } )
        assert res.status_code == 200

        assert elasticc2.models.WantedSpectra.objects.count() == oldn
        dbobjs = elasticc2.models.WantedSpectra.objects.filter( requester='tests',
                                                                diaobject_id=lstobj )
        assert len( dbobjs ) == 1
        assert dbobjs[0].wanttime > old_dbobj.wanttime
        assert dbobjs[0].priority == newprio


    def test_what_are_wanted_initial( self, ask_for_spectra, tomclient ):
        objs, prios = ask_for_spectra

        # Ask for no limiting magnitude, and anything with any detection
        res = tomclient.post( 'elasticc2/spectrawanted', json={ 'detected_since_mjd': 0 } )

        wantedobjs = res.json()
        assert wantedobjs['status'] == 'ok'
        wantedobjs = wantedobjs['wantedspectra']
        assert isinstance( wantedobjs, list )

        assert len( wantedobjs ) == 25

        # I should do better than just check the first one...
        assert wantedobjs[0]['oid'] == 114982
        assert wantedobjs[0]['ra'] == pytest.approx( 93.721805, abs=0.25/3600. )   # cos(dec) term...
        assert wantedobjs[0]['dec'] == pytest.approx( -65.509372, abs=0.25/3600. )
        assert wantedobjs[0]['prio'] == 5
        assert wantedobjs[0]['latest']['i']['mjd'] == pytest.approx( 60294.17, abs=0.01 )
        assert wantedobjs[0]['latest']['i']['mag'] == pytest.approx( 19.125, abs=0.01 )

        # Now test that we only get things that have been detected since mjd 60660
        # First, figure out what we expect:
        objnewenough = set()
        for objinfo in wantedobjs:
            for band, bandinfo in objinfo['latest'].items():
                if bandinfo['mjd'] >= 60660:
                    objnewenough.add( objinfo['oid'] )
                    break

        # Then, see if we get those when we ask for it
        res = tomclient.post( 'elasticc2/spectrawanted', json={ 'detected_since_mjd': 60660 } )
        detsince_specinfo = res.json()['wantedspectra']
        assert { o['oid'] for o in detsince_specinfo } == objnewenough

        # Ask for more spectra then make sure that if we ask for ones that
        #   were requested since a certain time, we only get the new ones
        # Strip the format down to YYYY-MM-DDTHH:MM:SS
        # Wait a second before and after to make sure the roundoff doesn't screw the test
        time.sleep( 1 )
        wanttime = datetime.datetime.now( tz=datetime.timezone.utc ).isoformat()[0:19]
        time.sleep( 1 )

        newobjs = elasticc2.models.DiaObject.objects.all().order_by("diaobject_id")
        newobjs = [ o for o in newobjs if o.diaobject_id not in objs ]
        try:
            req = { 'requester': 'tests',
                    'objectids': [ o.diaobject_id for o in newobjs[0:3] ],
                    'priorities': [ 1, 2, 3 ] }
            res = tomclient.post( 'elasticc2/askforspectrum', json=req )
            assert res.status_code == 200

            # Ask for everyting wanted since forever ago so we get the three new ones;
            #   we should get *just* the three new ones because of requested_since
            res = tomclient.post( 'elasticc2/spectrawanted', json={ 'requested_since': wanttime,
                                                                    'detected_since_mjd': 0. } )
            wantedobjs = res.json()[ 'wantedspectra' ]
            assert set( [ i['oid'] for i in wantedobjs ] ) == set( req['objectids'] )
            pass

        finally:
            # Clean out the additional ones we just asked for from the database, to avoid munging future tests
            elasticc2.models.WantedSpectra.objects.filter(
                diaobject_id__in=[ o.diaobject_id for o in newobjs[0:3] ]
            ).delete()


    def test_plan_spectrum( self, ask_for_spectra, tomclient ):
        objs, prios = ask_for_spectra
        wanted = tomclient.post( 'elasticc2/spectrawanted', json={ 'detected_since_mjd': 0 } )

        wanted0 = wanted.json()['wantedspectra'][0]
        mjd = 0.
        for band, info in wanted0['latest'].items():
            if float( info['mjd'] ) > mjd:
                mjd = float( info['mjd'] )
        plantime = astropy.time.Time( mjd + 2, format='mjd' ).isot

        res = tomclient.post( 'elasticc2/planspectrum',
                              json={ 'objectid': wanted0['oid'],
                                     'plantime': plantime,
                                     'facility': 'Test Spectrumifier' } )

        assert elasticc2.models.PlannedSpectra.objects.count() == 1

        retval = res.json()
        assert retval['status'] == 'ok'
        assert retval['objectid'] == wanted0['oid']
        assert retval['facility'] == 'Test Spectrumifier'
        assert ( astropy.time.Time( retval['created_at'] ).mjd
                 == pytest.approx( astropy.time.Time( datetime.datetime.now() ).mjd, abs=0.0003 ) )
        assert astropy.time.Time( retval['plantime'] ).mjd == pytest.approx( mjd+2, abs=0.01 )
        assert retval['comment'] == ''

        firstcreatetime = dateutil.parser.parse( retval['created_at'] )

        # Make sure this object no longer shows up in wanted spectra if we ask
        #   for things

        res = tomclient.post( 'elasticc2/spectrawanted', json={ 'detected_since_mjd': 0,
                                                                'not_claimed_in_last_days': 1 } )
        wantedobjs = res.json()
        assert wantedobjs['status'] == 'ok'
        assert len( wantedobjs['wantedspectra'] ) == 24

        # But make sure it shows up if we tell it to ignore claims
        res = tomclient.post( 'elasticc2/spectrawanted', json={ 'detected_since_mjd': 0,
                                                                'not_claimed_in_last_days': 0 } )
        wantedobjs = res.json()
        assert wantedobjs['status'] == 'ok'
        assert len( wantedobjs['wantedspectra'] ) == 25

        # Make sure that the entry gets replaced if we resubmit

        res = tomclient.post( 'elasticc2/planspectrum',
                              json={ 'objectid': res.json()['wantedspectra'][0]['oid'],
                                     'plantime': astropy.time.Time( mjd + 3, format='mjd' ).isot,
                                     'facility': 'Test Spectrumifier' } )

        plans = elasticc2.models.PlannedSpectra.objects.all()
        assert len(plans) == 1
        plan = plans[0]
        assert astropy.time.Time( plan.plantime ).mjd == pytest.approx( astropy.time.Time( plantime ).mjd + 1,
                                                                        abs=0.01 )
        assert plan.facility == 'Test Spectrumifier'
        assert plan.diaobject_id == wanted0['oid']
        assert plan.comment == ''
        assert plan.created_at > firstcreatetime

        # Make sure that the entry is not replaced if we use a different facility

        newplantime = ( astropy.time.Time( plantime ) + astropy.time.TimeDelta( 1, format='jd' )  ).isot
        res = tomclient.post( 'elasticc2/planspectrum',
                              json={ 'objectid': wanted0['oid'],
                                     'plantime': newplantime,
                                     'facility': 'Test Spectrumifier 2' } )
        assert elasticc2.models.PlannedSpectra.objects.count() == 2

        # Make sure we can remove a plan

        res = tomclient.post( 'elasticc2/removespectrumplan',
                              json={ 'objectid': wanted0['oid'],
                                     'facility': 'Test Spectrumifier' } )
        assert res.status_code == 200
        res = res.json()
        assert res['status'] == 'ok'
        assert res['facility'] == 'Test Spectrumifier'
        assert res['objectid'] == wanted0['oid']
        assert res['n_deleted'] == 1

        plans = elasticc2.models.PlannedSpectra.objects.all()
        assert len(plans) == 1
        assert plans[0].facility == 'Test Spectrumifier 2'


    def test_report_spectrum( self, ask_for_spectra, tomclient ):
        plans = elasticc2.models.PlannedSpectra.objects.all()
        oid = plans[0].diaobject_id
        facility = plans[0].facility

        res = tomclient.post( 'elasticc2/reportspectruminfo', json={ 'objectid': oid,
                                                                     'facility': facility,
                                                                     'mjd': 65536.,
                                                                     'z': 0.25,
                                                                     'classid': 2222    #SN Ia
                                                                    } )
        res = res.json()
        assert res['status'] == 'ok'
        assert res['objectid'] == oid
        assert res['facility'] == facility
        assert res['mjd'] == pytest.approx( 65536., abs=0.01 )
        assert res['z'] == pytest.approx( 0.25, abs=0.001 )
        assert res['classid'] == 2222
        now = astropy.time.Time( datetime.datetime.now() ).mjd
        instime = astropy.time.Time( res['inserted_at'] ).mjd
        assert instime == pytest.approx( now, abs=0.01 )

        # Make sure the entry is there

        sinfos = elasticc2.models.SpectrumInfo.objects.all()
        assert len(sinfos) == 1
        assert sinfos[0].facility == 'Test Spectrumifier 2'
        assert sinfos[0].diaobject_id == oid
        assert sinfos[0].mjd == pytest.approx( 65536., abs=0.01 )
        assert sinfos[0].z == pytest.approx( 0.25, abs=0.001 )
        assert sinfos[0].classid == 2222
        assert ( astropy.time.Time( sinfos[0].inserted_at ).mjd ==
                 pytest.approx( astropy.time.Time( datetime.datetime.now() ).mjd, abs=3./3600./24. ) )

        # Make sure both the plan and the wanted went away

        assert elasticc2.models.PlannedSpectra.objects.count() == 0
        wanted = elasticc2.models.WantedSpectra.objects.all()
        assert len(wanted) == 24
        assert oid not in [ o.diaobject_id for o in wanted ]

    def test_get_spectrum_info( self, ask_for_spectra, tomclient ):
        # Get everything

        res = tomclient.post( 'elasticc2/getknownspectruminfo', json={} )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'ok'
        assert len( data['spectra'] ) == 1
        assert data['spectra'][0]['objectid'] == 114982
        assert data['spectra'][0]['mjd'] == pytest.approx( 65536., abs=0.01 )
        assert data['spectra'][0]['z'] == pytest.approx( 0.25, abs=0.01 )
        assert data['spectra'][0]['classid'] == 2222


    def test_cleanup( self ):
        # Lots of previous tests left stuff in the database.  Clean it out.
        elasticc2.models.SpectrumInfo.objects.all().delete()
        elasticc2.models.WantedSpectra.objects.all().delete()
        elasticc2.models.PlannedSpectra.objects.all().delete()
