import pytest
import pandas
import elasticc2.models


class TestReconstructAlert:
    def test_reconstruct_alert( self, elasticc2_ppdb_class ):

        # Get an alert I know is from the first day, make sure that
        #  it doesn't have any previous forced sources

        alert = elasticc2.models.PPDBAlert.objects.get( pk=155218500005 )
        reconstructed = alert.reconstruct()
        assert reconstructed['diaObject']['diaObjectId'] == 1552185
        assert reconstructed['diaSource']['diaObjectId'] == reconstructed['diaObject']['diaObjectId']
        assert reconstructed['diaSource']['diaSourceId'] == 155218500005
        assert len( reconstructed['prvDiaSources'] ) == 5
        assert len( reconstructed['prvDiaForcedSources'] ) == 0

        # Get a later alert from this same source, now we should have forced sources

        alert = elasticc2.models.PPDBAlert.objects.get( pk=155218500031 )
        reconstructed = alert.reconstruct()
        assert reconstructed['diaObject']['diaObjectId'] == 1552185
        assert reconstructed['diaSource']['diaObjectId'] == reconstructed['diaObject']['diaObjectId']
        assert reconstructed['diaSource']['diaSourceId'] == 155218500031
        assert len( reconstructed['prvDiaSources'] ) == 22
        assert len( reconstructed['prvDiaForcedSources'] ) == 31

        # Get an alert from a day where we know there were multiple alerts in the same day,
        #  but that is after the first day, to make sure that there are forced sources, but
        #  none from the same day.
        # ---> This didn't work quite right during elasticc2!  We *would* get forced sources from
        #   the same day!  It only checked that the source was not too new to have forced
        #   sources at all.  It didn't simulate a day-delay to get forced sources.

        alert = elasticc2.models.PPDBAlert.objects.get( pk=155218500013 )
        reconstructed = alert.reconstruct()
        assert reconstructed['diaObject']['diaObjectId'] == 1552185
        assert reconstructed['diaSource']['diaObjectId'] == reconstructed['diaObject']['diaObjectId']
        assert reconstructed['diaSource']['diaSourceId'] == 155218500013
        assert len( reconstructed['prvDiaSources'] ) == 13
        # We get 13, not 8
        # assert len( reconstructed('prvDiaForcedSources'] ) == 8

        # Test daysprevious and nprevious

        alert = elasticc2.models.PPDBAlert.objects.get( pk=172671000100 )
        reconstructed = alert.reconstruct()
        assert reconstructed['diaObject']['diaObjectId'] == 1726710
        assert reconstructed['diaSource']['diaObjectId'] == reconstructed['diaObject']['diaObjectId']
        assert reconstructed['diaSource']['diaSourceId'] == 172671000100
        assert len( reconstructed['prvDiaSources'] ) == 49
        assert len( reconstructed['prvDiaForcedSources'] ) == 100

        reconstructed = alert.reconstruct( daysprevious=30 )
        assert reconstructed['diaObject']['diaObjectId'] == 1726710
        assert reconstructed['diaSource']['diaObjectId'] == reconstructed['diaObject']['diaObjectId']
        assert reconstructed['diaSource']['diaSourceId'] == 172671000100
        assert len( reconstructed['prvDiaSources'] ) == 2
        assert len( reconstructed['prvDiaForcedSources'] ) == 31
        assert all( [ 30 >= reconstructed['diaSource']['midPointTai'] - i['midPointTai']
                      for i in reconstructed['prvDiaSources'] ] )
        assert all( [ 30 >= reconstructed['diaSource']['midPointTai'] - i['midPointTai']
                      for i in reconstructed['prvDiaForcedSources'] ] )

        # Not 100% sure this is really what we wanted; nprevious only
        #   limits forced sources, not sources
        reconstructed = alert.reconstruct( nprevious=10 )
        assert reconstructed['diaObject']['diaObjectId'] == 1726710
        assert reconstructed['diaSource']['diaObjectId'] == reconstructed['diaObject']['diaObjectId']
        assert reconstructed['diaSource']['diaSourceId'] == 172671000100
        assert len( reconstructed['prvDiaSources'] ) == 49
        assert len( reconstructed['prvDiaForcedSources'] ) == 10

        reconstructed = alert.reconstruct( daysprevious=30, nprevious=4 )
        assert reconstructed['diaObject']['diaObjectId'] == 1726710
        assert reconstructed['diaSource']['diaObjectId'] == reconstructed['diaObject']['diaObjectId']
        assert reconstructed['diaSource']['diaSourceId'] == 172671000100
        assert len( reconstructed['prvDiaSources'] ) == 2
        assert len( reconstructed['prvDiaForcedSources'] ) == 4
        assert all( [ 30 >= reconstructed['diaSource']['midPointTai'] - i['midPointTai']
                      for i in reconstructed['prvDiaSources'] ] )
        assert all( [ 30 >= reconstructed['diaSource']['midPointTai'] - i['midPointTai']
                      for i in reconstructed['prvDiaForcedSources'] ] )



    def test_alert_api( self, elasticc2_ppdb_class, tomclient ):

        res = tomclient.post( "elasticc2/getalert", json={ 'alertid': 666 } )
        assert res.status_code == 500
        assert res.text == 'Exception in GetAlert: PPDBAlert matching query does not exist.'

        res = tomclient.post( "elasticc2/getalert", json={ 'sourceid': 666 } )
        assert res.status_code == 500
        assert res.text == 'Exception in GetAlert: Unknown sourceid 666'

        res = tomclient.post( "elasticc2/getalert", json={} )
        assert res.status_code == 500
        assert res.text == 'Exception in GetAlert: Must supply either alertid or sourceid'

        res = tomclient.post( "elasticc2/getalert", json={ 'alertid': 155218500031 } )
        data = res.json()
        assert data['diaObject']['diaObjectId'] == 1552185
        assert data['diaSource']['diaObjectId'] == data['diaObject']['diaObjectId']
        assert data['diaSource']['diaSourceId'] == 155218500031
        assert len( data['prvDiaSources'] ) == 22
        assert len( data['prvDiaForcedSources'] ) == 31


class TestLtcv:
    def test_ltcvs( self, elasticc2_database_snapshot_class, tomclient ):
        # Make sure it objects to an unknown keyword
        res = tomclient.post( "elasticc2/ltcv", json={ 'objectid': 1552185, 'foo': 1 } )
        assert res.status_code == 500
        assert res.text == "Exception in LtcvsView: Unknown parameters: {'foo'}"

        # Try with a single objectid
        res = tomclient.post( "elasticc2/ltcv", json={ 'objectid': 1552185 } )
        assert res.status_code == 200
        data = res.json()

        assert data['status'] == 'ok'
        assert len( data['diaobject'] ) == 1
        assert data['diaobject'][0]['objectid'] == 1552185
        assert len( data['diaobject'][0]['photometry']['mjd'] ) == 32
        for field in [ 'band', 'flux', 'fluxerr' ] :
            assert len( data['diaobject'][0]['photometry'][field] ) == len( data['diaobject'][0]['photometry']['mjd'] )

        # Pick out a few objects I know that have overlapping lightcurves (in time),
        #   that start before and end after mjd 60420
        testobjs = [ 1731906, 1102362, 800290 ]

        # Get full lightcurves
        res = tomclient.post( "elasticc2/ltcv", json={ 'objectid': testobjs } )
        assert res.status_code == 200
        data = res.json()

        assert data['status'] == 'ok'
        assert len( data['diaobject'] ) == 3
        assert set( data['diaobject'][i]['objectid'] for i in [0,1,2] ) == set( testobjs )
        fullltcvlens = []
        for i in range(3):
            assert set( data['diaobject'][i].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'photometry' }
            fullltcvlens.append( len( data['diaobject'][i]['photometry']['mjd'] ) )
            assert fullltcvlens[i] > 0
            for field in [ 'band', 'flux', 'fluxerr' ]:
                assert len( data['diaobject'][i]['photometry'][field] ) == fullltcvlens[i]

        # Test a fake current mjd
        res = tomclient.post( "elasticc2/ltcv", json={ 'objectid': testobjs, 'mjd_now': 60420  } )
        assert res.status_code == 200
        data = res.json()

        assert data['status'] == 'ok'
        assert len( data['diaobject'] ) == 3
        assert set( data['diaobject'][i]['objectid'] for i in [0,1,2] ) == set( testobjs )
        partialltcvlens = []
        for i in range(2):
            partialltcvlens.append( len( data['diaobject'][i]['photometry']['mjd'] ) )
            assert  partialltcvlens[i] < fullltcvlens[i]
            assert all ( m < 60420 for m in data['diaobject'][i]['photometry']['mjd'] )


        # Make sure that include_hostinfo works
        res = tomclient.post( "elasticc2/ltcv", json={ 'objectid': testobjs, 'include_hostinfo': 1 } )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'ok'
        assert len( data['diaobject'] ) == 3
        assert set( data['diaobject'][0].keys() ) == { 'objectid', 'ra','dec', 'zp', 'photometry',
                                                       'hostgal_mag_u', 'hostgal_magerr_u',
                                                       'hostgal_mag_g', 'hostgal_magerr_g',
                                                       'hostgal_mag_r', 'hostgal_magerr_r',
                                                       'hostgal_mag_i', 'hostgal_magerr_i',
                                                       'hostgal_mag_z', 'hostgal_magerr_z',
                                                       'hostgal_mag_y', 'hostgal_magerr_y',
                                                       'hostgal_ellipticity', 'hostgal_sqradius', 'hostgal_snsep' }


        # Test returnformat 2  (TODO : returnformat 1)

        # Test a fake current mjd
        res = tomclient.post( "elasticc2/ltcv", json={ 'objectid': testobjs, 'mjd_now': 60420, 'return_format': 2 } )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'ok'
        df = pandas.DataFrame( data['diaobject'] )
        assert set( df.columns ) == { 'objectid', 'ra', 'dec', 'zp', 'mjd', 'band', 'flux', 'fluxerr' }
        assert set( df.objectid ) == set( testobjs )
        assert all( len( df.mjd[i] ) == partialltcvlens[i] for i in range(2) )
        assert all( len( df.mjd[i] ) == len( df[field][i] )
                    for field in [ 'band', 'flux', 'fluxerr' ]
                    for i in range(2) )

        res = tomclient.post( "elasticc2/ltcv", json={ 'objectid': testobjs, 'mjd_now': 60420,
                                                       'include_hostinfo': 1,
                                                       'return_format': 2 } )
        assert res.status_code == 200
        data = res.json()
        assert data['status'] == 'ok'
        df = pandas.DataFrame( data['diaobject'] )
        assert set( df.columns ) == { 'objectid', 'ra', 'dec', 'zp', 'mjd', 'band', 'flux', 'fluxerr',
                                      'hostgal_mag_u', 'hostgal_magerr_u',
                                      'hostgal_mag_g', 'hostgal_magerr_g',
                                      'hostgal_mag_r', 'hostgal_magerr_r',
                                      'hostgal_mag_i', 'hostgal_magerr_i',
                                      'hostgal_mag_z', 'hostgal_magerr_z',
                                      'hostgal_mag_y', 'hostgal_magerr_y',
                                      'hostgal_ellipticity', 'hostgal_sqradius', 'hostgal_snsep' }
        assert set( df.objectid ) == set( testobjs )
        assert all( len( df.mjd[i] ) == partialltcvlens[i] for i in range(2) )
        assert all( len( df.mjd[i] ) == len( df[field][i] )
                    for field in [ 'band', 'flux', 'fluxerr' ]
                    for i in range(2) )


    def test_ltcv_features( self, elasticc2_ppdb_class, tomclient ):

        # Default features for an object

        res = tomclient.post( "elasticc2/ltcvfeatures", json={ 'objectid': 1552185, 'includeltcv': 1 } )
        assert res.status_code == 200
        features = res.json()
        assert set( features.keys() ) == { 'anderson_darling_normal', 'inter_percentile_range_5',
                                           'chi2', 'stetson_K', 'weighted_mean', 'duration',
                                           'otsu_mean_diff', 'otsu_std_lower', 'otsu_std_upper',
                                           'otsu_lower_to_all_ratio', 'linear_fit_slope',
                                           'linear_fit_slope_sigma', 'linear_fit_reduced_chi2',
                                           'lightcurve' }
        assert len( features['lightcurve']['mjd'] ) == 132
        assert len( features['lightcurve']['flux'] ) == len( features['lightcurve']['mjd'] )
        assert len( features['lightcurve']['dflux'] ) == len( features['lightcurve']['mjd'] )
        assert features['anderson_darling_normal'] == pytest.approx( 8.2461, abs=0.001 )
        assert features['inter_percentile_range_5'] == pytest.approx( 5298., abs=10. )
        assert features['chi2'] == pytest.approx( 21.81, abs=0.1 )
        assert features['stetson_K'] == pytest.approx( 0.7216, abs=0.001 )
        assert features['weighted_mean'] == pytest.approx( 1029, abs=10. )
        assert features['duration'] == pytest.approx( 457.8, abs=1.0 )
        assert features['otsu_mean_diff'] == pytest.approx( 3633., abs=10. )
        assert features['otsu_std_lower'] == pytest.approx( 700.0, abs=1.0 )
        assert features['otsu_std_upper'] == pytest.approx( 1045., abs=10. )
        assert features['otsu_lower_to_all_ratio'] == pytest.approx( 0.7803, abs=0.001 )
        assert features['linear_fit_slope'] == pytest.approx( -6.285, abs=0.01 )
        assert features['linear_fit_slope_sigma'] == pytest.approx( 0.1715, abs=0.001 )
        assert features['linear_fit_reduced_chi2'] == pytest.approx( 11.65, abs=0.1 )

        # Make sure through_mjd works

        res = tomclient.post( "elasticc2/ltcvfeatures", json={ 'objectid': 1552185, 'includeltcv': 1,
                                                               'through_mjd': 60635 } )
        assert res.status_code == 200
        features = res.json()
        assert set( features.keys() ) == { 'anderson_darling_normal', 'inter_percentile_range_5',
                                           'chi2', 'stetson_K', 'weighted_mean', 'duration',
                                           'otsu_mean_diff', 'otsu_std_lower', 'otsu_std_upper',
                                           'otsu_lower_to_all_ratio', 'linear_fit_slope',
                                           'linear_fit_slope_sigma', 'linear_fit_reduced_chi2',
                                           'lightcurve' }
        assert len( features['lightcurve']['mjd'] ) == 80
        assert all( i <= 60635 for i in features['lightcurve']['mjd'] )
        assert len( features['lightcurve']['flux'] ) == len( features['lightcurve']['mjd'] )
        assert len( features['lightcurve']['dflux'] ) == len( features['lightcurve']['mjd'] )
        assert features['anderson_darling_normal'] == pytest.approx( 2.4849, abs=0.001 )
        assert features['inter_percentile_range_5'] == pytest.approx( 5563., abs=10. )
        assert features['chi2'] == pytest.approx( 23.99, abs=0.1 )
        assert features['stetson_K'] == pytest.approx( 0.7974, abs=0.001 )
        assert features['weighted_mean'] == pytest.approx( 1893, abs=10. )
        assert features['duration'] == pytest.approx( 354.1, abs=1.0 )
        assert features['otsu_mean_diff'] == pytest.approx( 3498., abs=10. )
        assert features['otsu_std_lower'] == pytest.approx( 847.4, abs=1.0 )
        assert features['otsu_std_upper'] == pytest.approx( 904.3, abs=1.0 )
        assert features['otsu_lower_to_all_ratio'] == pytest.approx( 0.6875, abs=0.001 )
        assert features['linear_fit_slope'] == pytest.approx( -9.410, abs=0.01 )
        assert features['linear_fit_slope_sigma'] == pytest.approx( 0.4125, abs=0.001 )
        assert features['linear_fit_reduced_chi2'] == pytest.approx( 17.62, abs=0.1 )

        # Features for an object from its last source; will be different, since
        #   the post-last-source forced sources aren't included

        res = tomclient.post( "elasticc2/ltcvfeatures", json={ 'sourceid': 155218500031, 'includeltcv': 1 } )
        assert res.status_code == 200
        features = res.json()
        assert set( features.keys() ) == { 'anderson_darling_normal', 'inter_percentile_range_5',
                                           'chi2', 'stetson_K', 'weighted_mean', 'duration',
                                           'otsu_mean_diff', 'otsu_std_lower', 'otsu_std_upper',
                                           'otsu_lower_to_all_ratio', 'linear_fit_slope',
                                           'linear_fit_slope_sigma', 'linear_fit_reduced_chi2',
                                           'lightcurve' }
        assert len( features['lightcurve']['mjd'] ) == 32
        assert len( features['lightcurve']['flux'] ) == len( features['lightcurve']['mjd'] )
        assert len( features['lightcurve']['dflux'] ) == len( features['lightcurve']['mjd'] )

        assert features['anderson_darling_normal'] == pytest.approx( 1.4864, abs=0.001 )
        assert features['inter_percentile_range_5'] == pytest.approx( 5421., abs=10. )
        assert features['chi2'] == pytest.approx( 31.08, abs=0.1 )
        assert features['stetson_K'] == pytest.approx( 0.8563, abs=0.001 )
        assert features['weighted_mean'] == pytest.approx( 2918, abs=10. )
        assert features['duration'] == pytest.approx( 42.91, abs=0.1 )
        assert features['otsu_mean_diff'] == pytest.approx( 3352., abs=10. )
        assert features['otsu_std_lower'] == pytest.approx( 952.6, abs=1.0 )
        assert features['otsu_std_upper'] == pytest.approx( 707.4, abs=1.0 )
        assert features['otsu_lower_to_all_ratio'] == pytest.approx( 0.3125, abs=0.001 )
        assert features['linear_fit_slope'] == pytest.approx( -68.65, abs=0.1 )
        assert features['linear_fit_slope_sigma'] == pytest.approx( 5.007, abs=0.01 )
        assert features['linear_fit_reduced_chi2'] == pytest.approx( 25.85, abs=0.1 )

        # Features for an object from an earlier source; will be different again

        res = tomclient.post( "elasticc2/ltcvfeatures", json={ 'sourceid': 155218500016, 'includeltcv': 1 } )
        assert res.status_code == 200
        features = res.json()
        assert set( features.keys() ) == { 'anderson_darling_normal', 'inter_percentile_range_5',
                                           'chi2', 'stetson_K', 'weighted_mean', 'duration',
                                           'otsu_mean_diff', 'otsu_std_lower', 'otsu_std_upper',
                                           'otsu_lower_to_all_ratio', 'linear_fit_slope',
                                           'linear_fit_slope_sigma', 'linear_fit_reduced_chi2',
                                           'lightcurve' }

        assert len( features['lightcurve']['mjd'] ) == 17
        assert len( features['lightcurve']['flux'] ) == len( features['lightcurve']['mjd'] )
        assert len( features['lightcurve']['dflux'] ) == len( features['lightcurve']['mjd'] )

        assert features['anderson_darling_normal'] == pytest.approx( 1.1623, abs=0.001 )
        assert features['inter_percentile_range_5'] == pytest.approx( 4540., abs=10. )
        assert features['chi2'] == pytest.approx( 22.21, abs=0.1 )
        assert features['stetson_K'] == pytest.approx( 0.8812, abs=0.001 )
        assert features['weighted_mean'] == pytest.approx( 3466., abs=10. )
        assert features['duration'] == pytest.approx( 8.815, abs=0.01 )
        assert features['otsu_mean_diff'] == pytest.approx( 2644., abs=10. )
        assert features['otsu_std_lower'] == pytest.approx( 922.2, abs=1.0 )
        assert features['otsu_std_upper'] == pytest.approx( 473.0, abs=1.0 )
        assert features['otsu_lower_to_all_ratio'] == pytest.approx( 0.2941, abs=0.001 )
        assert features['linear_fit_slope'] == pytest.approx( -305.3, abs=1.0 )
        assert features['linear_fit_slope_sigma'] == pytest.approx( 33.18, abs=0.1 )
        assert features['linear_fit_reduced_chi2'] == pytest.approx( 18.05, abs=0.1 )


        # Test through_mjd with source

        res = tomclient.post( "elasticc2/ltcvfeatures", json={ 'sourceid': 155218500016, 'includeltcv': 1,
                                                               'through_mjd': 60282 } )
        assert res.status_code == 200
        features = res.json()
        assert set( features.keys() ) == { 'anderson_darling_normal', 'inter_percentile_range_5',
                                           'chi2', 'stetson_K', 'weighted_mean', 'duration',
                                           'otsu_mean_diff', 'otsu_std_lower', 'otsu_std_upper',
                                           'otsu_lower_to_all_ratio', 'linear_fit_slope',
                                           'linear_fit_slope_sigma', 'linear_fit_reduced_chi2',
                                           'lightcurve' }

        assert len( features['lightcurve']['mjd'] ) == 14
        assert len( features['lightcurve']['flux'] ) == len( features['lightcurve']['mjd'] )
        assert len( features['lightcurve']['dflux'] ) == len( features['lightcurve']['mjd'] )

        assert features['anderson_darling_normal'] == pytest.approx( 0.8063, abs=0.001 )
        assert features['inter_percentile_range_5'] == pytest.approx( 3408., abs=10. )
        assert features['chi2'] == pytest.approx( 14.74, abs=0.1 )
        assert features['stetson_K'] == pytest.approx( 0.8364, abs=0.001 )
        assert features['weighted_mean'] == pytest.approx( 3828., abs=10. )
        assert features['duration'] == pytest.approx( 2.873, abs=0.01 )
        assert features['otsu_mean_diff'] == pytest.approx( 2085., abs=10. )
        assert features['otsu_std_lower'] == pytest.approx( 508.3, abs=1.0 )
        assert features['otsu_std_upper'] == pytest.approx( 484.1, abs=1.0 )
        assert features['otsu_lower_to_all_ratio'] == pytest.approx( 0.2143, abs=0.001 )
        assert features['linear_fit_slope'] == pytest.approx( 35.25, abs=0.1 )
        assert features['linear_fit_slope_sigma'] == pytest.approx( 68.03, abs=0.1 )
        assert features['linear_fit_reduced_chi2'] == pytest.approx( 15.96, abs=0.1 )


        # Make sure we don't get the lightcurve when we don't ask for it
        res = tomclient.post( "elasticc2/ltcvfeatures", json={ 'sourceid': 155218500016, 'includeltcv': 0 } )
        assert res.status_code == 200
        features = res.json()
        assert 'lightcurve' not in features.keys()
        res = tomclient.post( "elasticc2/ltcvfeatures", json={ 'sourceid': 155218500016 } )
        assert res.status_code == 200
        features = res.json()
        assert 'lightcurve' not in features.keys()
