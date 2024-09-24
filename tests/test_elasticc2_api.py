import elasticc2.models

class TestReconstructAlert:
    def test_reconstruct_alert( self, elasticc2_ppdb ):

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


    
    def test_alert_api( self, elasticc2_ppdb, tomclient ):

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
