import elasticc2.models

def test_elasticc2_database_snapshot( elasticc2_database_snapshot ):
    import pdb; pdb.set_trace()
    assert elasticc2.models.BrokerClassifier.objects.count() == 3
    assert elasticc2.models.BrokerMessage.objects.count() == 1950
    assert elasticc2.models.DiaForcedSource.objects.count() == 5765
    assert elasticc2.models.DiaObject.objects.count() == 131
    assert elasticc2.models.DiaObjectTruth.objects.count() == 346
    assert elasticc2.models.DiaSource.objects.count() == 650
    assert elasticc2.models.PPDBAlert.objects.count() == 1862
    assert elasticc2.models.PPDBDiaForcedSource.objects.count() == 52172
    assert elasticc2.models.PPDBDiaObject.objects.count() == 346
    assert elasticc2.models.PPDBDiaSource.objects.count() == 1862
    assert elasticc2.models.DiaObjectInfo.objects.count() == 268
    assert elasticc2.models.BrokerSourceIds.objects.count() == 650

