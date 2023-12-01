from django.urls import path, include
from rest_framework.routers import DefaultRouter
from elasticc2 import views

app_name = 'elasticc2'

router = DefaultRouter()
router.register( f'ppdbdiaobject', views.PPDBDiaObjectViewSet )
router.register( f'ppdbdiasource', views.PPDBDiaSourceViewSet )
router.register( f'ppdbdiaforcedsource', views.PPDBDiaForcedSourceViewSet )
router.register( f'ppdbdiaobjectwithsources', views.PPDBDiaObjectSourcesViewSet )
router.register( f'ppdbdiaobjectofsource', views.PPDBDiaObjectAndPrevSourcesForSourceViewSet )

urlpatterns = [
    path( '', views.Elasticc2MainView.as_view(), name='elasticc2' ),

    path( 'adminsummary', views.Elasticc2AdminSummary.as_view() ),
    path( 'alertstreamhists', views.Elasticc2AlertStreamHistograms.as_view() ),
    path( 'classifiers', views.Elasticc2KnownClassifiers.as_view() ),
    path( 'brokertimedelays', views.Elasticc2BrokerTimeDelayGraphs.as_view() ),
    path( 'brokercompleteness', views.Elasticc2BrokerCompletenessGraphs.as_view() ),

    path( 'confmatrixlatest', views.Elasticc2ConfMatrixLatest.as_view() ),

    path( 'brokerclassfortruetype/<int:classifierid>/<int:gentype>/',
          views.Elasticc2BrokerClassificationForTrueType.as_view(),
          name='brokerclassfortruetype' ),
    
    path('brokermessage/<int:info>/', views.BrokerMessageView.as_view(), name='brokermesssage-int'),
    path('brokermessage/<path:info>/', views.BrokerMessageView.as_view(), name='brokermessage-path'),
    path('brokermessage/', views.BrokerMessageView.as_view(), name='brokermessage-noparam'),

    path( '', include( ( router.urls, 'elasticc2' ), namespace=app_name ) )
]
