from django.urls import path, include
from rest_framework.routers import DefaultRouter
from elasticc import views

app_name = 'elasticc'

router = DefaultRouter()
router.register(f'diaobject', views.DiaObjectViewSet)
router.register(f'diasource', views.DiaSourceViewSet)
router.register(f'diatruth', views.DiaTruthViewSet)
router.register(f'diaalert', views.DiaAlertViewSet)

urlpatterns = [
    path('', views.ElasticcMainView.as_view(), name="elasticc"),

    path('adddiaobject', views.MaybeAddDiaObject.as_view()),
    path('addelasticcalert', views.MaybeAddAlert.as_view()),
    path('addtruth', views.MaybeAddTruth.as_view()),
    path('addobjecttruth', views.MaybeAddObjectTruth.as_view()),
    path('markalertsent', views.MarkAlertSent.as_view()),
    
    path('adminsummary', views.ElasticcAdminSummary.as_view()),
    path('summary', views.ElasticcSummary.as_view()),
    path('latestconfmatrix', views.ElasticcLatestConfMatrix.as_view()),
    path('alertstreamhists', views.ElasticcAlertStreamHistograms.as_view()),
    path('brokerstreamgraphs', views.ElasticcBrokerStreamGraphs.as_view()),
    path('brokercompletenessgraphs', views.ElasticcBrokerCompletenessGraphs.as_view()),
    path('brokertimedelays', views.ElasticcBrokerTimeDelayGraphs.as_view()),
    path('tmpbrokeralerthists', views.ElasticcTmpBrokerHistograms.as_view()),
    
    path('getalerts', views.GetAlerts.as_view()),
    path('getalertsandtruth', views.GetAlertsAndTruth.as_view()),
    
    path('brokermessage/<int:info>/', views.BrokerMessageView.as_view(), name='brokermesssage-int'),
    path('brokermessage/<path:info>/', views.BrokerMessageView.as_view(), name='brokermessage-path'),
    path('brokermessage/', views.BrokerMessageView.as_view(), name='brokermessage-noparam'),

    path('testing/<int:info>', views.Testing.as_view(), name='testing' ),
    path('testing/<path:info>', views.Testing.as_view(), name='testing' ),
    path('testing/', views.Testing.as_view(), name='testing' ),
    
    path('', include((router.urls, 'elasticc'), namespace=app_name)),
]
