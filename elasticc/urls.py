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
    path('adddiaobject', views.MaybeAddDiaObject.as_view()),
    path('addelasticcalert', views.MaybeAddAlert.as_view()),
    path('addtruth', views.MaybeAddTruth.as_view()),
    path('addobjecttruth', views.MaybeAddObjectTruth.as_view()),

    path('brokermessage/<int:info>/', views.BrokerMessageView.as_view(), name='brokermesssage-int'),
    path('brokermessage/<path:info>/', views.BrokerMessageView.as_view(), name='brokermessage-path'),
    path('brokermessage/', views.BrokerMessageView.as_view(), name='brokermessage-noparam'),

    path('testing/<int:info>', views.Testing.as_view(), name='testing' ),
    path('testing/<path:info>', views.Testing.as_view(), name='testing' ),
    path('testing/', views.Testing.as_view(), name='testing' ),
    
    path('', include((router.urls, 'elasticc'), namespace=app_name)),
]
