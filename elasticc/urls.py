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

    path('brokermessage/<int:msgid>/', views.BrokerMessageGet.as_view(), name='brokermesssage-get'),
    path('brokermessage/', views.BrokerMessagePut.as_view(), name='brokermessage-put'),

    path('', include((router.urls, 'elasticc'), namespace=app_name)),
]
