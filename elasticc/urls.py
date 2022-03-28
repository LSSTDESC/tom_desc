from django.conf.urls import include, url
from rest_framework.routers import DefaultRouter
from elasticc import views

app_name = 'elasticc'

router = DefaultRouter()
router.register(f'diaobject', views.DiaObjectViewSet)
router.register(f'diasource', views.DiaSourceViewSet)
router.register(f'diatruth', views.DiaTruthViewSet)
router.register(f'diaalert', views.DiaAlertViewSet)

urlpatterns = [
    url('adddiaobject', views.MaybeAddDiaObject.as_view()),
    url('addelasticcalert', views.MaybeAddAlert.as_view()),
    url('addtruth', views.MaybeAddTruth.as_view()),
    url('brokermessage', views.BrokerMessageView.as_view(), name='brokermessage'),
    url('', include((router.urls, 'elasticc'), namespace=app_name)),
]
