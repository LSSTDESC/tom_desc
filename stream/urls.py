from django.conf.urls import include, url
from rest_framework.routers import DefaultRouter
from stream import views

app_name = 'stream'

router = DefaultRouter()
router.register(r'targets', views.TargetViewSet)
router.register(r'alerts', views.AlertViewSet)
router.register(r'topics', views.TopicViewSet)
router.register(r'events', views.EventViewSet)

urlpatterns = [
    url('dumprknoptest', views.DumpRknopTest.as_view()),
    url('addelasticcdiaobject', views.MaybeAddElasticcDiaObject.as_view()),
    url('', include((router.urls, 'stream'), namespace='stream')),
]
