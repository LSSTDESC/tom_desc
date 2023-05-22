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
    path( '', include( ( router.urls, 'elasticc' ), namespace=app_name ) )
]
