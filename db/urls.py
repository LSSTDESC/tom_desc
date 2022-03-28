from django.conf.urls import url
from db import views

urlpatterns = [
    url('runsqlquery', views.RunSQLQuery.as_view()),
]
