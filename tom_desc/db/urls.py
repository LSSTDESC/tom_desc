from django.urls import path
from db import views

urlpatterns = [
    path('runsqlquery/', views.RunSQLQuery.as_view()),
]
