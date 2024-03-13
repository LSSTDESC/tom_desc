from django.urls import path
from db import views

urlpatterns = [
    path('runsqlquery/', views.RunSQLQuery.as_view()),
    path('submitsqlquery/', views.SubmitLongSQLQuery.as_view()),
    path('checksqlquery/<str:queryid>/', views.CheckLongSQLQuery.as_view()),
]
