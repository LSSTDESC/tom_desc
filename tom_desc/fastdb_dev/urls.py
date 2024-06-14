from django.urls import path
from rest_framework.authtoken import views

from . import SupportTools
from . import DataTools


urlpatterns = [
    path("support_tools_index", SupportTools.index, name="support_tools_index"),
    path("create_new_snapshot", SupportTools.create_new_snapshot, name="create_new_snapshot"),
    path("create_new_processing_version", SupportTools.create_new_processing_version, name="create_new_processing_version"),
    path("edit_processing_version", SupportTools.edit_processing_version, name="edit_processing_version"),
    path("index", SupportTools.index, name="index"),
    path("create_new_snapshot_tag", SupportTools.create_new_snapshot_tag, name="create_new_snapshot_tag"),
    path("get_dia_sources", DataTools.get_dia_sources, name="get_dia_sources"),
    path("get_dia_objects", DataTools.get_dia_objects, name="get_dia_objects"),
    path("acquire_token",DataTools.acquire_token, name="acquire_token"),
    path('api-token-auth/', views.obtain_auth_token),
    path('raw_query_long',DataTools.raw_query_long, name="raw_query_long"),
    path('raw_query_short',DataTools.raw_query_short, name="raw_query_short"),
    path('store_dia_source_data',DataTools.store_dia_source_data, name="store_dia_source_data"),
    path('store_ds_pv_ss_data',DataTools.store_ds_pv_ss_data, name="store_ds_pv_ss_data"),
    path('update_ds_pv_ss_valid_flag',DataTools.update_ds_pv_ss_valid_flag, name="update_ds_pv_ss_valid_flag"),
    path('update_dia_source_valid_flag',DataTools.update_dia_source_valid_flag, name="update_dia_source_valid_flag"),
    path('create_new_view', SupportTools.create_new_view, name="create_new_view"),
    path('add_broker', SupportTools.add_broker, name="add_broker"),
]

