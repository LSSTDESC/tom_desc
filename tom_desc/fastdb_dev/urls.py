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
    path("acquire_token",DataTools.acquire_token, name="acquire_token"),
    path('api-token-auth/', views.obtain_auth_token),
    path('submit_long_query',DataTools.SubmitLongQuery, name="SubmitLongQuery"),
    path('submit_short_query',DataTools.SubmitShortQuery, name="SubmitShortQuery"),
    path('check_long_sql_query',DataTools.CheckLongSQLQuery, name="CheckLongSQLQuery"),
     path('get_long_sql_query',DataTools.GetLongSQLQuery, name="GetLongSQLQuery"),
    path('store_dia_source_data',DataTools.store_dia_source_data, name="store_dia_source_data"),
    path('store_ds_pv_ss_data',DataTools.store_ds_pv_ss_data, name="store_ds_pv_ss_data"),
    path('update_ds_pv_ss_valid_flag',DataTools.update_ds_pv_ss_valid_flag, name="update_ds_pv_ss_valid_flag"),
    path('update_dia_source_valid_flag',DataTools.update_dia_source_valid_flag, name="update_dia_source_valid_flag"),
    path('bulk_create_dia_source_data',DataTools.bulk_create_dia_source_data, name="bulk_create_dia_source_data"),
    path('bulk_create_ds_pv_ss_data',DataTools.bulk_create_ds_pv_ss_data, name="bulk_create_ds_pv_ss_data"),
    path('bulk_update_ds_pv_ss_valid_flag',DataTools.bulk_update_ds_pv_ss_valid_flag, name="bulk_update_ds_pv_ss_valid_flag"),
    path('bulk_update_dia_source_valid_flag',DataTools.bulk_update_dia_source_valid_flag, name="bulk_update_dia_source_valid_flag"),
    path('create_new_view', SupportTools.create_new_view, name="create_new_view"),
    path('add_broker', SupportTools.add_broker, name="add_broker"),
]

