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
    path('create_dia_source',DataTools.CreateDiaSource.as_view(), name="create_dia_source"),
    path('create_ds_pv_ss',DataTools.CreateDSPVSS.as_view(), name="create_ds_pv_ss"),
    path('update_ds_pv_ss_valid_flag',DataTools.UpdateDSPVSSValidFlag.as_view(), name="update_ds_pv_ss_valid_flag"),
    path('update_dia_source_valid_flag',DataTools.UpdateDiaSourceValidFlag.as_view(), name="update_dia_source_valid_flag"),
    path('create_new_view', SupportTools.create_new_view, name="create_new_view"),
    path('add_broker', SupportTools.add_broker, name="add_broker"),
]

