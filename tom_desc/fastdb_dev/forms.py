from django import forms
from .models import Snapshots


class SnapshotForm(forms.Form):
    snapshot_name = forms.CharField(label="Snapshot name", max_length=20)

class ProcessingVersionsForm(forms.Form):
    version = forms.CharField(label="Processing Version", max_length=20)
    validity_start = forms.DateTimeField(label="Valid Start Date")

class EditProcessingVersionsForm(forms.Form):
    version = forms.CharField(label="Processing Version", max_length=20)
    validity_start = forms.DateTimeField(label="Valid Start Date")
    validity_end = forms.DateTimeField(label="Valid End Date")

class SnapshotTagsForm(forms.Form):
    name =  forms.CharField(label="Tag name", max_length=20)
    snapshot_name = forms.ModelChoiceField(label="Available Snapshots", queryset=Snapshots.objects.filter().only('name'))

class CreateViewForm(forms.Form):
    view_name = forms.CharField(label="View name", max_length=50)
    tag_name = forms.CharField(label="Tag name", max_length=50)
    
