from django.shortcuts import render
from django.template.loader import render_to_string
from django.shortcuts import redirect
import datetime
import psycopg2
import os

from .models import Snapshots,SnapshotTags
from .models import ProcessingVersions, DBViews
from .forms import SnapshotForm,ProcessingVersionsForm
from .forms import EditProcessingVersionsForm
from .forms import SnapshotTagsForm
from .forms import CreateViewForm
from django.conf import settings

from django.db import connection
from django.db.migrations.recorder import MigrationRecorder


def index(request):
    snapshots = Snapshots.objects.all().order_by("insert_time")
    processing_versions = ProcessingVersions.objects.all().order_by("validity_start")
    snapshot_tags = SnapshotTags.objects.all().order_by("insert_time")
    db_views = DBViews.objects.all().order_by("insert_time")
    
    context = {"snapshots": snapshots, "processing_versions": processing_versions, "snapshot_tags": snapshot_tags, "db_views":db_views}
    print(context)
    return render(request, "snapshots_index.html", context)

def create_new_snapshot(request):
    
    if request.method == "POST":
        # create a form instance and populate it with data from the request:
        form = SnapshotForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            # process the data in form.cleaned_data as required
            name = form.cleaned_data["snapshot_name"]
            s = Snapshots(name=name, insert_time=datetime.datetime.now(tz=datetime.timezone.utc))
            s.save()
            
            # redirect to a new URL:
            return redirect("./index")
 
        # if a GET (or any other method) we'll create a blank form
    else:

        form = SnapshotForm()

    return render(request, "create_new_snapshot.html", {"form": form})

def create_new_processing_version(request):
    
    if request.method == "POST":
        # create a form instance and populate it with data from the request:
        form = ProcessingVersionsForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            # process the data in form.cleaned_data as required
            version = form.cleaned_data["version"]
            validity_start = form.cleaned_data["validity_start"]
            pv = ProcessingVersions(version=version, validity_start=validity_start)
            pv.save()

            # Create migration to create new partitions for dia_source, dia_forced_source, ds_to_pv_to_ss,
            # dfs_to_pv_to_ss

            # Get the most recent migration from the database

            lm = MigrationRecorder.Migration.objects.filter(app='alerts').last()
            nm = lm.name
            parts = nm.split('_')
            mig_num = str(int(parts[0])+1)
            mig_num = mig_num.rjust(4,'0')

            # Construct migration file name
            
            mig_name = "%s_partitioning_%s.py" % (mig_num,version)
            base_dir = settings.BASE_DIR
            mig_file = "%s/alerts/migrations/%s" % (base_dir,mig_name)

            # Fill template
            
            mig_template =  "%s/alerts/templates/migration_template.txt" % (base_dir)
            mig_dict = {}
            mig_dict['depend'] = nm
            mig_dict['name'] = "pv_%s" % version
            mig_dict['version'] = version
            rendered = render_to_string(mig_template, mig_dict)

            # Save output as .py file in migrations directory
            
            mig_file_out = open(mig_file, 'w+')
            mig_file_out.write(rendered)
            mig_file_out.close()

            
            # redirect to a new URL:
            return redirect("./index")
 
        # if a GET (or any other method) we'll create a blank form
    else:

        form = ProcessingVersionsForm()

    return render(request, "create_new_processing_version.html", {"form": form})

def edit_processing_version(request):
    
    # create a form instance and populate it with data from the request:
    form = EditProcessingVersionsForm(request.POST)
    # check whether it's valid:
    if form.is_valid():
        # process the data in form.cleaned_data as required
        version = form.cleaned_data["version"]
        validity_start = form.cleaned_data["validity_start"]
        validity_end = form.cleaned_data["validity_end"]
        pv = ProcessingVersions.objects.get(version=version)
        pv.validity_end = validity_end
        pv.save()
        
        # redirect to a new URL:
        return render(request, "alerts/snapshots_index.html")

    else:

        version = request.GET.get("version")
        pv = ProcessingVersions.objects.get(version=version)
        form = EditProcessingVersionsForm(initial={"version":version, "validity_start":pv.validity_start})


    return render(request, "edit_processing_version.html", {"form": form})

def create_new_snapshot_tag(request):
    
    if request.method == "POST":
        # create a form instance and populate it with data from the request:
        form = SnapshotTagsForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            # process the data in form.cleaned_data as required
            name = form.cleaned_data["name"]
            snapshot_name = form.cleaned_data["snapshot_name"]
            s = Snapshots.objects.get(name=snapshot_name)
            st = SnapshotTags(name=name, insert_time=datetime.datetime.now(tz=datetime.timezone.utc))
            st.snapshot_name = s
            st.save()
            
            # redirect to a new URL:
            return redirect("./index")
 
        # if a GET (or any other method) we'll create a blank form
    else:

        form = SnapshotTagsForm()

    return render(request, "create_new_snapshot_tag.html", {"form": form})

def create_new_view(request):

    if request.method == "POST":
        # create a form instance and populate it with data from the request:
        form = CreateViewForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            # process the data in form.cleaned_data as required
            view_name = form.cleaned_data["view_name"]
            tag_name = form.cleaned_data["tag_name"]
            query = "create view %s as select dia_source.* from ds_to_pv_to_ss,dia_source,snapshot_tags where ds_to_pv_to_ss.snapshot_name = (select snapshot_tags.snapshot_name from snapshot_tags where snapshot_tags.name = '%s') and dia_source.dia_source=ds_to_pv_to_ss.dia_source and ds_to_pv_to_ss.valid_flag = 1 and ds_to_pv_to_ss.valid_flag=dia_source.valid_flag" % (view_name,tag_name)

            print(query)

            cursor = connection.cursor()
            cursor.execute(query)
            
            db_v = DBViews(view_name=view_name, view_sql=query, insert_time=datetime.datetime.now(tz=datetime.timezone.utc))
            db_v.save()
            
            # redirect to a new URL:
            return redirect("./index")
 
        # if a GET (or any other method) we'll create a blank form
    else:
        
        form = CreateViewForm()

    return render(request, "create_new_view.html", {"form": form})
