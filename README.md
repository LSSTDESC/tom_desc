# DESC TOM

Based on the [Tom Toolkit](https://lco.global/tomtoolkit/)

# Using the TOM

The "production" server at nersc is `https://desc-tom.lbl.gov` ; ask Rob (raknop@lbl.gov or Rob Knop on the LSST Slack) if you need a user account.

At the moment, the TOM is only holding the alerts and broker messages for the ELAsTiCC and ELAsTiCC2 campaigns.  It will evolve from here (using at least ELAsTiCC as a test data set) into something that will prioritize and schedule follow-up observations.

Being a Django server, the TOM is divided into different "applications".  The most relevant ones right now are:

* [ELAsTiCC](#elasticc)
* [ELAsTiCC2](#elasticc2) (Under construction!)

## Accessing the TOM

### Interactively with a browser

Just go.  Look at (for example) the ELAsTiCC, ELAsTiCC2, and Targets links in the header navigation bar.

### Via SQL

TODO: make sure this is up to date

You can access the database directly by passing SQL to a thin http interface; see `sql_query_tom_db.py` or (similar code as a Jupyter notebook) `sql_query_tom_db.ipynb`; see [Database Schema and Notes](#database-schema-and-notes) for more information.  Both of these (IF THEY'RE UP TO DATE) use `tomclient.py`, which defines a class `TomClient` that is really just a very simple front-end to python `requests` that handles some annoying django login details..  The docstring for the class in that file describes how to use it.

# <a name="elasticc"></a>The ELAsTiCC application

You can get to its web interface by clicking on the "ELAsTiCC" link in the navbar at the top of the TOM web page, or by going straight to [https://desc-tom.lbl.gov/elasticc]

## Example REST interface usage

To use these, you need to have an account on the TOM, and you need to log into it with the client you're using.
Here's an example (ROB UPDATE THIS):

```
import requests
import json

# Configure

url = 'https://desc-tom.lbl.gov'
username = '<your TOM username>'
password = '<your TOM password>'

# Log in.
# Note that Django is *very* particular about trailing slashes.

rqs = requests.session()
rqs.get( f'{url}/accounts/login/' )
rqs.post( f'{url}/accounts/login/',
          data={ 'username': username,
                 'password': password,
                 'csrfmiddlewaretoken': rqs.cookies['csrftoken'] } )
csrfheader = { 'X-CSRFTokien': rqs.cookies['csrftoken'] }

# Pull down information about objects

response = rqs.get( f'{url}/elasticc/diaobject/' )
data = json.loads( response.text )
print( data.keys() )
print( data['count'] )
print( len( data['results'] ) )
print( json.dumps( data['results'][0], indent=4 ) )

# Pull down information about one object (pick #5 from the list for no
# good reason).  This is gratuitous, because all that information was
# already in the list we just downloaded, but really this is just here
# to demonstrate the API

objid = data['results'][5]['diaObjectId']
response = rqs.get( f'{url}/elasticc/diaobject/{objid}/' )
data = json.loads( response.text )
print( data.keys() )
print( f'{data["ra"]}  {data["decl"]}' )
```

Currently defined are:
* `https://desc-tom.lbl.gov/elasticc/diaobject/`
* `https://desc-tom.lbl.gov/elasticc/diasource/`
* `https://desc-tom.lbl.gov/elasticc/diatruth/`
* `https://desc-tom.lbl.gov/elasticc/diaalert/`
* (and some other things I should still document)

Note that as the elasticc database has grown, many of these endpoints will time out before they're able to pull and
send all of their informaiton.

Called by themselves, they return a JSON dict as in the example above, with `count` giving the total number of objects (or sources or truth table entries), and `results` having an array of dictionaries for the first hundred objects.  `next` and `previous` have URLs for getting the next or previous 100 from the list.  (This will be fraught if records are actively being addded to the database at the same time as when you're running your queries.)  As in the example above, you can append a single number (with a trailing slash after it) to pull down the information for that one object or source; that number is (respectively) diaObjectId or diaSourceId.  (For the Truth table, pass the relevant diaSourceId.)

## Database Schema and Notes

The schema for the elasticc tables in the database can be found in the comments at the end of `sql_query_tom_db.py`.  Everybody can read the `elasticc_broker*` tables; only people in the `elasticc_admin` group can read the other `elasticc_*` tables.

### classId and gentype

Broker Messages include a classification in the field `classId`; these ids use the [ELAsTiCC taxonomy](https://github.com/LSSTDESC/elasticc/blob/main/taxonomy/taxonomy.ipynb).  This is a hierarchical classification.  Classifications in the range 0-9 correspond to a broad class.  Classifications in the range 10-99 correspond to a more specific category.  Classifications in the range 100-999 correspond to an "exact" classification (or, as exact as the taxonomy gets).  The first digit of a classification tells you its broad class, the second digit tells you the specific category within the broad class, and the third digit tells you the exact classification within the specific category.  (Go look at the taxonomy; there's a map there that will make it clearer than this description.)

The true model type used to create the original alerts are in the field `gentype` in the table `elasticc_diaobjecttruth`; this field is an internal SNANA index.  The mapping between `classId` and `gentype` is complicated for a few reasons.  First, SNANA (of course) uses individual models to generate lightcurves, so there will be no `gentype`s corresponding to the broad class or specific categories of the taxonomy.  Second, in some cases (e.g. core-collapse supernovae), there are multipel different SNANA models (and thus multiple different `gentype` values) that correspond to the same `classId`.

There are two database tables to help matching broker classifications to truth, but additional logic beyond just looking up lines in this table will be needed for the reasons desribed above.

`elasticc_gentypeofclassid` gives a mapping of `classId` to all associated `gentype` values.  This table has one entry for each gentype, but it also has a number of entries where `gentype` is null.  These latter entries are the cases where there is no `gentype` (i.e. SNANA model) that corresponds to a given `classId` (e.g. in the case of categories).  There are multiple entries for several `classId` values (e.g. `classId` 113, for a SNII, has six different SNANA models, and thus six different `gentype` values, associated with it).  This is the table you would want to join to the truth table in order to figure out which `classId` a broker _should_ have given to an event if it classified it exactly right.

`elasticc_classidofgentype` is useful if you want to figure out if the broker got the general category right.  There are multiple entries for each `classId`, and multiple entries for each `gentype`.  None of the entries in this table have a null value for either `classId` or `gentype`.  If the `classId` is a three-digit identification (i.e. an exact time), then the fields `categorymatch` and `exactmatch` will both be `true`, and the information is redundant with whats in the `elasticc_gentypeofclassid` table.  If the `classId` is a two-digit identification, then the `exactmatch` will be `false` and the `categorymatch` will be true.  There will be an entry for _every_ `gentype` that corresponds to something in this category.  (So, for `classId` 11, "SN-like", there will be entries in this table for the `gentype`s of all SNANA supernova models of all types.)  If the `classId` is a one-digit identification, i.e. a broad class, then both `categorymatch` and `exactmatch` will be false, and there be a large number of lines in this table, one for each SNANA model that corresponds to anything in the broad class.


---

# <a name="elasticc2"></a>The ELAsTiCC2 application

TODO

---

# Internal Documentation

The rest of this is only interesting if you want to develop it or deploy a private version for hacking.

## Branch Management

The branch `main` has the current production code.

Make a branch `/u/{yourname}/{name}` to do dev work, which (if appropriate) may later be merged into `main`.


## Deployment a dev environment with Docker

If you want to test the TOM out, you can deploy it on your local machine.  If you're lucky, all you need to do is:

<!--
<ul>
<li> Run <code>git submodule update --init --recursive</code>.  There are a number of git submodules that have the standard TOM code.  By default, when you clone, git doesn't clone submodules, so do this in order to make sure all that stuff is there.  (Alternative, if instead of just <code>git clone...</code> you did <code>git clone --recurse-submodules ...</code>, then you've already taken care of this step.)  If you do a <code>git pull</code> later, you either need to do <code>git pull --recurse-submodules</code>, or do <code>git submodule --update --recursive</code> after your pull.</li>
<li> Run <code>docker-compose up -d tom</code>.  This will use the <code>docker-compose.yml</code> file to either build or pull two images (the web server and the postgres server), and run two containers.  It will also create a docker volume named "tomdbdata" where postgres will store its contents, so that you can persist the database from one run of the container to the next.</li>
<li>The first time you run it for a given postgres volume, once the containers are up you need to run a shell on the server container with <code>docker compose exec -it tom /bin/bash</code>, and then run the commands:
<ul>
    <li><code>python manage.py migrate</code></li>
    <li><code>python manage.py createsuperuser</code> (and answer the prompts)</li>
</ul></li>
</ul>
-->

* Run <code>git submodule update --init --recursive</code>.  There are a number of git submodules that have the standard TOM code.  By default, when you clone, git doesn't clone submodules, so do this in order to make sure all that stuff is there.  (Alternative, if instead of just <code>git clone...</code> you did <code>git clone --recurse-submodules ...</code>, then you've already taken care of this step.)  If you do a <code>git pull</code> later, you either need to do <code>git pull --recurse-submodules</code>, or do <code>git submodule --update --recursive</code> after your pull.</li>

* Run <code>docker-compose up</code>.  This will use the <code>docker-compose.yml</code> file to either build or pull three images (the web server, the postgres server, and the cassandra server), and create three containers.  It will also create a docker volume named "tomdbdata" and "tomcassandradata" where postgres and cassandra respectively will store their contents, so that you can persist the databases from one run of the container to the next.</li>

* The first time you run it for a given postgres volume, once the containers are up you need to run a shell on the server container with <code>docker exec -it tom_desc_tom_1 /bin/bash</code> (substituting the name your container got for "tom_desc_tom_1"), and then run the commands:
  - <code>python manage.py createsuperuser</code> (and answer the prompts)

Database migrations are applied automatically as part of the docker compose setup, but you need to manually create the TOM superuser account so that you have something to log into.

At this point, you should be able to connect to your running TOM at `localhost:8080`.

If you are using a new postgres data volume (i.e. you're not reusing one from a previous run of docker compose), you need to create the "Public" group.  You need to do this before adding any users.  If all is well, any users added thereafter will automatically be added to this group.  Some of the DESC specific code will break if this group does not exist.  (The TOM documentation seems to imply that this group should have been created automatically, but that doesn't seem to be the case.)  To do this:
``` python manage.py shell
>>> from django.contrib.auth.models import Group
>>> g = Group( name='Public' )
>>> g.save()
>>> exit()
```

If you ever run a server that exposes its interface to the outside web, you probably want to edit your local version of the file `secrets/django_secret_key`.  Don't commit anything sensitive to git, and especially don't upload it to github!  (There *are* postgres passwords in the github archive, which would seem to voilate this warning.  The reason we're not worried about that is that both in the docker-compose file, and as the server is deployed in production, the postgres server is not directly accessible from outside, but only from within the docker environment (or, for production, the Spin namespace). Of course, it would be better to add the additional layer of security of obfuscating those passwords, but, whatever.)

### Populating the database

#### For ELAsTiCC
<a href="https://portal.nersc.gov/cfs/lsst/DESC_TD_PUBLIC/users/raknop/elasticc_subset.sql">Here is a small subset</a> of the tables from September 2022-January 2203 ELAsTiCC campaign.  It includes:

* 1,000 objects selected randomly
* 10,145 sources (and thus alerts) for those objects
* 28.900 forced sources for those objects
* 54 broker classifiers
* 60,586 broker messages for those alerts
* 1,306,702 broker classifications from those broker messages

*Note*: this SQL dump is compatible with the schema in the database as of 2022-03-23.  If the schema evolve, then this SQL dump will (probably) no longer be able to be loaded into the database.

To populate the `elasticc` tables of the database with this subset, copy this file to the `tom_desc` subdirectory of your checkout.  (That is, if your checkout is in `tom_desc`, copy this file to the `tom_desc/tom_desc/` directory.)  Get a shell on your running tom_desc_tom container (using a command something like `docker exec -it tom_desc_tom_1 /bin/bash`).  Once there, run the command:

`psql -h postgres -U postgres tom_desc < elasticc_subset.sql`

You will be prompted for the postgres password, which is "fragile".  (This postgres instance should not be accessible outside of your docker container environment, which is why it doesn't have a secure password.)  If all goes well, you'll get a bunch of numbers telling you how many rows were put into various tables, and you will get no error messages.  After that, your database should be populated.  Verify this by going to the link `http://localhost:8080/elasticc/summary` and verify that the numbers match what's listed above.  (You will need to be logged into your instance of the TOM for that page to load.)

#### For ELAsTiCC2

TODO

### Development and database migrations

Note that the web software itself doesn't live in the docker image, but is volume mounted from the "tom_desc" subdirectory.  For development, you can just edit the software directly there.  To get the server to pull in your changes, you need to run a shell on the server's container and run `kill -HUP 1`.  That restarts the gunicorn webserver, which forces it to reread all of the python code that define the web ap.

If you change any database schema, you have to get a shell on the server's container and:
* `python manage.py pgmakemigrations` (**NOTE**: Do NOT run makemigrations, which is what django and tom documentation will tell you to do, as the models use some postgres extentions (in particular, partitioned tables) that makemigrations will not succesfully pick up.)
* Check to make sure the migrations created look right, and do any
  manual intervention that's needed.  (Ideally, manual intervention will
  be unnecessary, or at worst small!)
* `python manage.py migrate`
* Deal with the situation if the migration didn't apply cleanly.
* `kill -HUP 1` to get the running webserver synced with the current code.

BE CAREFUL ABOUT DATABASE MIGRATIONS.  For throw-away development environments, it's fine.  But, the nature of database migrations is such that forks in database schema history are potentially a lot more painful to deal with than forks in source code (where git and text merge can usually handle it without _too_ much pain).  If you're going to make migrations that you want to have pulled into the main branch, coordinate with the other people working on the DESC Tom.  (As of this writing, that's me, Rob Knop.)

#### Cassandra Migrations

`pgmakemigrations` will create a migration file, but it doesn't seem to do anything.  You have to run `python manage.py sync_cassandra` to update cassandra schema.  This is scary; it doesn't seem to actually handle reversable migrations.


## Deployment at NERSC

The server runs on NERSC Spin, in the `desc-tom` namespace of production m1727.  Rob maintains this.

The actual web ap software is *not* read from the docker image (although a version of the software is packaged in the image).  Rather, the directory `/tom_desc` inside the container is mounted from the NERSC csf file system.  This allows us to update the software without having to rebuild and redploy the image; the image only needs to be redeployed if we have to add prerequisite packages, or if we want to update the OS software for security patches and the like.  The actual TOM software is deployed at `/global/cfs/cdirs/desc-td/SOFTWARE/tom_deployment/production/tom_desc` (with the root volume for the web server in the `tom_desc` subdirectory below that).  Right now, that deployment is just handled as a git checkout.  After it's updated, things need to happen *on* the Spin server (migrations, telling the server to reload the software).  My (Rob's) notes on this are in `rob_notes.txt`, though they may have fallen somewhat out of date.

### Steps for deployment

This is assuming a deployment from scratch.  You probably don't want to do this on the production server, as you stand a chance of wiping out the existing database!  Only do this if you really know what you're doing (which, at the moment, is probably only Rob.)  If you're in a situation where Rob isn't available, if you understand NERSC Spin the vocabulary here should make sense; the NERSC Spin support people should be able to help, as should the `#spin` channel on the NERSC Users Slack.

Do `module load spin` on perlmutter.  Do `rancher context switch` to get in the right rancher cluster and context.  Create a namespace (if it doesn't exist already) with `rancher namespace create <name>`.  Rob uses `desc-tom` on the spin dev cluster, and for production deployment, `desc-tom` and `desc-tom-2` on the production cluster.

All of the things necessary to get the TOM running are specified in YAML files found in the `spin_admin` subdirectory.  To create or update something on Spin, you "apply" the YAML file with

  `rancher kubectl apply --namespace <namespace> -f <filename>`

where `<namespace>` is the Spin namespace you're working in, and `<filename>` is the YAML file you're using.  If you're not using the default deployment, you *will* have to edit the various YAML files for what you're doing; among other things, all "namespace" and "workloadselector" fields will have to be updated for the namespace you're using.

You can see what you have running by doing all of:

* `rancher kubectl get all --namespace <namespace>`
* `rancher kubectl get pvc --namespace <namespace>`
* `rancher kubectl get secret --namespace <namespace>`

(it seems that "all" doesn't really mean all).  You can also just look to see what actual pods are running with `get pods` in place of `get all`.  This is a good way to make sure that the things you think are running are really running.  You can get the logs (stdout and stderr from the entrypoint command) for a pod with

  `rancher kubectl logs --namespace <namespace> <pod>`

where you cut and paste the full ugly pod name from the output of `get all` or `get pods`.

(It's also possible to use the web interface to monitor what's going; you should know about that if you've been trained on NERSC Spin.)

#### The steps necessary to create the production TOM from scratch:

After each step, it's worth running a `rancher kubectl get...` command to make sure that the thing you created or started is working.  There are lots of reasons why things can fail, some of which aren't entirely under your control (e.g. servers that docker images are pulled from).

- Create the two persistent volume claims.  There are two files, `tom-postgres-pvc.yaml` and `tom-cassandra-pvc.yaml` that describe these.  If you're making a new deployment somewhere, you *will* need to edit them (so that things like "namespace" and "workloadselector" are consistent with everything else you're doing).  Be very careful with these files, as you stand a chance of blowing away the existing TOM database if you do the wrong thing.

- Create the cassandra deployment.  (As of this writing, the cassandra database is actually not actively used, but the TOM won't start up without it being available.)  The YAML file is `tom-cassandra.yaml`

- Create the postgres deployment.  The YAML file is `tom-postgres.yaml`

- Create the secrets.  The yaml file is `tom-secrets.yaml`.  This file *will* need to be edited, because we don't want to commit the actual secrets to the git archive.  Rob keeps a copy of the YAML file with the actual secrets in place in his `~/secrets` directory; you won't be able to read this, but if you're stuck needing to create this when Rob isn't around, NERSC people should be able to help you.  (TODO: find a safe collabration-available place to keep this.)

- Create the web certificate for the web app.  The YAML file is `tom-cert.yaml`.  Like `tom-secrets.yaml`, this one needs to be edited.  Because certificates expire, the contents of this have to be updated regularly (yearly, using the LBL certificates that Rob uses).  The actual content of the secrets is created with a combination of `openssl` and `base64` commands.  TODO: really document this.

- Create the webap server.  The yaml file is `tom-app.yaml`.  This one has to run as a normal user because it needs to bind-mount directories.  The file right now has Rob's userid as the user to run as.  To change this, edit the file and look at the following fields under `spec.template.metadata.annotations`:
  - `nersc.gov/collab_uids`
  - `nersc.gov/gid`
  - `nersc.gov/gids`
  - `nersc.gov/uid`
  - `nersc.gov/username`
...and maybe some others.

- Once everything is set up, you still have to actually create the database; to do this, get a shell on the app server with `rancher kubectl exec --stdin --tty --namespace=<namespace> <podname> -- /bin/bash` and run
  - `python manage.py migrate`
  - `python manage.py createsuperuser`

- Create the `Public` Group.   (The TOM documentation seems to imply that this group should have been created automatically, but that doesn't seem to be the case.)
  ```
   python manage.py shell
   >>> from django.contrib.auth.models import Group
   >>> g = Group( name='Public' )
   >>> g.save()
   >>> exit()
   ```

- You then probably want to do things to copy users and populate databases and things....

#### Updating the running code

Whenever making any changes to the code (which *might* include manual database manipulation, but hopefully doesn't), you need to tell the `gunicorn` web server on the tom-app pod to refresh itself.  Do this with a shell on that pod with `kill -HUP 1`.

#### Additional YAML files

There are some additional `.yaml` files in the `spin_admin` directory for other services that are useful for the actual business of ELAsTiCC2:

* `tom-send-alerts-cron.yaml` creates a cron job to send out alerts while the actual ELASTiCC2 campaign is running.  That file will need to be edited so that the `args` field has the right arguments to do what you want to do.

* `tom-pgdump.yaml` creates a cron job that (as currently configured) does a weekly pg_dump of the postgres database, to the directory found under the `hostPath:` field underneath `volumes:`.

(others)

#### Removing it all from Spin

This is fiddly.  You have to do a bunch of `rancher kubectl delete <thing> --namespace <namespace> <spec>`, where `<thing>` is deployment, service, ingress, secret, and pvc.  You can figure out what there is with the three `rancher kubectl get...` commands above.  BE VERY CAREFUL BEFORE DELETING A PVC.  For everything else, after you delete it, you can recreate it, if you have current yaml files.  If you delete a persistent volume claim, all of the data on it is **gone**.

# Notes for ELASTICC

## ELASTICC

Since the first ELAsTiCC campaign is long over, the realtime stuff here is not relevant any more.

### Streaming to ZADS

This is in the `LSSTDESC/elasticc` archive, under the `stream-to-zads` directory.  The script `stream-to-zads.py` is designed to run in a Spin container; it reads alerts from where they are on disk, and based on the directory names of the alerts (which are linked to dates), and configuration, figures out what it needs to send

### Pulling from brokers

The django management command `elasticc/management/commands/brokerpoll.py` handled the broker polling.  It ran in its own Spin container with the same Docker image as the main tom web server (but did not open a webserver port to the outside world).


## ELASTICC2

### Streaming to ZADS

The django management command `elasticc2/management/commands/send_elasticc2_alerts.py` is able to construct avro alerts from the tables in the database, and send those avro alerts on to a kafka server.  This command is run in the cron job described by `spin_admin/tom-send-alerts-cron.yaml`.

### Fake broker

In the `LSSTDESC/elasticc` archive, under the `stream-to-zads` directory, there is a script `fakebroker.py`.  This is able to read ELaSTiCC alerts from one kafka server, construct broker messages (which are the right structure, but have no real thought behind the classifications), and send those broker messages on to another kafka server.  It's run as aprt of the test suite (see below).


## Testing

The `tests` subdirectory has tests.  They don't test the basic TOM functionality; they test the DESC stuff that we've added on top of the TOM.  (The hope is that the TOM passes its own tests internally.)  Currently, tests are still being written, so much of the functionality doesn't have any tests, sadly.  One key important test, though, is the one that verifies that the flow of alerts from TOM to kafka server, and back from broker's kafka servers to the TOM, or posted to the TOM via the API that AMPEL uses, works.

The tests are designed to run inside the framework described by the `docker-compose.yaml` file.  Tests of the functionality require several different services running:

  * two kafka services (zookeeper and server)
  * a backend postgres service
  * a backend cassandra service
  * the TOM web server
  * a fake broker (to ingest alerts and produce broker classifications for testing purposes)
  * a process polling the fake broker for classifications to stick into the tom
  * a client container on which to run the tests

The `docker-compose.yaml` file has an additional service `createdb` that creates the database tables once the postgres server is up; subsequent services don't start until that service is finished.  It starts up one or two different client services, based on how you run it; either one to automatedly run all the tests (which can potentially take a very long time), or one to provice a shell host where you can manually run individual tests or poke about.

### Building the framework

Make sure all of the necessary images are built on your system by moving into the `tests` subdirectory and running:
```
docker compose build
```

### Running a shell in the framework
```
To get an environment in which to run the tests manually, run
```
ELASTICC2_TEST_DATA=<dir> docker compose up -d shellhost
```
where `<dir>` is a directory that has three models from the 1% ELAsTiCC2 test set that the tests are designed to run with; on brahms (Rob's) desktop, this would be:
```
ELASTICC2_TEST_DATA=/data/raknop/elasticc2_train_1pct_3models docker compose up -d shellhost
```
If you're not going to use these, you can omit the variable.  You can then connect to that shell host with `docker exec -it <container> /bin/bash`, giving it the name of the container started by docker compose (probably `tests-shellhost-1`).  The postgres server will on the host named `postgres` (so you can `psql -h postgres -U postgres tom_desc` to poke at the database— the password is "fragile", which you can see in the `docker-compose.yaml` file).  The web server will be on the host named `tom`, listening on port 8080 without SSL  (so, you could do `netcat tom 8080`, or write a python script that uses requests to connect to `http://tom:8080`... or, for that matter, use the `tom_client.py` file included in the `tests` directory).

When you're done with everything, do
```
docker compose down
```

### Automatically running all the tests

In the `tests` directory (on your machine, _not_ inside a container), after having built the framework, do:
```
ELASTICC2_TEST_DATA=<dir> docker compose run runtests
```
where `<dir>` is a directory with the 1% ELAsTiCC2 test set (just as with running a shell host, above).

After the tests complete (which could take a long time), do
```
docker compose down
```
to clean up.  (If you don't do this, the next time you try to run the tests, it won't have a clean slate and the startup will fail.)
