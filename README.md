# DESC TOM

Based on the [Tom Toolkit](https://lco.global/tomtoolkit/)

# Using the TOM

The "production" server at nersc is `https://desc-tom.lbl.gov` ; ask Rob
(raknop@lbl.gov or Rob Knop on the LSST Slack) if you need a user account.

At the moment, the TOM is only holding the alerts and broker messages
for the ELAsTiCC challenge.  It will evolve from here (using at least
ELAsTiCC as a test data set) into something that will prioritize and
schedule follow-up observations; it may also evolve into the FAST
database.

There are a few ways to use it:

* Interactively with a browser.  There's not much there yet.
* There are a few REST-like interfaces for ELAsTiCC you can use to pull down information
* You can access the database directly by passing SQL to a thin http
interface; see `sql_query_tom_db.py` or (similar code as a Jupyter
notebook) `sql_query_tom_db.ipynb`; see [Database Schema and Notes](#database-schema-and-notes) for more information.

## Example REST interface usage

To use these, you need to have an account on the TOM, and you need to
log into it with the client you're using.  Here's an example:

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

Note that as the elasticc database has grown, many of these endpoints will
time out before they're able to pull and send all of their informaiton.


Called by themselves, they return a JSON dict as in the example above,
with `count` giving the total number of objects (or sources or truth
table entries), and `results` having an array of dictionaries for the
first hundred objects.  `next` and `previous` have URLs for getting the
next or previous 100 from the list.  (This will be fraught if records
are actively being addded to the database at the same time as when
you're running your queries.)  As in the example above, you can append a
single number (with a trailing slash after it) to pull down the
information for that one object or source; that number is (respectively)
diaObjectId or diaSourceId.  (For the Truth table, pass the relevant
diaSourceId.)

## Database Schema and Notes

The schema for the elasticc tables in the database can be found in the
comments at the end of `sql_query_tom_db.py`.  Everybody can read the
`elasticc_broker*` tables; only people in the `elasticc_admin` group can
read the other `elasticc_*` tables.

### classId and gentype

Broker Messages include a classification in the field `classId`; these
ids use the [ELAsTiCC
taxonomy](https://github.com/LSSTDESC/elasticc/blob/main/taxonomy/taxonomy.ipynb).
This is a hierarchical classification.  Classifications in the range 0-9
correspond to a broad class.  Classifications in the range 10-99
correspond to a more specific category.  Classifications in the range
100-999 correspond to an "exact" classification (or, as exact as the
taxonomy gets).  The first digit of a classification tells you its broad
class, the second digit tells you the specific category within the
broad class, and the third digit tells you the exact classification
within the specific category.  (Go look at the taxonomy; there's a
map there that will make it clearer than this description.)

The true model type used to create the original alerts are in the field
`gentype` in the table `elasticc_diaobjecttruth`; this field is an
internal SNANA index.  The mapping between `classId` and `gentype` is
complicated for a few reasons.  First, SNANA (of course) uses individual
models to generate lightcurves, so there will be no `gentype`s
corresponding to the broad class or specific categories of the
taxonomy.  Second, in some cases (e.g. core-collapse supernovae), there
are multipel different SNANA models (and thus multiple different
`gentype` values) that correspond to the same `classId`.

There are two database tables to help matching broker classifications to
truth, but additional logic beyond just looking up lines in this table
will be needed for the reasons desribed above.

`elasticc_gentypeofclassid` gives a mapping of `classId` to all
associated `gentype` values.  This table has one entry for each gentype,
but it also has a number of entries where `gentype` is null.  These
latter entries are the cases where there is no `gentype` (i.e. SNANA
model) that corresponds to a given `classId` (e.g. in the case of
categories).  There are multiple entries for several `classId` values
(e.g. `classId` 113, for a SNII, has six different SNANA models, and
thus six different `gentype` values, associated with it).  This is the
table you would want to join to the truth table in order to figure out
which `classId` a broker _should_ have given to an event if it
classified it exactly right.

`elasticc_classidofgentype` is useful if you want to figure out if the
broker got the general category right.  There are multiple entries for
each `classId`, and multiple entries for each `gentype`.  None of the
entries in this table have a null value for either `classId` or
`gentype`.  If the `classId` is a three-digit identification (i.e. an
exact time), then the fields `categorymatch` and `exactmatch` will both
be `true`, and the information is redundant with whats in the
`elasticc_gentypeofclassid` table.  If the `classId` is a two-digit
identification, then the `exactmatch` will be `false` and the
`categorymatch` will be true.  There will be an entry for _every_
`gentype` that corresponds to something in this category.  (So, for
`classId` 11, "SN-like", there will be entries in this table for the
`gentype`s of all SNANA supernova models of all types.)  If the
`classId` is a one-digit identification, i.e. a broad class, then
both `categorymatch` and `exactmatch` will be false, and there be a
large number of lines in this table, one for each SNANA model that
corresponds to anything in the broad class.


---

# Internal Documentation

The rest of this is only interesting if you want to develop it or deploy
a private version for hacking.

## Branch Management

The branch `main` has the current production code.

Make a branch `/u/{yourname}/{name}` to do dev work, which (if
appropriate) may later be merged into `main`.


## Deployment with Docker

If you want to test the TOM out, you can deploy it on your local
machine.  If you're lucky, all you need to do is:

<ul>

<li> Run <code>git submodule update --init --recursive</code>.  There
  are a number of git submodules that have the standard TOM code.  By
  default, when you clone, git doesn't clone submodules, so do this in
  order to make sure all that stuff is there.  (Alternative, if instead
  of just <code>git clone...</code> you did <code>git clone
  --recurse-submodules ...</code>, then you've already taken care of
  this step.)  If you do a <code>git pull</code> later, you either need
  to do <code>git pull --recurse-submodules</code>, or do <code>git
  submodule --update --recursive</code> after your pull.</li>
<li> Run <code>docker-compose up</code>.  This will use the <code>docker-compose.yml</code> file
  to either build or pull two images (the web server and the postgres
  server), and run two containers.  It will also create a docker volume
  named "tomdbdata" where postgres will store its contents, so that you
          can persist the database from one run of the container to the next.</li>
<li>The first time you run it for a given postgres volume, once the
  containers are up you need to run a shell on the server container with
  <code>docker exec -it tom_desc_tom_1 /bin/bash</code> (substituting the name your
  container got for "tom_desc_tom_1"), and then run the commands:
  <ul>
    <li><code>python manage.py migrate</code></li>
    <li><code>python manage.py createsuperuser</code> (and answer the prompts)</li>
  </ul></li>
</ul>

This will set up the database schema, and create root user.  At this
point, you should be able to connect to your running TOM at
`localhost:8080`.

If you ever run a server that exposes its interface to the outside web,
you probably want to edit your local version of the file
`secrets/django_secret_key`.  Don't commit anything sensitive to git,
and especially don't upload it to github!  (There *are* postgres
passwords in the github archive, which would seem to voilate this
warning.  The reason we're not worried about that is that both in the
docker-compose file, and as the server is deployed in production, the
postgres server is not directly accessible from outside, but only from
within the docker environment (or, for production, the Spin
namespace). Of course, it would be better to add the additional layer of
security of obfuscating those passwords, but, whatever.)

### Populating the database

<a href="https://portal.nersc.gov/cfs/lsst/DESC_TD_PUBLIC/users/raknop/elasticc_subset.sql">Here
is a small subset</a> of the tables from September 2022-January 2203
ELAsTiCC campaign.  It includes:

* 1,000 objects selected randomly
* 10,145 sources (and thus alerts) for those objects
* 28.900 forced sources for those objects
* 54 broker classifiers
* 60,586 broker messages for those alerts
* 1,306,702 broker classifications from those broker messages

*Note*: this SQL dump is compatible with the schema in the database as
of 2022-03-23.  If the schema evolve, then this SQL dump will
(probably) no longer be able to be loaded into the database.

To populate the `elasticc` tables of the database with this subset, copy
this file to the `tom_desc` subdirectory of your checkout.  (That is, if
your checkout is in `tom_desc`, copy this file to the `tom_desc/tom_desc/`
directory.)  Get a shell on your running tom_desc_tom container (using a
command something like `docker exec -it tom_desc_tom_1 /bin/bash`).
Once there, run the command:

`psql -h postgres -U postgres tom_desc < elasticc_subset.sql`

You will be prompted for the postgres password, which is "fragile".
(This postgres instance should not be accessible outside of your docker
container environment, which is why it doesn't have a secure password.)
If all goes well, you'll get a bunch of numbers telling you how many
rows were put into various tables, and you will get no error messages.
After that, your database should be populated.  Verify this by going
to the link `http://localhost:8080/elasticc/summary` and verify
that the numbers match what's listed above.  (You will need to be logged
into your instance of the TOM for that page to load.)

### Development and database migrations

Note that the web software itself doesn't live in the docker image, but
is volume mounted from the "tom_desc" subdirectory.  For development,
you can just edit the software directly there.  To get the server to
pull in your changes, you need to run a shell on the server's container
and run `kill -HUP 1`.  That restarts the gunicorn webserver, which
forces it to reread all of the python code that define the web ap.

If you change any database schema, you have to get a shell on the
server's container and:
* `python manage.py pgmakemigrations` (**NOTE**: Do NOT run makemigrations, which is what django and tom documentation will tell you to do, as the models use some postgres extentions (in particular, partitioned tables) that makemigrations will not succesfully pick up.)
* Check to make sure the migrations created look right, and do any
  manual intervention that's needed.  (Ideally, manual intervention will
  be unnecessary, or at worst small!)
* `python manage.py migrate`
* Deal with the situation if the migration didn't apply cleanly.
* `kill -HUP 1` to get the running webserver synced with the current code.

BE CAREFUL ABOUT DATABASE MIGRATIONS.  For throw-away development
environments, it's fine.  But, the nature of database migrations is such
that forks in database schema history are potentially a lot more painful
to deal with than forks in source code (where git and text merge can
usually handle it without _too_ much pain).  If you're going to make
migrations that you want to have pulled into the main branch, coordinate
with the other people working on the DESC Tom.  (As of this writing,
that's me, Rob Knop.)

## Deployment at NERSC

The server runs on NERSC Spin, in the `desc-tom` namespace of production
m1727.  It reads its container image from
`registry.services.nersc.gov/raknop/tom-desc-production`; that container
image is built the Dockerfile in the "docker" subdirectory of this
repository.  (I also run another instance on the development Spin
server, for, of course, development.)

The actual web ap software is *not* read from the docker image (although
a version of the software is packaged in the image).  Rather, the
directory `/tom_desc` inside the container is mounted from the NERSC csf
file system.  This allows us to update the software without having to
rebuild and redploy the image; the image only needs to be redeployed if
we have to add prerequisite packages, or if we want to update the OS
software for security patches and the like.  The actual TOM software is
deployed at
`/global/cfs/cdirs/desc-td/SOFTWARE/tom_deployment/production/tom_desc`
(with the root volume for the web server in the `tom_desc` subdirectory
below that).  Right now, that deployment is just handled as a git
checkout.  After it's updated, things need to happen *on* the Spin
server (migrations, telling the server to reload the software).  My
(Rob's) notes on this are in `rob_notes.txt`.

### Steps for deployment

This is assuming a deployment from scratch.  You probably don't want to do this on the production server, as you stand a chance of wiping out the existing database!  Only do this if you really know what you're doing (which, at the moment, is probably only Rob.)

### With the command line

Do `module load spin` on perlmutter.  Do `rancher context switch` to get in the right rancher cluster and context.  Create a namespace (if it doesn't exist already) with `rancher namespace create <name>`.  Rob uses `desc-tom` on the spin dev cluster, and for production deployment, `desc-tom` and `desc-tom-2` on the production cluster.

- Create the persistent volume claim.  Make a copy of and edit `tom-rknop-dev-postgres-pvc.yaml` to be consistent with everything.  Be careful about this; a bunch of things need to be edited for consistency (including lots of names, namespaces, and references.  This will be fraught if you don't know what you're doing.  There are things like "workloadselector" that have to be consistent with the namespace name and the deployment name.  When done, create the pvc with
   `rancher kubectl apply --namespace=<namespace> -f <filename>`

- Create the postgres deployment.  Make a copy of and edit `tom-rknop-dev-postgres.yaml`, again being careful to get everything right!  Create the postgres installation with another `rancher kubectl apply` command, giving the new yaml filename.  You can check that the deployment is there with `rancher kubectl get deployment --namespace=<namesdpace>`, and that it's running with `rancher kubectl get pods --namespace=<namespace>`.  Check the logs of the pod to make sure postgres created its directory structure on the persistent volume, and got started up right, with `rancher kubectl logs --namepace=<filename> <podname>`, where you can get the podname from the output of the `get pods` command.

- Create the postgres service; this is the thing that's needed so that the TOM web ap server is able to communicate internally with the postgres server.  This "just happens" when you do it from the UI, but you have to do it as a separate step here.  The yaml file to copy and edit is `tom-rknop-dev-postgres-service.yaml`.

- Create the secrets.  The yaml file to copy and edit is `tom-rknop-dev-secrets.yaml`.

- Create the webap server.  The yaml file to copy and edit is `tom-rknop-dev-app.yaml`.  There is an additional wrinkle with this.  Right now, I do *not* have the django code baked into the Docker image, because everything is still under active development.  Rather, I have the django code in a directory on CFS, and a mount that as a volume in spin.  This requires running the workload under the same UID as the owner of the directory.  This means (at least) editing some fields under `spec.template.metadata.annotations`
  - `nersc.gov/collab_uids`
  - `nersc.gov/gid`
  - `nersc.gov/gids`
  - `nersc.gov/uid`
  - `nersc.gov/username`
...and maybe some others.

- Create the tom webap service; `tom-rknop-dev-app-service.yaml`.

- Create the service for the ingress:

- Create the ingress: `<filename>`.  Deal with certificates; I do this in the UI right now, and am not sure how to translate it properly to command-line stuff.

- Once everything is set up, you still have to actually create the database; to do this, get a shell on the app server with `rancher kubectl exec --stdin --tty --namespace=<namespace> <podname> -- /bin/bash` and run
  - `python manage.py migrate`
  - `python manage.py createsuperuser`

- You then probably want to do things to copy users and populate databases and things....

- Whenever making any changes to the code (which *might* include manual database manipulation, but hopefully doesn't), you need to tell the `gunicorn` web server on the app workload to refresh itself.  Do this with a shell on that workload with `kill -HUP 1`.

### With the UI

**This may be out of date!  In fact, it certainly is, because Spin is migrating to a new UI (the "cluster explorer"), and this is for the old one.  But, it may be out of date even for that.**

For the passwords obscured below, look at the `0022_ro_users.py` migration file in `tom_desc/elasticc/migrations`.

- Create a secrets volume with
     - django_secret_key equal to something long and secure
     - postgres_password equal to fragile
     - postgres_elasticc_ro equal to <the right password>
     - postgres_elasticc_admin_ro equal to <the right password>
- Create the postgres workload at spin.
  - image: rknop/tom-desc-postgres
  - env vars
      - POSTGRES_PASSWORD_FILE=/secrets/postgres-password
      - POSTGRES_USER=postgres
      - POSTGRES_DB=tom_desc
  - volume: persistent storage claim mounted at /var/lib/postgresql/data
      - Size to request: hard to say.  I put in 1024GB.
  - Bind-mount a volume to mount the secrets described above mounted at /secrets
  - Otherwise standard spin stuff (I think)
      - This includes under "Security & Host Config" (available via
        "Show Advanced Options" in the lower-right) selecting "ALL"
        under "Drop Capabilities" and "CHWON", "DAC_OVERRIDE", "FOWNER",
        "NET_BIND_SERVICE", "SETGID", and "SETUID" under "Add Capabilities".
  - Fix an annoying spin permissions issue so that postgres can read the volume
      - Don't start the postgres workload (make it a scalable deployment of 0 pods)
      - Make a temporary workload that gives you a linux shell and mounts the same volume
      - chown {uid}:{gid} on the mounted volume inside a pod running that temporary workload
          where uid and gid are of the postgres user (101 and 104 in my case)
      - Now set the postgres workload to a scalable deployment of 1 pod;
      - it should run happily.
- Create the tom workload with:
   - image rknop/tom-desc-production
   - env vars:
       - DB_HOST={name of the postgres workload}
       - DB_NAME=tom_desc
       - DB_PASS=fragile
       - DB_USER=postgres
   - Volumes
       - secrets described above mounted at /secrets
       - a bind mount of the tom_desc subirectory of the CFS directory
         where there is a checkout of this archive; mount this at
         /tom_desc
- Under "Command", User ID must have the uid that owns the CFS directory, and Filesystem Group the gid
   - Under "Security and Host Config"
       - Run as non-root must be "Yes"
       - Under "Security & Host Config", instead of the usual spin
         recommendations *only* add the NET_BIND_SERVICE capability
   - Create a ingress under "Load Balancing".  (More information needed.)

When you first create the database and the TOM, the tom won't work,
because the database tables aren't set up. Run a shell on the TOM's
workload, and then do `python manage.py migrate` to set up those
database tables.  When done, do `kill -HUP 1` to restart the web
server.  This is only necessary when you start the first time.

### Copying users

I haven't figured out the "right" way to do this with django, so here's an ugly hack.

- On the existing server, run
   `pg_dump -h <postgreshost> -U postgres tom_desc -t auth_user -a -f users.sql`

- Edit the .sql file to remove the rows with `AnonymousUser` and `root`.

- Restore the dump on the new server with
   `psql -h <postgreshost> -U postgres tom_desc < users.sql`

# Notes for ELASTICC

## ELASTICC

### Streaming to ZADS

This is in the `LSSTDESC/elasticc` archive, under the `stream-to-zads` directory.  The script `stream-to-zads.py` is designed to run in a Spin container; it reads alerts from where they are on disk, and based on the directory names of the alerts (which are linked to dates), and configuration, figures out what it needs to send

### Pulling from brokers

The django management command
`elasticc/management/commands/brokerpoll.py` handled the broker polling.
It ran in its own Spin container with the same Docker image as the main
tom web server (but did not open a webserver port to the outside world).


## ELASTICC2

### Streaming to ZADS

The django management command `elasticc/management/commands/send_elasticc_alerts.py` is able to construct avro alerts from the tables in the database, and send those avro alerts on to a kafka server.

### Fake broker

In the `LSSTDESC/elasticc` archive, under the `stream-to-zads` directory, there is a script `fakebroker.py`.  This is able to read ELaSTiCC alerts from one kafka server, construct broker messages (which are the right structure, but have no real thought behind the classifications), and send those broker messages on to another kafka server.