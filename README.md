# Using the TOM

The "production" server at nersc is `https://desc-tom.lbl.gov` ; ask Rob
(raknop@lbl.gov or Rob Knop on the LSST Slack) if you need a user account.

There are a few ways to use it.  The browser-base web interface hasn't
been well developed yet, but there are some of the basic things that
come with the TOM there.  The ELAsTiCC challenge information is all
being stored underneath `https://desc-tom.lbl.gov/stream`.  You can try
clicking about there to see what you get, but this isn't terrily useful
let.

There are some REST-like interfaces that you could use programmatically
to pull down information.  To do this, you need to have an account on
the TOM, and you need to log into it with the client you're using.
Here's an example:

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

response = rqs.get( f'{url}/stream/elasticcdiaobject' )
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
response = rqs.get( f'{url}/stream/elasticcdiaobject/{objid}' )
data = json.loads( response.text )
print( data.keys() )
print( f'{data["ra"]}  {data["decl"]}' )
```

Currently defined are:
* `https://desc-tom.lbl.gov/stream/elasticcdiaobject`
* `https://desc-tom.lbl.gov/stream/elasticcdiasource`
* `https://desc-tom.lbl.gov/stream/elasticcdiatruth`

Called by themselves, they return a JSON dict as in the example above,
with `count` giving the total number of objects (or sources or truth
table entries), and `results` having an array of dictionaries for the
first hundred objects.  `next` and `previous` have URLs for getting the
next or previous 100 from the list.  (This will be fraught if records
are actively being addded to the database at the same time as when
you're running your queries.)  As in the example above, you can append a
single number (after a slash) to pull down the information for that one
object or source; that number is (respectively) diaObjectId or
diaSourceId.  (For the Trutht table, pass the relevant diaSourceId.)

If you want low-level access to the database by sending SQL queries, you
can read the database via a thin web API; see
[`sql_query_tom_db.py`](sql_query_tom_db.py)
for instructions and an example (and some of the table schema).  This
requires you to have an account on the TOM, and is read-only access.

The rest of this is only interesting if you want to develop it or deploy
a private version for hacking.

---

# Deployment at NERSC

The server runs on NERSC Spin, in the `desc-tom` namespace of production
m1727.  It reads its container image from
`registry.services.nersc.gov/raknop/tom-desc-production`; that container
image is built from the Dockerfile in the top level directory of this
archive.

The actual web ap software is *not* read from the docker image (although
a version of the software is packaged in the image).  Rather, the
directory `/tom_desc` inside the container is mounted from the NERSC csf
file system.  This allows us to update the software without having to
rebuild and redploy the image; the image only needs to be redeployed if
we have to add prerequisite packages, or if we want to update the OS
software for security patches and the like.  The actual TOM software is
deployed at `/global/cfs/cdirs/m1727/tom/deploy_production/tom_desc`.
Right now, that deployment is just handled as a git checkout.  After
it's updated, things need to happen *on* the Spin server (migrations,
telling the server to reload the software).  My (Rob's) notes on this
are in `rob_notes.txt`.

# Local deployment

## [Experimental] Development with Docker Compose

You can try deploying the TOM Toolkit and a Postgres (w/ PostGIS) database via
`docker-compose`:

```
git clone https://github.com/LSSTDESC/tom_desc
cd tom_desc
docker-compose up
```

This will spin up the TOM and create a superuser `root:password`. It runs in
development mode so should hot-reload any changes you make. An exception is
database changes. The environment will run database migrations the first time
that you start it but if you make changes to models you will have to apply them
manually:

```
docker-compose exec tom python manage.py makemigrations
docker-compose exec tom python manage.py migrate
```

You can pass additional environment variables to the TOM container in
`docker-compose.yml` by adding to the `services.tom.enviroment` field.

## Installing the TOM Toolkit locally

The toolkit github repository is at https://github.com/LSSTDESC/tom_desc.  Install it locally.

The use of a virutal environment is recommended.

```bash
python3 -m venv tom_env/
```
Now that we have created the virtual environment, we can activate it:
```bash
source tom_env/bin/activate
```

Install the requisite packages into the virtual environment

```bash
pip install -r requirements.txt
```
## Local deployment environment variables

Authentication is handled by environment variables.  These are consumed by
`tom_desc/settings.py`.  The `DB` variables are required for the database.
The other variables are needed if you want to access streams.

```
DB_PASS
DB_HOST=localhost

ANTARES_KEY
ANTARES_SECRET

HOPSKOTCH_USER
HOPSKOTCH_PASSWORD

FINK_USERNAME
FINK_GROUP_ID
FINK_SERVER
FINK_TOPIC

GOOGLE_CLOUD_PROJECT
GOOGLE_APPLICATION_CREDENTIALS
```

## Local Database Server

Getting a dockerized  database up and running is a required. Here's how:
```bash
 export DB_HOST=127.0.0.1
 docker run --name tom-desc-postgres -v /var/lib/postgresql/data -p 5432:5432 -e POSTGRES_PASSWORD=<PG_PASS> -d postgis/postgis:11-2.5-alpine

docker exec -it tom-desc-postgres /bin/bash  # start a shell inside the postgres container

createdb -U postgres tom_desc                # create the tom_demo database
exit                                         # leave the container, back to your shell
```

If this is your first time creating the `tom_desc` database, you must create the tables and put
some data in the database that you just created.
```bash
# make sure you are in your virtual environment, then
./manage.py migrate           # create the tables
./manage.py collectstatic     # gather up the static files for serving
```
#

## Running the TOM
Now that you have a database server up and running on your local machine, consider these alternatives for local development your TOM:

### Aternative 1: Running `tom-desc` in your virtual environment, via `./manage.py runserver`
<details>

```bash
./manage.py runserver &
# see the output "Starting development server at <URL>" for where to point your browser.
```
</details>

### Alternative 2: Running `tom-desc` dockerized, via `docker run`
<details>

```bash
docker build -t tom-desc .
```

According to TOM instructions this works but it didn't on my Mac.
```bash
docker build -t tom-desc .                     # build a docker image of your current sandbox
docker run --network="host" tom-desc &
# point your browser at localhost 
```

To get it working on my Mac I had to do the following
```bash
docker network create tom-net
docker network connect tom-net tom-desc-postgres
docker run -p 8080:8080 --network=tom-net tom-desc &
# point your browser at localhost:8080
```
</details>
