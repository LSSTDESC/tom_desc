# DESC TOM

Based on the [Tom Toolkit](https://lco.global/tomtoolkit/)

# Using the TOM

The "production" server at nersc is `https://desc-tom.lbl.gov` ; ask Rob
(raknop@lbl.gov or Rob Knop on the LSST Slack) if you need a user account.

There are a few ways to use it.  The browser-based web interface hasn't
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
image is built the Dockerfile in the "docker" subdirectory of this
repository.

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

## Steps for deployment

This is assuming a deployment from scratch.  You probably don't want to
do this on the production server, as you stand a chance of wiping out
the existing database!

- Create a secrets volume with
     - django_secret_key equal to something long and secure
     - postgres_password equao to fragile
     - postgres_ro_password equal to SOMETHING
- Create the postgres workload at spin.
  - image: registry.services.nersc.gov/raknop/tom-desc-postgres
  - env vars
      - POSTGRES_DATA_DIR=/var/lib/postgresql/data
      - POSTGRES_PASSWORD_FILE=/secrets/postgres-password
      - POSTGRES_USER=postgres
  - volume: persistent storage claim mounted at /var/lib/postgresql/data
  - secrets described above mounted at /secrets
  - Otherwise standard spin stuff (I think)
  - Fix an annoying spin permissions issue so that postgres can read the volume
      - Don't start the postgres workload (make it a scalable deployment of 0 pods)
      - Make a temporary workload that gives you a linux shell and mounts the same volume
      - chown {uid}:{gid} on the mounted volume inside a pod running that temporary workload
          where uid and gid are of the postgres user (101 and 104 in my case)
      - Now set the postgres workload to a scalable deployment of 1 pod;
      - it should run happily.
  - Create the postgres_ro user used by the db app:
      - CREATE USER postgres_ro PASSWORD '{password}';   (password is what you put in secrets)
      - GRANT CONNECT ON DATABASE tom_desc TO postgres_ro;
      - In the tom_desc database:
          - GRANT USAGE ON SCHEMA public TO postgres_ro;
          - GRANT SELECT ON ALL TALBES IN SCHEMA public TO postgres_ro;
          - ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO postgres_ro;
- Create the tom workload with:
   - image registry.services.nersc.gov/raknop/tom-desc-production (or -dev)
   - env vars:
       - DB_HOST={name of the postgres workload}
       - DB_NAME=tom_desc
       - DB_PASS=fragile
       - DB_USER=postgres
   - Volumes
       - secrets described above mounted at /secrets
       - a bind mount of the CFS directory where there is a checkout of this archive; mount this at /tom_desc
   - Under "Command", User ID must have the uid that owns the CFS directory, and Filesystem Group the gid
   - Under "Security and Host Config"
       - Run as non-root must be "Yes"
       - Instead of the usual spin recommendations *only* add the NET_BIND_SERVICE capability
   - Ingress, etc.           

Note that in order to get some of the right files in tom_desc
(settings.py and some others), I originally mounted the workload with an
entrypoint of /bin/bash (so that it wouldn't run gunicorn).  I then ran
"./manage.py migrate" to get the database tables all created.  I then
made the entrypoint the standard, and redeployed the workload.  This is
only necessary when first getting started.


# Branch Management

The branch `main` has the current production code.

Make a branch `/u/{yourname}/{name}` to do dev work, which (if
appropriate) may later be merged into `main`.
