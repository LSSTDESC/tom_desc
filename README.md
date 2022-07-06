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

- Create the postgres workload at spin.
  - image: registry.services.nersc.gov/raknop/tom-desc-postgres
  - env var: POSTGRES_DATA_DIR=/var/lib/postgresql/data
  - volume: persistent storage claim mounted at /var/lib/postgresql/data
  - Otherwise standard spin stuff (I think)
  - Fix an annoying spin permissions issue so that postgres can read the volume
      - Don't start the postgres workload (make it a scalable deployment of 0 pods)
      - Make a temporary workload that gives you a linux shell and mounts the same volume
      - chown <uid>:<gid> on the mounted volume inside a pod running that temporary workload
          where uid and gid are of the postgres user (101 and 104 in my case)
      - Now set the postgres workload to a scalable deployment of 1 pod; it should run happily.
- Create the tom workload with:
   - image registry.services.nersc.gov/raknop/tom-desc-production (or -dev)
   - env vars:
       - DB_HOST=<name of the postgres workload>
       - DB_NAME=tom_desc
       - DB_PASS=fragile
       - DB_USER=postgres
       - DJANGO_SECRET_KEY=
       


# Branch Management

The branch `tom_upstream_main` is intended to train the main branch (or,
perhaps, a stable release) on https://github.com/TOMToolkit/tom_base .
When I checkout, I add that repository as the "upstream" remote.  I have
this so I can then merge it into a dev branch, to get upstream updates.

The branch `main` has the current production code.

Make a branch `/u/<yourname>/<name>` to do dev work, which (if
appropriate) may later be merged into `main`.

Historically, the DESC Tom was based on a fork of the TOM Toolkit from a
few years ago.  In July 2022, I started over by adding the
tom_upstream_main branch, then branching main from that, and manually
putting back in the changes necessary to recreate the DESC Tom.


# TOM Toolkit
[![pypi](https://img.shields.io/pypi/v/tomtoolkit.svg)](https://pypi.python.org/pypi/tomtoolkit)
[![run-tests](https://github.com/TOMToolkit/tom_base/actions/workflows/run-tests.yml/badge.svg)](https://github.com/TOMToolkit/tom_base/actions/workflows/run-tests.yml)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/a09d330b4dca4a4a86e68755268b7da3)](https://www.codacy.com/gh/TOMToolkit/tom_base/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=TOMToolkit/tom_base&amp;utm_campaign=Badge_Grade)
[![Coverage Status](https://coveralls.io/repos/github/TOMToolkit/tom_base/badge.svg?branch=main)](https://coveralls.io/github/TOMToolkit/tom_base?branch=main)
[![Documentation Status](https://readthedocs.org/projects/tom-toolkit/badge/?version=stable)](https://tom-toolkit.readthedocs.io/en/stable/?badge=stable)
[Documentation](https://tom-toolkit.readthedocs.io/en/latest/)

![logo](tom_common/static/tom_common/img/logo-color.png)

The TOM Toolkit is a web framework for building TOMs: Target and Observation
Managers. TOMs are meant to facilitate collaborative astronomical observing
projects. A typical TOM allows its users to curate target lists, request
observations of those targets at various observatories as well as manage and
organize their data. [Read more](https://tom-toolkit.readthedocs.io/en/stable/introduction/about.html) about TOMs.

## Getting started with the TOM Toolkit
The [getting started guide](https://tom-toolkit.readthedocs.io/en/latest/introduction/getting_started.html)
will guide you through the process of setting up a TOM for the first time.

## Reporting issues/feature requests
Please use the [issue tracker](https://github.com/TOMToolkit/tom_base/issues) to
report any issues or support questions.

## Contributing to the project
If you'd like to contribute to the TOM Toolkit, first of all, thanks! Secondly, we
have a [contribution guide](https://tom-toolkit.readthedocs.io/en/stable/introduction/contributing.html) that
you might find helpful. We are particularly interested in the contribution of
observation and alert modules.

## Developer information
For development information targeted at the maintainers of the project, please see [README-dev.md](README-dev.md).


## Plugins

### tom_alerts_dash

The [tom_alerts_dash](https://github.com/TOMToolkit/tom_alerts_dash) plugin adds responsive ReactJS views to the 
`tom_alerts` module for supported brokers.

### Antares

The [tom-antares](https://github.com/TOMToolkit/tom_antares) plugin adds support
for querying the Antares broker for targets of interest.

### tom_nonsidereal_airmass

The [tom_nonsidereal_airmass](https://github.com/TOMToolkit/tom_nonsidereal_airmass) plugin provides a templatetag
that supports plotting for non-sidereal objects. The plugin is fully supported by the TOM Toolkit team; however,
non-sidereal visibility calculations require the PyEphem library, which is minimally supported while its successor
is in development. The library used for the TOM Toolkit sidereal visibility, astroplan, does not yet support
non-sidereal visibility calculations.

### tom-lt

This module provides the ability to submit observations to the Liverpool Telescope Phase 2 system. It is in a very alpha
state, with little error handling and minimal instrument options, but can successfully submit well-formed observation
requests.

[Github](https://github.com/TOMToolkit/tom_lt)

### tom_registration

The [tom_registration](https://github.com/TOMToolkit/tom_registration) plugin introduces support for two TOM registration 
flows--an open registration, and a registration that requires administrator approval.
