# DESC TOM

Based on the [Tom Toolkit](https://lco.global/tomtoolkit/)

* [Using the TOM](#using-the-tom)
  * [Accessing the TOM](#accessing-the-tom)
    * [Interactively with a browser](#interactively-with-a-browser)
    * [Via Web API](#via-web-api)
    * [Via SQL](#via-sql)
* [The ELAsTiCC application](#elasticc)
  * [Example REST interface usage](#example-rest-interface-usage)
  * [Database Schema and Notes](#database-schema-and-notes)
  * [classId and gentype](#classid-and-gentype)
* [The ELAsTiCC2 application](#elasticc2)
  * [Getting and pushing spectrum information](#elasticc2spec)
    * [Finding hot SNe](#elasticc2hotsne)
    * [Asking for a spectrum](#elasticc2askspec)
    * [Finding out what spectra are wanted](#elasticc2getwantedspec)
    * [Declaring intent to take a spectrum](#elasticc2planspec)
    * [Removing declared intent to take a spectrum](#elasticc2removespecplan)
    * [Reporting spectrum information](#elasticc2reportspec)
    * [Getting information about reported spectra](#elasticc2getknownspec)

* [Internal Documentation](#internal-documentation)
  * [Branch Management](#branch-management)
  * [Deploying a dev environment with Docker](#deploying-a-dev-environment-with-docker)
    * [Populating the database](#populating-the-database)
      * [For ELAsTiCC](#for-elasticc)
      * [For ELAsTiCC2](#for-elasticc2)
    * [Development and database migrations](#development-and-database-migrations)
      * [Cassandra Migrations](#cassandra-migrations)
  * [Deployment at NERSC](#deployment-at-nersc)
    * [Steps for deployment](#steps-for-deployment)
      * [The steps necessarcy to create the production TOM from scratch](#prodscratch)
      * [Updating the running code](#updating-the-running code)
      * [Additional YAML files](#additional-yaml-files)
      * [Removing it all from Spin](#removing-it-all-from-spin)
* [Notes for ELAsTiCC](#notes-for-elasticc)
  * [ELAsTiCC](#noteselasticc)
    * [Streaming to ZADS](#elasticcstream)
    * [Pulling from brokers](#elasticcpull)
  * [ELAsTiCC2](#noteselasticc2)
    * [Streaming to ZADS](#elasticc2stream)
    * [Fake Broker](#elasticc2fakebroker)
  * [Testing](#elasticctesting)
    * [Building the framework](#elasticctestbuild)
    * [Running a shell in the framework](#elasticctestshell)
    * [Automatically running all the tests](#elasticctestauto)

# Using the TOM

The "production" server at nersc is `https://desc-tom.lbl.gov` ; ask Rob (raknop@lbl.gov or Rob Knop on the LSST Slack) if you need a user account.

At the moment, the TOM is only holding the alerts and broker messages for the ELAsTiCC and ELAsTiCC2 campaigns.  It will evolve from here (using at least ELAsTiCC as a test data set) into something that will prioritize and schedule follow-up observations.

Being a Django server, the TOM is divided into different "applications".  The most relevant ones right now are:

* [ELAsTiCC](#elasticc)
* [ELAsTiCC2](#elasticc2) (Under construction!)

## Accessing the TOM

### Interactively with a browser

Just go.  Look at (for example) the ELAsTiCC, ELAsTiCC2, and Targets links in the header navigation bar.

### Via Web API

You can hit the Web API directly using then python `requests` module (or anything else that can POST to and pull from HTTP).  However, there are some annoying bits having to do with django and headers and authentication.  To make this easier, look at the file `tom_client.py` in the top level of the repository.  Read the documentation there.  In summary, you can connect by doing something like:

```
   from tom_client import TomClient
   tomclient = TomClient( url='https://desc-tom-2.lbl.gov', username='rknop',
                          passwordfile='/home/raknop/secrets/tom_rknop_passwd' )
   response = tomclient.post( "elasticc2/classids" )
   assert response.status_code = 200
   classidinfo = response.json()
```

In the constuctor, `username` is your username on the TOM.  This is *not* the same account as anywhere else.
Contact Rob if you need an account on the TOM.  You must specify either `password` or `passwordfile`; **make sure
not to commit your password to any git repository**.  This is why I use a passwordfile myself, in a
non-world-readable directory where I keep things like that.  (This is, of course, not ideal, but it seems to be the
best solution to not having to type things all the time.)

**Note: `desc-tom-2.lbl.gov` is a development TOM.**  Only use that URL in the TomClient constructor if you know what you're doing.  The production tom is `desc-tom.lbl.gov`, and is the default value for `url` (so you can leave the `url=` argument out of the TomClient constructor if you're going to `desc-tom.lbl.gov`).

### Via SQL

#### The fast query interface

You can use the TomClient to hit the url `db/runsqlquery` on the TOM.

For documentation and an example of this in action, see `sql_query_tom_db.ipynb`.

Pass it a json-encoded dictionary as POST data.  The dictionary should have one or two keys:

* `query : str` — The query to send to the database, or a list of queries.  These will be run through `psycopg2`'s `cursor.execute()` in order.  Any parameters that you calculate at runtime should in general *not* be interpolated into the string directly (using f-strings or `.format` or similar); rather, use standard `psycopg2` substitution with things like `%(varname)s`.
* `subdict : dict` — A dictionary of substitutions in `query`, e.g. `{ 'varname': 'Bobby Tables' }`, or a list of dictionaries if `query` was a list of queries.  You can omit this if you don't have any substitutions to make.

A call would look something like:
```
  res = tomclient.post( 'db/runsqlquery/',
                        json={ 'query': 'SELECT * FROM nonexistent_table WHERE name=%(name)s',
                               'subdict': { 'name': 'Bobby Tables' } } )
```

The response returned by `TomClient.post()` is a standard python `requests.Response` object.  If all is well, `res.status_code` will be 200; if it's not, something opaque went wrong that you probably can't do anything about.  (The most common failure will probably be timeouts, if you send a query that takes more than 5 minutes.  The postgres server can do this, but the web proxy will time out.)  If you look at `res.text`, you might get a hint, but you might also get a gigantic wadge of HTML.  (One particular gotcha: make sure that the URL you send to `tomclient.post` ends in a slash.  Django is very particular about that for some reason or another.)

Look at `res.json()`; that has a dictionary with key `status`.  If `status` is `"error"`, then something went wrong that the server code was able to catch; look at `res.json()['error']` for the error message.  (This is where SQL errors will show up.)

If `status` is `"ok"`, then `res.json()['rows']` will be a list of the rows returned by `cursor.fetchall()` after the the last `cursor.execute()` call on the server.  (The server uses `cursor_factory=psycopg2.extras.RealDictCursor`, so each row is a dictionary of `{ column: value }`.)

#### The slow query interface

Any request to the TOM's web API will time out if the requests takes more than 5 minutes to process.  (It's a proxy server that actually times out.)  As such, the interface above won't work for longer SQL queries.  There's another interface where you can submit a series of queries to run.  That series of queries is added to a queue of long queries that people have submitted.  A background process server-side works through that queue, saving the results either as a CSV file or a pickled Pandas data frame.  You make another web API call to check the status of your query; once it's done, a third web API call gets the data back.  All of this is documented, with an example, in `sql_query_tom_db.ipynb`.


#### Getting SQL schema

Ideally, the schema were documented somewhere.  If you want to see the schema for yourself, *be careful about assuming what columns are*, because they might not mean what you think they mean.

You can get all of the tables in (say) elasticc by sending the SQL query:
```
  SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'elasticc_%'
```

(Use `table_name LIKE 'elasticc2_%'` to see the ELAsTiCC2 tables.)

You can get the schema of a single table by sending the SQL query:
```
  SELECT column_name, data_type, column_default FROM information_schema.columns
     WHERE table_name='elasticc2_brokermessage'
```

(TODO: document getting foreign key info.)

---

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

The production location for this is https://desc-tom.lbl.gov/elasticc2/

TODO: flesh out

## <a name="elasticc2spec"></a>Getting and pushing spectrum information

The easiest way to do all of this is to use `tom_client.py`.  Make an object, and then call that object's `post` method to hit the URLs described below.

_*WARNING*_ : all of the APIs below should be considered preliminary, and may change.  In particular, there are things we probably need to add to some of them.

All of the examples below assume that you have the following code somewhere before that example:

```
   from tom_client import TomClient
   tom = TomClient( url="<url>", username="<username>", passwordfile="<passwordfile>" )
```

where `<url>` is the TOM's url; if you're going to the default (https://desc-tom.lbl.gov), then this is optional.  
See [Acessing the TOM:Via Web API](#via-web-api) for more information on using `TomClient`.


### <a name="elasticc2hotsne"></a>Finding hot transients

Currently hot transients can be found at the URL `elasticc2/gethottransients`.  POST to this URL, with the post body a json-encoded dict.  You can specifiy one of two keys in this dict:
* `detected_since_mjd: float` — will return all SNe detected since the indicated mjd. ("Detected" means a LSST alert was sent out, and at least one broker has returned a classification.)
* `detected_in_last_days: float` — will return all SNe detected between this many days before now and now.  The TOM will search what it knows about forced photometry, considering any point with S/N>5 as a detection.
* `mjd_now: float` — The mjd of right now.  Usually you don't want to specify this, and the server will automatically determine the current MJD.  This is here so it can be used with simulations, where "right now" in the simulation may not be the real right now.  You will not get back any detections or forced photometry with an MJD newer than this value.

Example:
```
   res = tom.post( 'elasticc2/gethottransients', json={ 'detected_since_mjd': 60380 } )
   assert res.status_code == 200
   assert res.json()['status'] == ok
   sne = res.json()['sne']
```

If you get something other than `status_code` 200 back from the server, it means something went wrong.  Look at `res.text` (assuming `res` is as in the example above) for a hint as to what that might be.

If you get a good return, then `res.json()` will give you a dictionary with two keywords: `status` (which is always `ok`), and `sne`.  The latter keyword is a list of dictionaries, where each element of the list has structure:
```
  { 'objectid': <int:objectid>,
    'ra': <float:ra>,
    'dec': <float:dec>,
    'zp': <float:zeropoint>,
    'redshift: <float:redshift>,
    'sncode': <int:sncode>,
    'photometry': { 'mjd': [ <float>, <float>, ... ],
                    'band': [ <str>, <str>, ... ],
                    'flux': [ <float>, <float>, ... ],
                    'fluxerr': [ <float>, <float>, ... ] }
  }
```

The `objectid` is what you will use to indicate a given SN in further communication with the TOM.  `redshift` will be less than -1 if the TOM doesn't have a redshift estimate.  `sncode` is an integer specifying the best-guess as to the object's type.  (TODO: document taxonomy, give a way to ask for details about the code classification.)  `sncode=-99` indicates the object's type is unknown.  NOTE: right now, this URL will always return `redshift` and `snocde` as -99 for everything.  Actually getting those values in there is a TODO.

### <a name="elasticc2askspec"></a>Asking for a spectrum

This URL is primarily for the SRSs ("Spectrum Recommendation System"), e.g. RESSPECT.  It tells the TOM that it wants to get a spectrum of a given object.  The URL is `elasticc2/askforspectrum`.  POST to this URL with a dictionary:

```
postdata = { 'requester': <str>,
             'objectids': [ <int>, <int>, ... ],
             'priorities': [ <int>, <int>, ... ] }
```

`requester` should be the name of the SRS instance or other facility asking for a spectrum.  `objectids` is a list of integers; these are the ones you found calling `elasticc2/gethotsne`, after filtering them and deciding which ones you really wanted.  `priorities` is a list of priorities, an integer between 1 (low priority) and 5 (high priority).  How prorities will be used is not clear at the moment.  The lengths of the objectids and priorities lists must be the same.

Example:
```
   res = tom.post( 'elasticc2/askforspectrum', json={ 'requester': 'RESSPECT',
                                                      'objectids': [ 4, 8, 15, 16, 23, 42 ],
                                                      'priorities': [ 5, 4, 3, 2, 1, 3 ] }
```

If all is well, `res.status_code` will be 200, and `res.json()` will return a dictionary with three keys.  `"status"` will be `"ok"`, `"message"` will be `"wanted spectra created"`, and `"num`" will be an integer with the number of database entries created indicating a wanted spectrum.  This number may be less than the number you asked for, because it's possible that requester/objectid already existed in the TOM's databsae.


### <a name="elasticc2getwantedspec"></a> Finding out what spectra are wanted

This is to figure out which spectra have been requested by an SRS.  If you aren't ineterested in what the SRSes have asked for, you may choose the spectra to take directly from [the list of hot SNe](#elasticc2hotsne).

The URL is `elasticc2/spectrawanted`.  Pass as the POST body a json-encoded dictionary.  This can be an empty dictionary, but it has five optional keys:

* `lim_mag : <float>`  If you pass this, you must also pass `lim_mag_band` with the single-character filter that `lim_mag` is in.  Passing this will only return objects whose most recent detection in the indicated band is this magnitude or brigther.  If you don't include this key, it doesn't use a limiting magnitude.
* `requested_since : YYYY-MM-DD` Only ask for spectra that have been requested (via an SRS posting to `elasticc2/askforspectrum`) since the indicated date.  If you don't pass this, it defaults to things requested in the last 14 days.
* `not_claimed_in_last_days : <int>`  The URL will only return objects where somebody else hasn't already said they will take a spectrum of it.  (That is, nobody else has called `elasticc2/planspectrum` (see below) for that object.)  By default, it will consider plans that have been made in the last 7 days.  Pass this to change that time window.  If you want everything that's been requested regardless of whether somebody else has already said they'll take a spectrum of it, pass 0 (or a negative number) here.
* `detected_since_mjd: <float>`  Only return objects that have a detection reported by LSST (i.e. a DiaSource was saved) between this mjd and now.  If you don't include this keyword, instead the server will use...
* `detected_in_last_days: <int>`  Only return objects that have a detected reported by LSST between this many days ago and now.  If you don't specify this value, it defaults to 14.
* `no_spectra_in_last_days: <int>`  Only return objects that don't have a spectrum yet.  This is a bit fraught; it only includes the spectra that the TOM knows about, of course, which means it only includes the one where somebody has called `elasticc2/planspectrum` (see below).  It will only consider spectra whose reported mjd of observation is between this many days ago and now.  If you don't specify this value, it defaults to 7.  If you want to have everything regardless of whether another spectrum has already been reported as taken, pass 0 (or a negative number) here.

Hit the URL with `tom.post` (like the other examples above).  Assuming all is well (the `status_code` field of the response is 200), then the `json()` method of the response will give a dectionary with two keys.  `status` will be `ok`, and `wantedspectra` will be a list.  Each element of that list is a dictionary with keys:

```
  { 'oid': <int>,    # The object id (called objectid elsewhere)
    'ra': <float>,   # The RA of the object as understood by the TOM database
    'dec': <float>,  # The Dec of the object as understood by the TOM database
    'req': <str>,    # The name of the system that requested the spectrum.
    'prio': <int>,   # The priority (between 1 (low priority) and 5 (high priority)) that the requested indicated
    'latest': <dict>
  }
```

The `latest` dictionary has the latest forced photometry detection (S/N>3) of this object.  Its keys are the filter (single-character), and the values are dictionaries with two keys, `mjd` and `mag`.

(TODO: nondetections.)

### <a name="elasticc2planspec"></a> Declaring intent to take a spectrum

Hit this URL if you plan to take a spectrum of an object.  This may be objects you found using one of the URLs above, but it doesn't have to be; it can be any object, as long as the object ID is known by the TOM.  (The TOM's object ids correspond to LSST AP DiaObjectId.)  The reason for this is so that multiple observatories can coordinate; you can declare that you're taking a spectrum of this object, so that others calling `elasticc2/getwantedspec` will then have that object filtered out.

The URL is `elasticc2/planspectrum`.  Use `TomClient` to post to it as in the examples above.  The POST body should be a json-encoded dictionary with keys;

* `objectid : int` — the object ID (as understood by the TOM) of the object.  If it's not known, you'll get an HTTP 500 ("internal server error") back.
* `facility : str` — the name of the telescope or project or similar that will be taking this spectrum.
* `plantime : YYYY-MM-DD or YYYY-MM-DD HH:MM:SS` — (or, really, anything that python's `dateutil.parser.parse` will properly interpret).  The night or time (UTC) you plan to take the spectrum.
* `comment: str` (optional) — additional text saved to the database with this declaration of intent.

If all is well (the `status_code` of the returned object is 200), then the `json()` method of the returned object will be a dictionary with the same keys that you passed, in addition to `status` (which is `ok`) and `created_at` (which should be roughly the time when you hit the URL0.

### <a name="elasticc2removespecplan"></a> Removing declared intent to take a spectrum

This is *not* necessary if you have succesfully taken a spectrum and use `elasticc2/reportspectruminfo` (below);
the plan will automatically be removed in that case.  However, if declared a plan to take a spectrum with
`elasticc2/planspectrum`, but then it was cloudy and you didn't get actually get the spectrum, it will help our
planning if you remove the plan, no longer indicating that you expect to get the spectrum.

You can remove a spectrum plan at the URL `elasticc2/removespectrumplan`.  The POST body should be a json-encoded ditionary with two keys: `objectid` and `facility`.  The values match the values that you passed to `elasticc2/planspectrum`.  If all is well, you'll get back a dictionary with the information as to what you removed, including a field `n_deleted` that tells you how many rows were actually deleted from the database.


### <a name="elasticc2reportspec"></a> Reporting spectrum information

When you've actually taken a spectrum, it will help us greatly if you tell us about it.  This both lets us know that a spectrum has been taken, and gives us information about type and redshift.  Eventually, we may have additional fields (something about S/N, something about type confidence, perhaps), but for now we're just asking for a redshift and a classid.

Post to URL `elasticc2/reportspectruminfo`.  The POST body should be a json-encoded dict with keys:

* `objectid : int` — the id of the object, the same number that all the previous URLs have used
* `facility : str` — the name of the facility.  If you submitted a plan, this should match the facililty that you sent to `elasticc2/planspectrum`.  (It's OK to report spectra that you didn't declare a plan for ahead of time!)
* `mjd : float` — the mjd of when the spectrum was taken.  (Beginning, middle, or end of exposure, doesn't matter.)
* `z : float` — the redshift of the supernova from the spectrum.  Leave this blank ("") if it cannot be determined.
* `classid : int` — the type from the spectrum.  Use the ELAsTiCC/DESC taxonomy found here: https://github.com/LSSTDESC/elasticc/blob/main/taxonomy/taxonomy.ipynb

If the type is totally indeterminate from the specrum, use type 300 ("NotClassified").  Otherwise, use the most specific type that can be confidently identified from the spectrum.

(TODO: think about allowing mulitiple types and probabilities.)

### <a name="elasticc2getknownspec"></a> Getting information about reported spectra

This is to get what spectrum information has been reported.  Post to URL `elasticc2/getknownspectruminfo`.  The POST body should be a json-encoded dict with keys:

* `objectid : int or list of int` — optional.  If included, only get spectrum information for this object.  (Query multiple objects by passing a list rather than a single int.)
* `facility : str` — optional.  If included, only get spectrum information from this facility.
* `mjd_min : float` — optional.  If included, only get information about spectra taken at this mjd or later.
* `mjd_max : float` — optional.  If included, only get information about spectra taken at this mjd or earlier.
* `classid : int` — optional.  If included, only get information about spectra tagged with this class id.
* `z_min : float` — optional.  If included, only get information about spectra with this redshift or higher.
* `z_max : float` — optional.  If included, only get information about spectra with this redshift or lower.
* `since : str` — optional.  Format "YYYY-MM-DDThh:mm:ss" (where "Thh:mm:ss" may be omitted).  Only get spectra that were reported to the TOM on this date/time (UTC) or later.

If you include no keys, you'll get information about all spectra that the database knows about, which may be overwhelming.  (The API may also time out.)

The response you get back is a json-encoded dictionary, call it `resp`.  `resp['status']` should be `ok`; if it's `error`, then `resp['error']` may have an error message.  (However, if there's an error, you're more likely to get an HTTP 500 response, in which case, look at the body of the response as plain text to see if there's an error message.)  `resp['spectra']` is an array of dictionaries, one for each known spectrum that matched your criteria, ordered by mjd.  Each dictionary in this array has keys:

* `objectid` — The same objectid that you've been using all along
* `facility` — The facility that was reported with the spectrum
* `mjd` — The MJD that the spectrum was reported to be taken at
* `z` — The reported redshift
* `classid` — The reported class id.

---

# Internal Documentation

The rest of this is only interesting if you want to develop it or deploy a private version for hacking.

## Branch Management

The branch `main` has the current production code.

Make a branch `/u/{yourname}/{name}` to do dev work, which (if appropriate) may later be merged into `main`.


## Deploying a dev environment with Docker

If you want to test the TOM out, you can deploy it on your local machine.  If you're lucky, all you need to do is, within the top level of the git checkout:

* Run <code>git submodule update --init --recursive</code>.  There are a number of git submodules that have the standard TOM code.  By default, when you clone, git doesn't clone submodules, so do this in order to make sure all that stuff is there.  (Alternative, if instead of just <code>git clone...</code> you did <code>git clone --recurse-submodules ...</code>, then you've already taken care of this step.)  If you do a <code>git pull</code> later, you either need to do <code>git pull --recurse-submodules</code>, or do <code>git submodule --update --recursive</code> after your pull.</li>

* (Optional) Run <code>docker compose build</code>.  This will be done automatically as part of the next step.  However, you may want to run it manually here, as this step will take a while the first time you do it.  (Once the docker images are built, they'll be cached on your machine.)

* Run <code>docker compose up -d tom</code>.  This will use the <code>docker-compose.yml</code> file to either build or pull three images (the postgres server, the web server, and a version of the web server with additional dev packages), and create three containers.  It will also create a docker volume named "tomdbdata" where postgres stores its contents, so that you can persist the databases from one run of the container to the next.</li>

* Database migrations are applied automatically as part of the docker compose setup, but you need to manually create the TOM superuser account so that you have something to log into. The first time you run it for a given postgres volume, once the containers are up you need to run a shell on the server container with <code>docker compose exec -it tom /bin/bash</code>, and then run the command:

** <code>python manage.py createsuperuser</code> (and answer the prompts)

* If you are using a new postgres data volume (i.e. you're not reusing one from a previous run of docker compose), you need to create the "Public" group.  You need to do this before adding any users.  If all is well, any users added thereafter will automatically be added to this group.  Some of the DESC specific code will break if this group does not exist.  (The TOM documentation seems to imply that this group should have been created automatically, but that doesn't seem to be the case.)  To do this:
``` python manage.py shell
>>> from django.contrib.auth.models import Group
>>> g = Group( name='Public' )
>>> g.save()
>>> exit()
```

At this point, you should be able to connect to your running TOM at `localhost:8080`.


If you ever run a server that exposes its interface to the outside web, you probably want to edit your local version of the file `secrets/django_secret_key`.  Don't commit anything sensitive to git, and especially don't upload it to github!  (There *are* postgres passwords in the github archive, which would seem to voilate this warning.  The reason we're not worried about that is that both in the docker compose file, and as the server is deployed in production, the postgres server is not directly accessible from outside, but only from within the docker environment (or, for production, the Spin namespace). Of course, it would be better to add the additional layer of security of obfuscating those passwords, but, whatever.)

To shut down all the containers you started, just run
```
docker compose down -v
```

If you include the `-v` at the end of that command, it will also destroy the Postgres and Cassandra data volumes you created.  If you *don't* add `-v`, next time you do `docker compose up -d tom`, the information in the database you had from last time around will still be there.

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

Here are three files with a subset of ELAsTiCC2:

(TODO: make sure the numbers listed below are up to date.)

* <a href="https://portal.nersc.gov/cfs/lsst/DESC_TD_PUBLIC/users/raknop/elasticc2_dump_10obj.sql">elasticc2_dump_10obj.sql</a> (128&nbsp;KiB — 10 objects, 97 sources, 2,029 forced sources, 97 alerts (48 sent), 332 broker messages)
* <a href="https://portal.nersc.gov/cfs/lsst/DESC_TD_PUBLIC/users/raknop/elasticc2_dump_100obj.sql">elasticc2_dump_100obj.sql</a> (1.4&nbsp;MiB — 100 objects, 3,226 sources, 26,686 forced sources, 3,226 alerts (990 sent), 6,463 broker messages)
* <a href="https://portal.nersc.gov/cfs/lsst/DESC_TD_PUBLIC/users/raknop/elasticc2_dump_1000obj.sql">elasticc2_dump_1000obj.sql</a> (14&nbsp;MiB — 1000 ojects, 27,888 sources, 230,370 forced sources, 27,888 alerts (10,167 sent), 66,740 broker messages)

They have data from the following tables:
* `elasticc2_ppdbdiaobject`
* `elasticc2_ppdbdiasource`
* `elasticc2_ppdbdiaforcedsource`
* `elasticc2_ppdbalert`
* `elasticc2_diaobjecttruth`
* `elasticc2_brokerclassifier`
* `elasticc2_brokermessage`

The 10, 100, or 1000 objects were chosen randomly.  As of this writing (2023-11-16), ELAsTiCC2 has recently started, so there are many objects in the main database for which no alerts have been sent out; the objects chosen for these subsets have at least one alert sent.  (The files will be regenerated after ELAsTiCC2 is over.)  All sources, forced sources, and alerts (including unsent ones) for those objects are included, as are all broker messages we've received in response to one of the alerts included.

Save the file you download into the `tom_desc/admin_tools` subdirectory if your git checkout.  (This directory already exists.)

For various reasons, the docker image for the Tom server is based on an older version (Chimaera) of the Linux distribution (Devuan).  The postgres image is based on Daedalus, which as of this writing is the current stable version of Devuan.  The restoration process requires `pg_restore` to have a version that's compatible with the postgres server, and for that reason you need to run a special shell just for this restoration process.  Start that shell with
```
docker compose up -d daedalus-shell
```
and then go into the shell with
```
docker compose exec -it daedalus-shell /bin/bash
```
cd into the directory `admin_tools` and run:
```
python import_elasticc2_subset_dump.py -f <filename>
```
where `<filename>` is the .sql file that you downloaded.

If all is well, when you're done run
```
docker compose down daedalus-shell
```

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

#### <a name="prodscratch"></a> The steps necessary to create the production TOM from scratch:

If you're making a new deployment somewhere (rather than just recreating
desc_tom on the Spin production server), you *will* need to edit the YAML files before applying them
(so that things like "namespace" and "workloadselector" are consistent
with everything else you're doing).  Please keep the YAML files in the top `spin_admin` directory which committed to the git archive as the ones necessary for the default production installation.

After each step, it's worth running a `rancher kubectl get...` command to make sure that the thing you created or started is working.  There are lots of reasons why things can fail, some of which aren't entirely under your control (e.g. servers that docker images are pulled from).

- Create the two persistent volume claims.  There are two files, `tom-postgres-pvc.yaml` and `tom-cassandra-pvc.yaml` that describe these.  Be very careful with these files, as you stand a chance of blowing away the existing TOM database if you do the wrong thing.

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

## <a name="noteselasticc"></a> ELASTICC

Since the first ELAsTiCC campaign is long over, the realtime stuff here is not relevant any more.

### <a name="elasticcstream"></a> Streaming to ZADS

This is in the `LSSTDESC/elasticc` archive, under the `stream-to-zads` directory.  The script `stream-to-zads.py` is designed to run in a Spin container; it reads alerts from where they are on disk, and based on the directory names of the alerts (which are linked to dates), and configuration, figures out what it needs to send

### <a name="elasticcpull"></a> Pulling from brokers

The django management command `elasticc/management/commands/brokerpoll.py` handled the broker polling.  It ran in its own Spin container with the same Docker image as the main tom web server (but did not open a webserver port to the outside world).


## <a name="noteselasticc2"></a> ELASTICC2

### <a name="elasticc2stream"></a> Streaming to ZADS

The django management command `elasticc2/management/commands/send_elasticc2_alerts.py` is able to construct avro alerts from the tables in the database, and send those avro alerts on to a kafka server.  This command is run in the cron job described by `spin_admin/tom-send-alerts-cron.yaml`.

### <a name="elasticc2fakebroker"></a> Fake broker

In the `LSSTDESC/elasticc` archive, under the `stream-to-zads` directory, there is a script `fakebroker.py`.  This is able to read ELaSTiCC alerts from one kafka server, construct broker messages (which are the right structure, but have no real thought behind the classifications), and send those broker messages on to another kafka server.  It's run as aprt of the test suite (see below).


## <a name="elasticctesting"> Testing

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

### <a name="elasticctestbuild"></a> Building the framework

Make sure all of the necessary images are built on your system by moving into the `tests` subdirectory and running:
```
docker compose build
```

You may also need to extract the test data used by some of the tests.  In the `tests` subdirectory, there is a file `elasticc2_alert_test_data.tar.bz2` that has SNANA FITS files with a test dataset that some of the tests (e.g. `test_alertcycle.py`) use.  You must extract this dataset with
```
tar -xvf elasticc2_alert_test_data.tar.bz2
```
to make it available.  Once you've done it (if there are about 3M of files in the directory `elasticc2_alert_test_data`), you don't have to do it again.

### <a name="elasticctestshell"></a> Running a shell in the framework

To get an environment in which to run the tests manually, run
```
docker compose up -d shellhost
```

You can then connect to that shell host with `docker compose exec -it shellhost /bin/bash`.  The postgres server will on the host named `postgres` (so you can `psql -h postgres -U postgres tom_desc` to poke at the database— the password is "fragile", which you can see in the `docker-compose.yaml` file).  The web server will be on the host named `tom`, listening on port 8080 without SSL  (so, you could do `netcat tom 8080`, or write a python script that uses requests to connect to `http://tom:8080`... or, for that matter, use the `tom_client.py` file included in the `tests` directory).

When you're done with everything, do
```
docker compose down
```

or

```
docker compose down -v
```

if you also want to delete the created volumes (which you should if you're running tests, so the next run will start with a clean environment).

### <a name="elasticctestauto"></a> Automatically running all the tests

__This may not work right now.  I haven't done it in a long time, I've just manually run individual tests with pytest commands in a shellhost as described above.__

In the `tests` directory (on your machine, _not_ inside a container), after having built the framework and (if necessary) unpacked `elasticc2_alert_test_data.tar.bz`, do:
```
docker compose run runtests
```

After the tests complete (which could take a long time), do
```
docker compose down -v
```
to clean up.  (If you don't do this, the next time you try to run the tests, it won't have a clean slate and the startup will fail.)
