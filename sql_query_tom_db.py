import sys
import os
import requests
import json

def main():
    # Put in your TOM username and password here.  What I have here
    # reads the password from a private directory, so this code will not
    # work as is for you.  You just need to set the username and
    # password variables for use in the rqs.post call below.
    
    url = "https://desc-tom.lbl.gov"
    # url = "https://desc-tom-rknop-dev.lbl.gov"
    username = "rknop"
    with open( os.path.join( os.getenv("HOME"), "secrets", "tom_rknop_passwd" ) ) as ifp:
        password = ifp.readline().strip()

    # First, log in.  Because of Django's prevention against cross-site
    # scripting attacks, there is some dancing about you have to do in
    # order to make sure it always gets the right "csrftoken" header.

    rqs = requests.session()
    rqs.get( f'{url}/accounts/login/' )
    rqs.post( f'{url}/accounts/login/',
              data={ "username": username,
                     "password": password,
                     "csrfmiddlewaretoken": rqs.cookies['csrftoken'] } )
    # Check login success here....
    # This is nontrivial, since the text of the response will be
    # HTML intended for rendering in a browser.
    csrfheader = { 'X-CSRFToken': rqs.cookies['csrftoken'] }

    # Next, send your query, passing the csrfheader with each request
    # You send the query as a json-encoded dictionary with two fields:
    #   'query' : the SQL query, with %(name)s for things that should
    #                be substituted.  (This is standard psycopg2.)
    #   'subdict' : a dictionary of substitutions for %(name)s things in your query
    #
    # The backend is to this web API call is readonly, so you can't
    # bobby tables this.  However, this does give you the freedom to
    # read anything from the tables if you know the schema.
    # (Some relevant schema are at the bottom.)
    
    # Note that I have to double-quote the column name beacuse otherwise
    #  Postgres converts things to lc for some reason or another.
    query = 'SELECT * FROM elasticc_diasource WHERE "diaObject_id"=%(id)s ORDER BY "midPointTai"'
    subdict = { "id": 1024 }
    result = rqs.post( f'{url}/db/runsqlquery/', headers=csrfheader,
                       json={ 'query': query, 'subdict': subdict } )


    # Look at the response.  It will be a JSON encoded dict with two fields:
    #  { 'status': 'ok',
    #    'rows': [...] }
    # where rows has the rows returned by the SQL query; each element of the row
    # is a dict.  There's probably a more efficient way to return this.  I'll
    # add formatting parameters later -- for instance, it might be nice to be able
    # to get a serialized pandas DataFrame, which then skips the binary-to-text-to-binary
    # translation that going through JSON will do.

    if result.status_code != 200:
        sys.stderr.write( f"ERROR: got status code {result.status_code} ({result.reason})\n" )
    else:
        data = json.loads( result.text )
        if ( 'status' not in data ) or ( data['status'] != 'ok' ):
            sys.stderr.write( "Got unexpected response" )
        else:
            for row in data['rows']:
                print( f'Object: {row["diaObject_id"]:<20d}, Source: {row["diaSourceId"]:<20d}, '
                       f'MJD: {row["midPointTai"]:9.2f}, Flux: {row["psFlux"]:10.2f}' )

# ======================================================================
if __name__ == "__main__":
    main()


# ======================================================================
# Some of the tables as of 2022-03-29
#
# (There are some indexes and foreign keys on some of these tables that
# aren't shown below.  Some of the foreign keys you could probably
# guess, e.g. diaAlert_id in elasticc_diaalertprvsource is a foreign key
# for alertId in elasticc_diaalert.  Let me know if you want that
# information.)

# Table "public.elasticc_diaalert"
#     Column    |  Type  | Collation | Nullable | Default 
# --------------+--------+-----------+----------+---------
#  alertId      | bigint |           | not null | 
#  diaObject_id | bigint |           |          | 
#  diaSource_id | bigint |           |          | 
# 
# 
# Table "public.elasticc_diaobject"
#         Column        |       Type       | Collation | Nullable | Default 
# ----------------------+------------------+-----------+----------+---------
#  diaObjectId          | bigint           |           | not null | 
#  simVersion           | text             |           |          |
#  ra                   | double precision |           | not null | 
#  decl                 | double precision |           | not null | 
#  mwebv                | double precision |           |          | 
#  mwebv_err            | double precision |           |          | 
#  z_final              | double precision |           |          | 
#  z_final_err          | double precision |           |          | 
#  hostgal_ellipticity  | double precision |           |          | 
#  hostgal_sqradius     | double precision |           |          | 
#  hostgal_z            | double precision |           |          | 
#  hostgal_z_err        | double precision |           |          | 
#  hostgal_zphot_q10    | double precision |           |          | 
#  hostgal_zphot_q20    | double precision |           |          | 
#  hostgal_zphot_q30    | double precision |           |          | 
#  hostgal_zphot_q40    | double precision |           |          | 
#  hostgal_zphot_q50    | double precision |           |          | 
#  hostgal_zphot_q60    | double precision |           |          | 
#  hostgal_zphot_q70    | double precision |           |          | 
#  hostgal_zphot_q80    | double precision |           |          | 
#  hostgal_zphot_q90    | double precision |           |          | 
#  hostgal_zphot_q99    | double precision |           |          | 
#  hostgal_mag_u        | double precision |           |          | 
#  hostgal_mag_g        | double precision |           |          | 
#  hostgal_mag_r        | double precision |           |          | 
#  hostgal_mag_i        | double precision |           |          | 
#  hostgal_mag_z        | double precision |           |          | 
#  hostgal_mag_Y        | double precision |           |          | 
#  hostgal_ra           | double precision |           |          | 
#  hostgal_dec          | double precision |           |          | 
#  hostgal_snsep        | double precision |           |          | 
#  hostgal_magerr_u     | double precision |           |          | 
#  hostgal_magerr_g     | double precision |           |          | 
#  hostgal_magerr_r     | double precision |           |          | 
#  hostgal_magerr_i     | double precision |           |          | 
#  hostgal_magerr_z     | double precision |           |          | 
#  hostgal_magerr_Y     | double precision |           |          | 
#  hostgal2_ellipticity | double precision |           |          | 
#  hostgal2_sqradius    | double precision |           |          | 
#  hostgal2_z           | double precision |           |          | 
#  hostgal2_z_err       | double precision |           |          | 
#  hostgal2_zphot_q10   | double precision |           |          | 
#  hostgal2_zphot_q20   | double precision |           |          | 
#  hostgal2_zphot_q30   | double precision |           |          | 
#  hostgal2_zphot_q40   | double precision |           |          | 
#  hostgal2_zphot_q50   | double precision |           |          | 
#  hostgal2_zphot_q60   | double precision |           |          | 
#  hostgal2_zphot_q70   | double precision |           |          | 
#  hostgal2_zphot_q80   | double precision |           |          | 
#  hostgal2_zphot_q90   | double precision |           |          | 
#  hostgal2_zphot_q99   | double precision |           |          | 
#  hostgal2_mag_u       | double precision |           |          | 
#  hostgal2_mag_g       | double precision |           |          | 
#  hostgal2_mag_r       | double precision |           |          | 
#  hostgal2_mag_i       | double precision |           |          | 
#  hostgal2_mag_z       | double precision |           |          | 
#  hostgal2_mag_Y       | double precision |           |          | 
#  hostgal2_ra          | double precision |           |          | 
#  hostgal2_dec         | double precision |           |          | 
#  hostgal2_snsep       | double precision |           |          | 
#  hostgal2_magerr_u    | double precision |           |          | 
#  hostgal2_magerr_g    | double precision |           |          | 
#  hostgal2_magerr_r    | double precision |           |          | 
#  hostgal2_magerr_i    | double precision |           |          | 
#  hostgal2_magerr_z    | double precision |           |          | 
#  hostgal2_magerr_Y    | double precision |           |          | 
# 
# 
# Table "public.elasticc_diasource"
#       Column       |       Type       | Collation | Nullable | Default 
# -------------------+------------------+-----------+----------+---------
#  diaSourceId       | bigint           |           | not null | 
#  ccdVisitId        | bigint           |           | not null | 
#  parentDiaSourceId | bigint           |           |          | 
#  midPointTai       | double precision |           | not null | 
#  filterName        | text             |           | not null | 
#  ra                | double precision |           | not null | 
#  decl              | double precision |           | not null | 
#  psFlux            | double precision |           | not null | 
#  psFluxErr         | double precision |           | not null | 
#  snr               | double precision |           | not null | 
#  nobs              | double precision |           |          | 
#  diaObject_id      | bigint           |           |          | 
# 
# 
# Table "public.elasticc_diaforcedsource"
#       Column       |       Type       | Collation | Nullable | Default 
# -------------------+------------------+-----------+----------+---------
#  diaForcedSourceId | bigint           |           | not null | 
#  ccdVisitId        | bigint           |           | not null | 
#  midPointTai       | double precision |           | not null | 
#  filterName        | text             |           | not null | 
#  psFlux            | double precision |           | not null | 
#  psFluxErr         | double precision |           | not null | 
#  totFlux           | double precision |           | not null | 
#  totFluxErr        | double precision |           | not null | 
#  diaObject_id      | bigint           |           | not null | 
# 
# 
# Table "public.elasticc_diaalertprvsource"
#     Column    |  Type  | Collation | Nullable |                        Default                         
# --------------+--------+-----------+----------+--------------------------------------------------------
#  id           | bigint |           | not null | nextval('elasticc_diaalertprvsource_id_seq'::regclass)
#  diaAlert_id  | bigint |           |          | 
#  diaSource_id | bigint |           |          | 
# 
# 
# Table "public.elasticc_diaalertprvforcedsource"
#        Column       |  Type  | Collation | Nullable | Default                            
# --------------------+--------+-----------+----------+------------
#  id                 | bigint |           | not null | nextval(...
#  diaAlert_id        | bigint |           |          | 
#  diaForcedSource_id | bigint |           |          | 
# 
# 
# Table "public.elasticc_diatruth"
#     Column    |       Type       | Collation | Nullable |                    Default                    
# --------------+------------------+-----------+----------+-----------------------------------------------
#  id           | bigint           |           | not null | nextval('elasticc_diatruth_id_seq'::regclass)
#  diaSourceId  | bigint           |           |          | 
#  diaObjectId  | bigint           |           |          | 
#  detect       | boolean          |           |          | 
#  true_gentype | integer          |           |          | 
#  true_genmag  | double precision |           |          | 
#
# 
# Table "public.elasticc_brokermessage"
#           Column          |           Type           | Collation | Nullable | Default
# --------------------------+--------------------------+-----------+----------+--------
#  dbMessageIndex           | bigint                   |           | not null | nextval(...
#  streamMessageId          | bigint                   |           |          | 
#  topicName                | character varying(200)   |           |          | 
#  alertId                  | bigint                   |           | not null | 
#  diaSourceId              | bigint                   |           | not null | 
#  descIngestTimestamp      | timestamp with time zone |           | not null | 
#  elasticcPublishTimestamp | timestamp with time zone |           |          | 
#  brokerIngestTimestamp    | timestamp with time zone |           |          | 
#  modified                 | timestamp with time zone |           | not null | 
# 
# 
# Table "public.elasticc_brokerclassifier"
#       Column       |           Type           | Collation | Nullable | Default
# -------------------+--------------------------+-----------+----------+--------
#  dbClassifierIndex | bigint                   |           | not null | nextval(...
#  brokerName        | character varying(100)   |           | not null | 
#  brokerVersion     | text                     |           |          | 
#  classifierName    | character varying(200)   |           | not null | 
#  classifierParams  | text                     |           |          | 
#  modified          | timestamp with time zone |           | not null | 
# 
# 
# Table "public.elasticc_brokerclassification"
#         Column         |           Type           | Collation | Nullable | Default
# -----------------------+--------------------------+-----------+----------+--------
#  dbClassificationIndex | bigint                   |           | not null | nextval(...
#  classId               | integer                  |           | not null | 
#  probability           | double precision         |           | not null | 
#  modified              | timestamp with time zone |           | not null | 
#  dbClassifier_id       | bigint                   |           |          | 
#  dbMessage_id          | bigint                   |           |          | 

