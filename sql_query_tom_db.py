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
    username = "root"
    with open( os.path.join( os.getenv("HOME"), "secrets", "tom_root_passwd" ) ) as ifp:
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
    query = 'SELECT * FROM stream_elasticcdiasource WHERE "diaObject_id"=%(id)s'
    subdict = { "id": 7055558 }
    result = rqs.post( f'{url}/stream/runsqlquery', headers=csrfheader,
                       json={ 'query': query, 'subdict': subdict } )


    # Look at the response.  It will be a JSON encoded dict with two fields:
    #  { 'status': 'ok',
    #    'rows': [...] }
    # where rows has the rows returned by the SQL query; each element of the row
    # is a dict.  There's probably a more efficient way to return this.  I'll
    # add formatting parameters later.

    if result.status_code != 200:
        sys.stderr.write( f"ERROR: got status code {result.status_code} ({result.reason})\n" )
    else:
        data = json.loads( result.text )
        if ( 'status' not in data ) or ( data['status'] != 'ok' ):
            sys.stderr.write( "Got unexpected response" )
        else:
            for row in data['rows']:
                print( f'Object: {row["diaObject_id"]}, Source: {row["diaSourceId"]}, Flux: {row["apFlux"]}' )

# ======================================================================
if __name__ == "__main__":
    main()


# ======================================================================
# Some of the tables as of 2022-02-07

#                  Table "public.stream_elasticcdiaobject"
#         Column        |       Type       | Collation | Nullable | Default
# ----------------------+------------------+-----------+----------+---------
#  diaObjectId          | bigint           |           | not null |
#  ra                   | double precision |           |          |
#  decl                 | double precision |           |          |
#  hostgal2_dec         | double precision |           |          |
#  hostgal2_ellipticity | double precision |           |          |
#  hostgal2_mag_Y       | double precision |           |          |
#  hostgal2_mag_g       | double precision |           |          |
#  hostgal2_mag_i       | double precision |           |          |
#  hostgal2_mag_r       | double precision |           |          |
#  hostgal2_mag_u       | double precision |           |          |
#  hostgal2_mag_z       | double precision |           |          |
#  hostgal2_magerr_Y    | double precision |           |          |
#  hostgal2_magerr_g    | double precision |           |          |
#  hostgal2_magerr_i    | double precision |           |          |
#  hostgal2_magerr_r    | double precision |           |          |
#  hostgal2_magerr_u    | double precision |           |          |
#  hostgal2_magerr_z    | double precision |           |          |
#  hostgal2_ra          | double precision |           |          |
#  hostgal2_snsep       | double precision |           |          |
#  hostgal2_sqradius    | double precision |           |          |
#  hostgal2_z           | double precision |           |          |
#  hostgal2_z_err       | double precision |           |          |
#  hostgal2_zphot_pz50  | double precision |           |          |
#  hostgal2_zphot_q10   | double precision |           |          |
#  hostgal2_zphot_q20   | double precision |           |          |
#  hostgal2_zphot_q30   | double precision |           |          |
#  hostgal2_zphot_q40   | double precision |           |          |
#  hostgal2_zphot_q50   | double precision |           |          |
#  hostgal2_zphot_q60   | double precision |           |          |
#  hostgal2_zphot_q70   | double precision |           |          |
#  hostgal2_zphot_q80   | double precision |           |          |
#  hostgal2_zphot_q90   | double precision |           |          |
#  hostgal_dec          | double precision |           |          |
#  hostgal_mag_Y        | double precision |           |          |
#  hostgal_mag_g        | double precision |           |          |
#  hostgal_mag_i        | double precision |           |          |
#  hostgal_mag_r        | double precision |           |          |
#  hostgal_mag_u        | double precision |           |          |
#  hostgal_mag_z        | double precision |           |          |
#  hostgal_magerr_Y     | double precision |           |          |
#  hostgal_magerr_g     | double precision |           |          |
#  hostgal_magerr_i     | double precision |           |          |
#  hostgal_magerr_r     | double precision |           |          |
#  hostgal_magerr_u     | double precision |           |          |
#  hostgal_magerr_z     | double precision |           |          |
#  hostgal_ra           | double precision |           |          |
#  hostgal_snsep        | double precision |           |          |
#  hostgal_sqradius     | double precision |           |          |
#  hostgal_z            | double precision |           |          |
#  hostgal_z_err        | double precision |           |          |
#  hostgal_zphot_pz50   | double precision |           |          |
#  hostgal_zphot_q10    | double precision |           |          |
#  hostgal_zphot_q20    | double precision |           |          |
#  hostgal_zphot_q30    | double precision |           |          |
#  hostgal_zphot_q40    | double precision |           |          |
#  hostgal_zphot_q50    | double precision |           |          |
#  hostgal_zphot_q60    | double precision |           |          |
#  hostgal_zphot_q70    | double precision |           |          |
#  hostgal_zphot_q80    | double precision |           |          |
#  hostgal_zphot_q90    | double precision |           |          |
#  hostgal_ellipticity  | double precision |           |          |
# Indexes:
#     "stream_elasticcdiaobject_pkey" PRIMARY KEY, btree ("diaObjectId")
#     "elasticcdiaobject_q3c_radec_idx" btree (q3c_ang2ipix(ra, decl))
# Referenced by:
#     TABLE "stream_elasticcdiasource" CONSTRAINT "stream_elasticcdiaso_diaObject_id_2fbc5706_fk_stream_el" FOREIGN KEY ("
# diaObject_id") REFERENCES stream_elasticcdiaobject("diaObjectId") DEFERRABLE INITIALLY DEFERRED


#                 Table "public.stream_elasticcdiasource"
#       Column       |       Type       | Collation | Nullable | Default
# -------------------+------------------+-----------+----------+---------
#  diaSourceId       | bigint           |           | not null |
#  midPointTai       | double precision |           | not null |
#  filterName        | text             |           | not null |
#  ra                | double precision |           |          |
#  decl              | double precision |           |          |
#  psFlux            | double precision |           | not null |
#  psFluxErr         | double precision |           | not null |
#  nobs              | double precision |           |          |
#  diaObject_id      | bigint           |           |          |
#  ccdVisitId        | bigint           |           |          |
#  parentDiaSourceId | bigint           |           |          |
#  snr               | double precision |           |          |
# Indexes:
#     "stream_elasticcdiasource_pkey" PRIMARY KEY, btree ("diaSourceId")
#     "elasticcdiasource_q3c_radec_idx" btree (q3c_ang2ipix(ra, decl))
#     "stream_elasticcdiasource_diaObject_id_2fbc5706" btree ("diaObject_id")
# Foreign-key constraints:
#     "stream_elasticcdiaso_diaObject_id_2fbc5706_fk_stream_el" FOREIGN KEY ("diaObject_id") REFERENCES stream_elasticcdiaobject("diaObjectId") DEFERRABLE INITIALLY DEFERRED
# Referenced by:
#     TABLE "stream_elasticcbrokerclassification" CONSTRAINT "stream_elasticcbroke_diaSource_id_e266c51f_fk_stream_el" FOREIGN KEY ("diaSource_id") REFERENCES stream_elasticcdiasource("diaSourceId") DEFERRABLE INITIALLY DEFERRED

#                                     Table "public.stream_elasticcdiatruth"
#     Column    |       Type       | Collation | Nullable |                       Default                       
# --------------+------------------+-----------+----------+-----------------------------------------------------
#  id           | bigint           |           | not null | nextval('stream_elasticcdiatruth_id_seq'::regclass)
#  diaSourceId  | bigint           |           |          | 
#  diaObjectId  | bigint           |           |          | 
#  detect       | boolean          |           |          | 
#  true_gentype | integer          |           |          | 
#  true_genmag  | double precision |           |          | 
# Indexes:
#     "stream_elasticcdiatruth_pkey" PRIMARY KEY, btree (id)
#     "stream_elas_diaObje_e18552_idx" btree ("diaObjectId")
#     "stream_elas_diaSour_e926b6_idx" btree ("diaSourceId")
