import sys
import os
import requests
import json

# This is a utility function that we use below.  It shows how
# you actually query the databse, once it's set up.
# See the main() function below.
def send_query( rqs, query, subdict, url="https://desc-tom.lbl.gov" ):
    """Send a SQL query to the TOM.

    rqs - a python requests session (returned by requests.session())
          must already be logged into the TOM
    query - The query as expected by psycopg2
    subdict - The substitution dictionary as expected by psycopg2
    url - The base url of the TOM (default: "https://desc-tom.lbl.gov")

    Returns: An array of rows returned from the database.  Each row is a
    dict of column->value.

    Raises exceptions: RunTimeError if anything goes wrong.  This includes
    if the HTTP request went just fine, but there was an error response
    from the database; in that case, the text of the RunTimeError is
    the error text returned from the server.
    """

    try:
        result = rqs.post( f'{url}/db/runsqlquery/',
                           json={ 'query': query, 'subdict': subdict } )
    except Exception as e:
        raise RuntimeError( f'HTTP call failed: {str(e)}' )

    if result.status_code != 200:
        raise RuntimeError( f"Error, got status {result.status_code} from HTTP call" )
    data = json.loads( result.text )
    if ( 'status' in data ) and ( data['status'] == 'error' ):
        raise RuntimeError( data['error'] )
    elif ( 'status' not in data ) or ( data['status'] != 'ok' ):
        raise RuntimeError( f"Got unexpected response from server: {json.dumps(data,indent=4)}" )
    return data['rows']
    

def main():
    # Put in your TOM username and password here.  What I have here
    # reads the password from a private directory, so this code will not
    # work as is for you.  You just need to set the username and
    # password variables for use in the rqs.post call below.
    
    # url = "https://desc-tom.lbl.gov"
    url = "https://desc-tom-rknop-dev.lbl.gov"
    username = "rknop"
    with open( os.path.join( os.getenv("HOME"), "secrets", "tom_rknop_passwd" ) ) as ifp:
        password = ifp.readline().strip()

    # First, log in.  Because of Django's prevention against cross-site
    # scripting attacks, there is some dancing about you have to do in
    # order to make sure it always gets the right "csrftoken" header.

    rqs = requests.session()
    rqs.get( f'{url}/accounts/login/' )
    res = rqs.post( f'{url}/accounts/login/',
                    data={ "username": username,
                           "password": password,
                           "csrfmiddlewaretoken": rqs.cookies['csrftoken'] } )
    if res.status_code != 200:
        raise RuntimeError( f"Failed to log in; http status: {res.status_code}" )
    if 'Please enter a correct' in res.text:
        # This is a very cheesy attempt at checking if the login failed.
        # I haven't found clean documentation on how to log into a django site
        # from an app like this using standard authentication stuff.  So, for
        # now, I'm counting on the HTML that happened to come back when
        # I ran it with a failed login one time.  One of these days I'll actually
        # figure out how Django auth works and make a version of /accounts/login/
        # designed for use in API scripts like this one, rather than desgined
        # for interactive users.
        raise RuntimeError( "Failed to log in.  I think.  Put in a debug break and look at res.text" )
    rqs.headers.update( { 'X-CSRFToken': rqs.cookies['csrftoken'] } )

    # Next, send your query, passing the csrfheader with each request.
    # (The rqs.headers.update call above makes that happen if you just
    # keep using the rqs object.)
    #
    # You send the query as a json-encoded dictionary with two fields:
    #   'query' : the SQL query, with %(name)s for things that should
    #                be substituted.  (This is standard psycopg2.)
    #   'subdict' : a dictionary of substitutions for %(name)s things in your query
    #
    # The backend is to this web API call is readonly, so you can't
    # bobby tables this. Only elasticc admins are able to read most of
    # the tables.  Anybody with an account can read the elasticc_broker*
    # tables.
    #
    # (Some relevant schema are at the bottom.)
    
    # Note that I have to double-quote the column name beacuse otherwise
    #  Postgres converts things to lc for some reason or another.
    query = 'SELECT * FROM elasticc_view_sourceclassifications ORDER BY "msgHdrTimestamp" LIMIT %(num)s'
    subdict = { 'num': 10 }
    rows = send_query( rqs, query, subdict, url=url )

    # If all goes well, rows has the rows returned by the SQL query;
    # each element of the row is a dict.  There's probably a more
    # efficient way to return this.  I'll add formatting parameters
    # later -- for instance, it might be nice to be able to get a
    # serialized pandas DataFrame, which then skips the
    # binary-to-text-to-binary translation that going through JSON will
    # do.

    print( 'brokerName        brokerVersion  classifierName    classId  trueClassId  msgHdrTimestamp' )
    print( '----------        -------------  --------------    -------  -----------  ---------------' )
    for row in rows:
        print( f'{row["brokerName"]:<16s}  '
               f'{row["brokerVersion"]:<13s}  '
               f'{row["classifierName"]:<16s}  '
               f'{row["classId"]:7d}  '
               f'{row["trueClassId"]:11d}  '
               f'{row["msgHdrTimestamp"]}' )

# ======================================================================
if __name__ == "__main__":
    main()


# ======================================================================
# elasticc tables as of 2022-08-19
#
# The order of the columns is what happens to be in the database, as a
# result of the specific history of django databse migrations.  It's not
# a sane order, alas.  You can find the same schema in the django source
# code, where the columns are in a more sane order; look at
# https://github.com/LSSTDESC/tom_desc/blob/main/elasticc/models.py

# NOTE REGARDING CLASSIFICATIONS: it's complicated.  See ...

# ----------------------------------------------------------------------
#
# Table "public.elasticc_brokermessage"
#           Column          |           Type           | Collation | Nullable |                             Default                              
# --------------------------+--------------------------+-----------+----------+------------------------------------------------------------------
#  brokerMessageId          | bigint                   |           | not null | nextval('"elasticc_brokermessage_dbMessageIndex_seq"'::regclass)
#  streamMessageId          | bigint                   |           |          | 
#  topicName                | character varying(200)   |           |          | 
#  alertId                  | bigint                   |           | not null | 
#  diaSourceId              | bigint                   |           | not null | 
#  descIngestTimestamp      | timestamp with time zone |           | not null | 
#  elasticcPublishTimestamp | timestamp with time zone |           |          | 
#  brokerIngestTimestamp    | timestamp with time zone |           |          | 
#  modified                 | timestamp with time zone |           | not null | 
#  msgHdrTimestamp          | timestamp with time zone |           |          | 
# Indexes:
#     "elasticc_brokermessage_pkey" PRIMARY KEY, btree ("brokerMessageId")
#     "elasticc_br_alertId_b419c9_idx" btree ("alertId")
#     "elasticc_br_diaSour_ca3044_idx" btree ("diaSourceId")
#     "elasticc_br_topicNa_73f5a4_idx" btree ("topicName", "streamMessageId")
# Referenced by:
#     TABLE "elasticc_brokerclassification" CONSTRAINT "elasticc_brokerclass_brokerMessageId_c3920691_fk_elasticc_" FOREIGN KEY ("brokerMessageId") REFERENCES elasticc_brokermessage("brokerMessageId") DEFERRABLE INITIALLY DEFERRED

# ----------------------------------------------------------------------
#
# Table "public.elasticc_brokerclassifier"
#       Column      |           Type           | Collation | Nullable |                                Default                                 
# ------------------+--------------------------+-----------+----------+------------------------------------------------------------------------
#  classifierId     | bigint                   |           | not null | nextval('"elasticc_brokerclassifier_dbClassifierIndex_seq"'::regclass)
#  brokerName       | character varying(100)   |           | not null | 
#  brokerVersion    | text                     |           |          | 
#  classifierName   | character varying(200)   |           | not null | 
#  classifierParams | text                     |           |          | 
#  modified         | timestamp with time zone |           | not null | 
# Indexes:
#     "elasticc_brokerclassifier_pkey" PRIMARY KEY, btree ("classifierId")
#     "elasticc_br_brokerN_38d99f_idx" btree ("brokerName", "classifierName")
#     "elasticc_br_brokerN_86cc1a_idx" btree ("brokerName")
#     "elasticc_br_brokerN_eb7553_idx" btree ("brokerName", "brokerVersion")
# Referenced by:
#     TABLE "elasticc_brokerclassification" CONSTRAINT "elasticc_brokerclass_classifierId_f49719e3_fk_elasticc_" FOREIGN KEY ("classifierId") REFERENCES elasticc_brokerclassifier("classifierId") DEFERRABLE INITIALLY DEFERRED

# ----------------------------------------------------------------------
#
# Table "public.elasticc_brokerclassification"
#       Column      |           Type           | Collation | Nullable |                                    Default                                     
# ------------------+--------------------------+-----------+----------+--------------------------------------------------------------------------------
#  classificationId | bigint                   |           | not null | nextval('"elasticc_brokerclassification_dbClassificationIndex_seq"'::regclass)
#  classId          | integer                  |           | not null | 
#  probability      | double precision         |           | not null | 
#  modified         | timestamp with time zone |           | not null | 
#  brokerMessageId  | bigint                   |           |          | 
#  classifierId     | bigint                   |           |          | 
# Indexes:
#     "elasticc_brokerclassification_pkey" PRIMARY KEY, btree ("classificationId")
#     "elasticc_brokerclassification_classifierId_f49719e3" btree ("classifierId")
#     "elasticc_brokerclassification_dbMessage_id_b8bd04da" btree ("brokerMessageId")
# Foreign-key constraints:
#     "elasticc_brokerclass_brokerMessageId_c3920691_fk_elasticc_" FOREIGN KEY ("brokerMessageId") REFERENCES elasticc_brokermessage("brokerMessageId") DEFERRABLE INITIALLY DEFERRED
#     "elasticc_brokerclass_classifierId_f49719e3_fk_elasticc_" FOREIGN KEY ("classifierId") REFERENCES elasticc_brokerclassifier("classifierId") DEFERRABLE INITIALLY DEFERRED

# ----------------------------------------------------------------------
#
# View "public.elasticc_view_sourceclassifications"
#           Column          |           Type           | Collation | Nullable | Default 
# --------------------------+--------------------------+-----------+----------+---------
#  classifierId             | bigint                   |           |          | 
#  brokerName               | character varying(100)   |           |          | 
#  brokerVersion            | text                     |           |          | 
#  classifierName           | character varying(200)   |           |          | 
#  classifierParams         | text                     |           |          | 
#  classId                  | integer                  |           |          | 
#  probability              | double precision         |           |          | 
#  diaSourceId              | bigint                   |           |          | 
#  diaObjectId              | bigint                   |           |          | 
#  gentype                  | integer                  |           |          | 
#  trueClassId              | integer                  |           |          | 
#  alertId                  | bigint                   |           |          | 
#  brokerMessageId          | bigint                   |           |          | 
#  alertSentTimestamp       | timestamp with time zone |           |          | 
#  elasticcPublishTimestamp | timestamp with time zone |           |          | 
#  brokerIngestTimestamp    | timestamp with time zone |           |          | 
#  msgHdrTimestamp          | timestamp with time zone |           |          | 
#  descIngestTimestamp      | timestamp with time zone |           |          | 

# ----------------------------------------------------------------------
#
# Table "public.elasticc_classidofgentype"
#     Column     |  Type   | Collation | Nullable |                        Default                        
# ---------------+---------+-----------+----------+-------------------------------------------------------
#  id            | integer |           | not null | nextval('elasticc_classidofgentype_id_seq'::regclass)
#  gentype       | integer |           | not null | 
#  classId       | integer |           | not null | 
#  exactmatch    | boolean |           | not null | 
#  categorymatch | boolean |           | not null | 
#  description   | text    |           | not null | 
# Indexes:
#     "elasticc_classidofgentype_pkey" PRIMARY KEY, btree (id)
#     "elasticc_classidofgentype_classId_40baddb0" btree ("classId")
#     "elasticc_classidofgentype_gentype_93da1db4" btree (gentype)

# ----------------------------------------------------------------------
#
# Table "public.elasticc_gentypeofclassid"
#    Column    |  Type   | Collation | Nullable |                        Default                         
# -------------+---------+-----------+----------+--------------------------------------------------------
#  id          | integer |           | not null | nextval('elasticc_classificationmap_id_seq'::regclass)
#  classId     | integer |           | not null | 
#  gentype     | integer |           |          | 
#  description | text    |           | not null | 
# Indexes:
#     "elasticc_classificationmap_pkey" PRIMARY KEY, btree (id)
#     "elasticc_classificationmap_classId_27acf0d1" btree ("classId")
#     "elasticc_classificationmap_snana_gentype_63c19152" btree (gentype)

# ----------------------------------------------------------------------
#
# Table "public.elasticc_diaalert"
#        Column       |           Type           | Collation | Nullable | Default 
# --------------------+--------------------------+-----------+----------+---------
#  alertId            | bigint                   |           | not null | 
#  diaObjectId        | bigint                   |           |          | 
#  diaSourceId        | bigint                   |           |          | 
#  alertSentTimestamp | timestamp with time zone |           |          | 
# Indexes:
#     "elasticc_diaalert_pkey" PRIMARY KEY, btree ("alertId")
#     "elasticc_diaalert_diaObject_id_809a8089" btree ("diaObjectId")
#     "elasticc_diaalert_diaSource_id_1f178060" btree ("diaSourceId")
# Foreign-key constraints:
#     "elasticc_diaalert_diaObjectId_f96f37de_fk_elasticc_" FOREIGN KEY ("diaObjectId") REFERENCES elasticc_diaobject("diaObjectId") DEFERRABLE INITIALLY DEFERRED
#     "elasticc_diaalert_diaSourceId_ac178389_fk_elasticc_" FOREIGN KEY ("diaSourceId") REFERENCES elasticc_diasource("diaSourceId") DEFERRABLE INITIALLY DEFERRED

# ----------------------------------------------------------------------
#
# Table "public.elasticc_diaobject"
#         Column        |       Type       | Collation | Nullable | Default 
# ----------------------+------------------+-----------+----------+---------
#  diaObjectId          | bigint           |           | not null | 
#  ra                   | double precision |           | not null | 
#  decl                 | double precision |           | not null | 
#  mwebv                | double precision |           |          | 
#  mwebv_err            | double precision |           |          | 
#  z_final              | double precision |           |          | 
#  z_final_err          | double precision |           |          | 
#  hostgal_ellipticity  | double precision |           |          | 
#  hostgal_sqradius     | double precision |           |          | 
#  hostgal_zspec        | double precision |           |          | 
#  hostgal_zspec_err    | double precision |           |          | 
#  hostgal_zphot_q010   | double precision |           |          | 
#  hostgal_zphot_q020   | double precision |           |          | 
#  hostgal_zphot_q030   | double precision |           |          | 
#  hostgal_zphot_q040   | double precision |           |          | 
#  hostgal_zphot_q050   | double precision |           |          | 
#  hostgal_zphot_q060   | double precision |           |          | 
#  hostgal_zphot_q070   | double precision |           |          | 
#  hostgal_zphot_q080   | double precision |           |          | 
#  hostgal_zphot_q090   | double precision |           |          | 
#  hostgal_zphot_q100   | double precision |           |          | 
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
#  hostgal2_zphot       | double precision |           |          | 
#  hostgal2_zphot_err   | double precision |           |          | 
#  hostgal2_zphot_q010  | double precision |           |          | 
#  hostgal2_zphot_q020  | double precision |           |          | 
#  hostgal2_zphot_q030  | double precision |           |          | 
#  hostgal2_zphot_q040  | double precision |           |          | 
#  hostgal2_zphot_q050  | double precision |           |          | 
#  hostgal2_zphot_q060  | double precision |           |          | 
#  hostgal2_zphot_q070  | double precision |           |          | 
#  hostgal2_zphot_q080  | double precision |           |          | 
#  hostgal2_zphot_q090  | double precision |           |          | 
#  hostgal2_zphot_q100  | double precision |           |          | 
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
#  simVersion           | text             |           |          | 
#  hostgal2_zphot_q000  | double precision |           |          | 
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
#  simVersion           | text             |           |          | 
#  hostgal2_zphot_q000  | double precision |           |          | 
#  hostgal2_zspec       | double precision |           |          | 
#  hostgal2_zspec_err   | double precision |           |          | 
#  hostgal_zphot        | double precision |           |          | 
#  hostgal_zphot_err    | double precision |           |          | 
#  hostgal_zphot_p50    | double precision |           |          | 
#  hostgal_zphot_q000   | double precision |           |          | 
#  hostgal2_zphot_p50   | double precision |           |          | 
# Indexes:
#     "elasticc_diaobject_pkey" PRIMARY KEY, btree ("diaObjectId")
#     "idx_elasticc_diaobject_q3c" btree (q3c_ang2ipix(ra, decl))
# Referenced by:
#     TABLE "elasticc_diaalert" CONSTRAINT "elasticc_diaalert_diaObjectId_f96f37de_fk_elasticc_" FOREIGN KEY ("diaObjectId") REFERENCES elasticc_diaobject("diaObjectId") DEFERRABLE INITIALLY DEFERRED
#     TABLE "elasticc_diaforcedsource" CONSTRAINT "elasticc_diaforcedso_diaObjectId_c9bf820c_fk_elasticc_" FOREIGN KEY ("diaObjectId") REFERENCES elasticc_diaobject("diaObjectId") DEFERRABLE INITIALLY DEFERRED
#     TABLE "elasticc_diaobjecttruth" CONSTRAINT "elasticc_diaobjecttr_diaObjectId_16c6d728_fk_elasticc_" FOREIGN KEY ("diaObjectId") REFERENCES elasticc_diaobject("diaObjectId") DEFERRABLE INITIALLY DEFERRED
#     TABLE "elasticc_diasource" CONSTRAINT "elasticc_diasource_diaObjectId_a1a6a675_fk_elasticc_" FOREIGN KEY ("diaObjectId") REFERENCES elasticc_diaobject("diaObjectId") DEFERRABLE INITIALLY DEFERRED

# ----------------------------------------------------------------------
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
#  diaObjectId       | bigint           |           |          | 
# Indexes:
#     "elasticc_diasource_pkey" PRIMARY KEY, btree ("diaSourceId")
#     "elasticc_diasource_diaObject_id_3b88bc59" btree ("diaObjectId")
#     "elasticc_diasource_midPointTai_5766b47f" btree ("midPointTai")
#     "idx_elasticc_diasource_q3c" btree (q3c_ang2ipix(ra, decl))
# Foreign-key constraints:
#     "elasticc_diasource_diaObjectId_a1a6a675_fk_elasticc_" FOREIGN KEY ("diaObjectId") REFERENCES elasticc_diaobject("diaObjectId") DEFERRABLE INITIALLY DEFERRED
# Referenced by:
#     TABLE "elasticc_diaalert" CONSTRAINT "elasticc_diaalert_diaSourceId_ac178389_fk_elasticc_" FOREIGN KEY ("diaSourceId") REFERENCES elasticc_diasource("diaSourceId") DEFERRABLE INITIALLY DEFERRED

# ----------------------------------------------------------------------
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
#  diaObjectId       | bigint           |           | not null | 
# Indexes:
#     "elasticc_diaforcedsource_pkey" PRIMARY KEY, btree ("diaForcedSourceId")
#     "elasticc_diaforcedsource_diaObject_id_8b1bc498" btree ("diaObjectId")
#     "elasticc_diaforcedsource_midPointTai_a80b03af" btree ("midPointTai")
# Foreign-key constraints:
#     "elasticc_diaforcedso_diaObjectId_c9bf820c_fk_elasticc_" FOREIGN KEY ("diaObjectId") REFERENCES elasticc_diaobject("diaObjectId") DEFERRABLE INITIALLY DEFERRED

# ----------------------------------------------------------------------
#
# Table "public.elasticc_diaobjecttruth"
#        Column       |       Type       | Collation | Nullable | Default 
# --------------------+------------------+-----------+----------+---------
#  libid              | integer          |           | not null | 
#  sim_searcheff_mask | integer          |           | not null | 
#  gentype            | integer          |           | not null | 
#  sim_template_index | integer          |           | not null | 
#  zcmb               | double precision |           | not null | 
#  zhelio             | double precision |           | not null | 
#  zcmb_smear         | double precision |           | not null | 
#  ra                 | double precision |           | not null | 
#  dec                | double precision |           | not null | 
#  mwebv              | double precision |           | not null | 
#  galid              | bigint           |           | not null | 
#  galzphot           | double precision |           | not null | 
#  galzphoterr        | double precision |           | not null | 
#  galsnsep           | double precision |           | not null | 
#  galsnddlr          | double precision |           | not null | 
#  rv                 | double precision |           | not null | 
#  av                 | double precision |           | not null | 
#  mu                 | double precision |           | not null | 
#  lensdmu            | double precision |           | not null | 
#  peakmjd            | double precision |           | not null | 
#  mjd_detect_first   | double precision |           | not null | 
#  mjd_detect_last    | double precision |           | not null | 
#  dtseason_peak      | double precision |           | not null | 
#  peakmag_u          | double precision |           | not null | 
#  peakmag_g          | double precision |           | not null | 
#  peakmag_r          | double precision |           | not null | 
#  peakmag_i          | double precision |           | not null | 
#  peakmag_z          | double precision |           | not null | 
#  peakmag_Y          | double precision |           | not null | 
#  snrmax             | double precision |           | not null | 
#  snrmax2            | double precision |           | not null | 
#  snrmax3            | double precision |           | not null | 
#  nobs               | integer          |           | not null | 
#  nobs_saturate      | integer          |           | not null | 
#  diaObjectId        | bigint           |           | not null | 
# Indexes:
#     "elasticc_diaobjecttruth_diaObjectId_16c6d728_pk" PRIMARY KEY, btree ("diaObjectId")
#     "elasticc_diaobjecttruth_diaObjectId_16c6d728_uniq" UNIQUE CONSTRAINT, btree ("diaObjectId")
#     "elasticc_diaobjecttruth_gentype_480cd308" btree (gentype)
#     "elasticc_diaobjecttruth_mjd_detect_first_c84be238" btree (mjd_detect_first)
#     "elasticc_diaobjecttruth_mjd_detect_last_adfc44a3" btree (mjd_detect_last)
#     "elasticc_diaobjecttruth_peakmjd_992f5cef" btree (peakmjd)
#     "elasticc_diaobjecttruth_sim_template_index_b33f9ab4" btree (sim_template_index)
#     "elasticc_diaobjecttruth_zcmb_fe532be5" btree (zcmb)
#     "elasticc_diaobjecttruth_zhelio_f2e6d521" btree (zhelio)
# Foreign-key constraints:
#     "elasticc_diaobjecttr_diaObjectId_16c6d728_fk_elasticc_" FOREIGN KEY ("diaObjectId") REFERENCES elasticc_diaobject("diaObjectId") DEFERRABLE INITIALLY DEFERRED


# ----------------------------------------------------------------------
#
# WARNING : this table is huge as it has an entry for every DiaSource
# and every DiaForcedSource.  If what you're after is the gentype of an object,
# use elasticc_diaobjecttruth.
#
# Table "public.elasticc_diatruth"
#    Column    |       Type       | Collation | Nullable | Default 
# -------------+------------------+-----------+----------+---------
#  diaSourceId | bigint           |           | not null | 
#  diaObjectId | bigint           |           |          | 
#  detect      | boolean          |           |          | 
#  gentype     | integer          |           |          | 
#  genmag      | double precision |           |          | 
#  mjd         | double precision |           |          | 
# Indexes:
#     "elasticc_diatruth_diaSourceId_648273bb_pk" PRIMARY KEY, btree ("diaSourceId")
#     "elasticc_diatruth_diaObjectId_7dd96889" btree ("diaObjectId")
#     "elasticc_diatruth_diaSourceId_648273bb_uniq" UNIQUE CONSTRAINT, btree ("diaSourceId")
