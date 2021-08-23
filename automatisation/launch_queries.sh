#!/bin/bash
TOMBASE=tom_desc
TOMBASEFOLDER=/home/cohen/lsst/alerts/
NIGHT=`date +"%Y%m%d"`

source ${TOMBASEFOLDER}/tom_env/bin/activate
export DB_HOST=localhost
export DB_PASS=postgres

cd $TOMBASEFOLDER/${TOMBASE}

# Fink
QUERYNAME=early_sn_ia_last_day
#QUERYNAME=fink_sso_ztf_candidates_ztf
LISTNAME=fink_${QUERYNAME}_${NIGHT}
python manage.py run_broker_query $QUERYNAME $LISTNAME

QUERYNAME=pseudo_sn
LISTNAME=antares_${QUERYNAME}_${NIGHT}
python $TOMBASEFOLDER/manage.py run_broker_query $QUERYNAME $LISTNAME

# Add more queries here
# The query_list does not need to exist
#QUERYNAME=
#LISTNAME=brokername_${QUERYNAME}_${NIGHT}
#python $TOMBASEFOLDER/manage.py run_broker_query $QUERYNAME $LISTNAME
