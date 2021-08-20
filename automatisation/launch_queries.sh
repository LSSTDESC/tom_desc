#!/bin/bash

TOMBASEFOLDER=/home/ubuntu/tom-marshall
NIGHT=`date +"%Y%m%d"`

cd $TOMBASEFOLDER

# Fink
QUERYNAME=early_sn_ia_last_day
LISTNAME=fink_${QUERYNAME}_${NIGHT}
python $TOMBASEFOLDER/manage.py run_broker_query $QUERYNAME $LISTNAME

# Add more queries here
# The query_list does not need to exist
#QUERYNAME=
#LISTNAME=brokername_${QUERYNAME}_${NIGHT}
#python $TOMBASEFOLDER/manage.py run_broker_query $QUERYNAME $LISTNAME
