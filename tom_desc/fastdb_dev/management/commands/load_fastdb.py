import sys
import pathlib
import logging
import fastavro
import json
import multiprocessing
import fastdb_dev.models
from django.core.management.base import BaseCommand, CommandError
import signal
import datetime
from datetime import timezone
import time
from psycopg2.extras import execute_values
from psycopg2 import sql
import psycopg2
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import pprint
import urllib.parse
import os
from fastdb_dev.models import LastUpdateTime, ProcessingVersions, HostGalaxy, Snapshots, DiaObject, DiaSource, DiaForcedSource
from fastdb_dev.models import DStoPVtoSS, DFStoPVtoSS, BrokerClassifier, BrokerClassification
from django.core.exceptions import ObjectDoesNotExist

_rundir = pathlib.Path(__file__).parent
print(_rundir)
sys.path.insert(0, str(_rundir) )


class Command(BaseCommand):
    help = 'Store alerts in FASTDB'

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )
        self.logger = logging.getLogger( "FASTDB log" )
        self.logger.propagate = False
        logout = logging.FileHandler( _rundir.parent.parent.parent.parent / f"code/logs/fastdb.log" )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - fastdb - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        self.logger.setLevel( logging.DEBUG )

    def add_arguments( self, parser ):
        parser.add_argument( '--season', default=1, help="Observing season" )
        parser.add_argument( '--brokers', nargs="*", help="List of brokers" )
        parser.add_argument( '--snapshot', help="Snapshot name" )
        parser.add_argument( '--pv', help="Processing version" )
        parser.add_argument( '--tag', help="Snapshot Tag" )

    def handle( self, *args, **options ):

        mongodb_collections = {'alerce':'alerce','antares':'antares','fink':'fink','ztf':'ztf', 'test':'test'}
        brokerNames = {'test':'FakeBroker'}
        self.logger.info( "********fastdb starting ***********" )

        season = options['season']
        snapshot = options['snapshot']
        processing_version = options['pv']

        mongo_username = urllib.parse.quote_plus(os.environ['MONGODB_ALERT_WRITER'])
        mongo_password = urllib.parse.quote_plus(os.environ['MONGODB_ALERT_WRITER_PASSWORD'])

        # mongodb running on port 27017 on host $MONGOHOST; default
        #   $MONGOHOST to fastdbdev-mongodb for backwards compatibility
        #   with previous installs
        mongohost = os.getenv( 'MONGOHOST', 'fastdbdev-mongodb' )
        client = MongoClient(f"mongodb://{mongo_username}:{mongo_password}@{mongohost}:27017/?authSource=alerts")
        self.db = client.alerts
        
        # Connect to the PPDB
            
        # Get password

        ppdb_name =  os.environ['DB_NAME']
        ppdb_host =  os.environ['DB_HOST']
        ppdb_user =  os.environ['DB_USER']
        ppdb_password = os.environ['DB_PASS']
        conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (ppdb_host,ppdb_name,ppdb_user,ppdb_password)
        conn = psycopg2.connect(conn_string)
        
        cursor = conn.cursor()
        self.logger.info("Connected to PPDB")


        # Get last update time

        lst = LastUpdateTime.objects.latest('last_update_time')
        last_update_time = lst.last_update_time
        self.logger.info(last_update_time)
        print(last_update_time)
            
        current_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        #current_datetime = datetime.datetime(2023,4,30,0,0,0,tzinfo=timezone.utc)

        # get ProcessingVersions

        pv = ProcessingVersions.objects.filter(version=processing_version)
        for p in pv:
            vs = p.validity_start
            ve = p.validity_end
            if ve is not None:
                self.logger.error("Invalid Processing Version - already has End Date")
                cursor.close()
                conn.close()
                exit
            else:
                print(current_datetime, vs)
                if current_datetime > vs:
                    processing_version = p.version
                else:
                    self.logger.error("Current date isn't gte than start date of Processing Version")
                    exit
                

        # get all the alerts that pass at least one of the SN criteria with probability > 0.1 since last_update_time
        # Loop over the brokers that were passed in via the argument list

        brokerstodo = options['brokers']
        print(brokerstodo)

        list_diaSourceId = []
        
        for name in brokerstodo:
            print(brokerNames[name])
            collection = self.db[mongodb_collections[name]]
            results = collection.find({"$and":[{"msg.brokerName":brokerNames[name]},{"timestamp":{'$gte':last_update_time, '$lt':current_datetime}},{"msg.classifications":{'$elemMatch':{'$and':[{"classId":{'$in':[2222,2223,2224,2225,2226,2242,2232]}},{"probability":{'$gte':0.1}}]}}}]})
 
            for r in results:
                diaSource_id = r['msg']['diaSourceId']
                alert_id = r['msg']['alertId']

                bc = BrokerClassification(alert_id=alert_id)
                bc.dia_source = r['msg']['diaSourceId']
                bc.topic_name = r['topic']
                bc.desc_ingest_timestamp =  datetime.datetime.now(tz=datetime.timezone.utc)
                bc.broker_ingest_timestamp =  r['timestamp']
                broker_version = r['msg']['brokerVersion']
                broker_name = r['msg']['brokerName']
                classifier_name = r['msg']['classifierName']
                broker_classifier = BrokerClassifier.objects.get(broker_name=broker_name, broker_version=broker_version, classifier_name=classifier_name)
                bc.classifier = broker_classifier.classifier_id  # Local copy of classifier to circumvent Django Foreign key rules
                bc.classifications = r['msg']['classifications']

                bc.save()
                
                list_diaSourceId.append(diaSource_id)

        # Get unique set of source Ids across all broker alerts

        uniqueSourceId = set(list_diaSourceId)
        self.logger.info("Number of Unique Source Ids %s" % len(uniqueSourceId))

        # Look for DiaSourceIds in the PPDB DiaSource table

        #columns = diaSourceId,diaObjectId,psFlux,psFluxSigma,midPointTai,ra,decl,snr,filterName,observeDate

        for d in uniqueSourceId:

            self.logger.info("Source Id %d" % d)
            query = sql.SQL( "SELECT * FROM {}  where {} = %s").format(sql.Identifier('elasticc2_ppdbdiasource'),sql.Identifier('diasource_id'))
            self.logger.info(query.as_string(conn))
                            
            cursor.execute(query,(d,))
            self.logger.info("Count = %d" % cursor.rowcount)
            if cursor.rowcount != 0:
                result = cursor.fetchone()

                # Store this new Source in the FASTDB

                ds = DiaSource(dia_source=result[0])
                ds.season = season
                ds.filter_name = result[2]
                ds.ra = result[3]
                ds.decl = result[3]
                ds.ps_flux = result[5]
                ds.ps_flux_err = result[6]
                ds.snr = result[7]
                ds.mid_point_tai = result[1]
                
                # Count how many brokers alerted on this Source Id
                ds.broker_count = list_diaSourceId.count(d)
                ds.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)

                diaObjectId = result[8]
                self.logger.info("Dia Object Id = %s" % diaObjectId)
                
                # Now look to see whether we already have this DiaObject in FASTDB
                
                try:
                    do = DiaObject.objects.get(pk=diaObjectId)
                    
                    # Update number of observations
                    
                    do.nobs +=1
                    do.save()
                    
                except ObjectDoesNotExist:
                        
                    self.logger.info("DiaObject not in FASTDB. Create new entry.")
                    
                    # Fetch the DiaObject from the PPDB
                    
                    query = sql.SQL("SELECT * from {} where {} = %s").format(sql.Identifier('elasticc2_ppdbdiaobject'),sql.Identifier('diaobject_id'))
                    
                    cursor.execute(query,(diaObjectId,))
                    if cursor.rowcount != 0:
                        result = cursor.fetchone()
                        do = DiaObject(dia_object=diaObjectId)
                        do.validity_start = datetime.datetime.now(tz=datetime.timezone.utc)
                        do.season = season
                        do.ra = result[2]
                        do.decl = result[3]
                        do.ra_sigma = 0.00001
                        do.decl_sigma = 0.00001
                        do.ra_dec_tai = ds.mid_point_tai
                        do.nobs = 1
                        do.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)
                        
                        # locate Host Galaxies in Data release DB Object table
                        # There is information in the PPDB for the 3 closest objects. Is this good enough?
                        # Where to get them in season 1?
                        
                        
                        do.save()


                # Store Foreign key to DiaObject, fake_id, season in DiaSource table

                do = DiaObject.objects.get(pk=diaObjectId)
                ds.dia_object = do
                ds.fake_id = do.fake_id
                ds.season = do.season
                ds.processing_version = processing_version

                ds.save()

                dspvss = DStoPVtoSS(dia_source=d)
                dspvss.processing_version = processing_version
                dspvss.snapshot_name = snapshot
                dspvss.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)

                dspvss.save()
                
                # Look to see if there any ForcedSource entries for this object

                # Now look to see whether we already have any ForcedSource in FASTDB
                
                dfs = DiaForcedSource.objects.filter(dia_object_id=diaObjectId)
                self.logger.info(len(dfs))
                if len(dfs) == 0:    

                     # diaForcedSourceId,diaObjectId,psFlux,psFluxSigma,filterName,observeDate
                    query = sql.SQL("SELECT * from {} where {} = %s").format(sql.Identifier('elasticc2_ppdbdiaforcedsource'),sql.Identifier('diaobject_id'))
                    cursor.execute(query,(diaObjectId,))
                    if cursor.rowcount != 0:
                        results = cursor.fetchall()
                        for r in results:
                            dfs = DiaForcedSource(dia_forced_source=r[0])
                            dfs.dia_force_source = r[0]
                            dfs.dia_object = do
                            dfs.season = season
                            dfs.fake_id = 0
                            dfs.filter_name = r[2]
                            dfs.ps_flux = r[3]
                            dfs.ps_flux_err = r[4]
                            dfs.mid_point_tai = r[1]
                            dfs.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)
                            dfs.processing_version = processing_version
                            self.logger.info("Forced Source Id %d" % r[0])
                            dfs.save()

                            dfspvss = DFStoPVtoSS(dia_forced_source=r[0])
                            dfspvss.processing_version = processing_version
                            dfspvss.snapshot_name = snapshot
                            dfspvss.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)
                            
                            dfspvss.save()



        # Store last_update_time
        lst.last_update_time = current_datetime
        lst.save()
        
        cursor.close()
        conn.close()
 
