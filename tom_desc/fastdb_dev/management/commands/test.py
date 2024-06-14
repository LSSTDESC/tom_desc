import pymongo
from pymongo import MongoClient
import urllib
import os
import pprint

mongo_username = urllib.parse.quote_plus(os.environ['MONGODB_ALERT_WRITER'])
mongo_password = urllib.parse.quote_plus(os.environ['MONGODB_ALERT_WRITER_PASSWORD'])
collection = 'test'

client = MongoClient("mongodb://%s:%s@fastdbdev-mongodb:27017/?authSource=alerts" %(mongo_username,mongo_password) )    
db = client.alerts
collection = db[collection]

print(collection)

pprint.pprint(collection.find_one({"$and":[{"msg.brokerName":"FakeBroker"},{"msg.classifications.classId":{'$in':[2222,2223,2224,2225,2226]}},{"msg.classifications.probability":{'$gte':0.1}}]}))

#for r in results:
    
#    pprint.pprint(r)

