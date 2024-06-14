import pymongo
from pymongo import MongoClient
import urllib
import os

mongo_username = urllib.parse.quote_plus(os.environ['MONGODB_ALERT_WRITER'])
mongo_password = urllib.parse.quote_plus(os.environ['MONGODB_ALERT_WRITER_PASSWORD'])
collection = 'test'

client = MongoClient("mongodb://%s:%s@fastdbdev-mongodb:27017/?authSource=alerts" %(mongo_username,mongo_password) )    
db = client.alerts
collection = db[collection]

print(collection)

