import sys
import os
from pymongo import MongoClient

host = os.getenv( 'MONGOHOST' )
user = os.getenv( 'MONGODB_ADMIN' )
password = os.getenv( 'MONGODB_ADMIN_PASSWORD' )

client = MongoClient( f"mongodb://{user}:{password}@{host}:27017/" )
sys.stderr.write( "Creating mongodb database alerts\n" )
db = client.alerts

users = [ os.getenv( "MONGODB_ALERT_WRITER" ), os.getenv( "MONGODB_ALERT_READER" ) ]
passwds = [ os.getenv( "MONGODB_ALERT_WRITER_PASSWORD" ), os.getenv( "MONGODB_ALERT_READER_PASSWORD" ) ]
roleses = [ [ "readWrite" ] , [ "read" ] ]

for user, passwd, roles in zip( users, passwds, roleses ):
    sys.stderr.write( f"Creating mongodb user {user} with password {passwd} and roles {roles}\n" )
    db.command( "createUser", user, pwd=passwd, roles=roles )
