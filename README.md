# Mongodb

import pymongo
# Connection to Mongodb 
client = pymongo.MongoClient('mongodb+srv://<User_name>:<Password>@mongodemo.lwmu0.mongodb.net/test')
# Chosen which database
  
 db=client["DataBase_Name"]
  
 # chosen which data collection 
 
 collection = db.get_collection(" volcanoes")
 
 # Create New Pipeline
  Pipeline1=([
{
    '$project': {
            '_id': 0, 
            'Volcano_Name': 1, 
            'Country': 1, 
            'Lat': 1, 
            'Lon': 1, 
            'Last_Eruption': 1
        }
    }, {
        '$match': {
            'Country': 'Japan'
        }
    }   
]
)
# this linw will save your pipeline to your collection and create view 
  db.create_collection("pipeline_1",viewOn=" volcanoes",pipeline=Pipeline1)
