#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymongo
client = pymongo.MongoClient('mongodb+srv://<User_name>:<Password>@mongodemo.lwmu0.mongodb.net/test')


# In[15]:


db=client["WeekendProject"]


# In[23]:


collection = db.get_collection(" volcanoes")


# In[24]:


collection.find_one()


# In[28]:


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


# In[30]:


db.create_collection("pipeline_1",viewOn=" volcanoes",pipeline=Pipeline1)


# In[31]:


Pipeline2=([
    {
        '$project': {
            '_id': 0, 
            'Volcano_Name': 1, 
            'Volcano_Type': 1, 
            'Country': 1, 
            'Last_Eruption': 1, 
            'Lat': 1, 
            'Lon': 1, 
            'population_within_5km': 1
        }
    }, {
        '$match': {
            'population_within_5km': {
                '$gt': 5000
            }
        }
    }, {
        '$sort': {
            'population_within_5km': 1
        }
    }
]

    

)


# In[32]:


db.create_collection("pipeline_2",viewOn=" volcanoes",pipeline=Pipeline2)


# In[33]:


Pipeline3=([
    {
        '$project': {
            '_id': 0, 
            'Volcano_Name': 1, 
            'Volcano_Image': 1, 
            'Volcano_Type': 1, 
            'Country': 1, 
            'Region': 1, 
            'Subregion': 1, 
            'epoch_period': 1, 
            'Last_Eruption': 1, 
            'Summit_and_Elevatiuon': 1, 
            'Latitude': 1, 
            'Longitude': 1, 
            'population_within_5km': 1, 
            'population_within_10km': 1, 
            'population_within_30km': 1, 
            'population_within_100km': 1, 
            'Lat': 1, 
            'Lon': 1, 
            'last_erupt': 1, 
            'elevation_feet': 1, 
            'elevation_meters': 1
        }
    }, {
        '$match': {
            'population_within_10km': {
                '$gt': 100000
            }
        }
    }, {
        '$group': {
            '_id': {
                'Volcano_Name': '$Volcano_Name', 
                'Volcano_Type': '$Volcano_Type', 
                'Country': '$Country'
            }
        }
    }
]
)


# In[34]:


db.create_collection("pipeline_3",viewOn=" volcanoes",pipeline=Pipeline3)


# In[35]:


Pipeline4=([
    {
        '$project': {
            '_id': 0, 
            'Volcano_Name': 1, 
            'Country': 1, 
            'Lon': 1, 
            'Lat': 1, 
            'elevation_feet': 1, 
            'elevation_meters': 1, 
            'Last_Eruption': 1
        }
    }, {
        '$match': {
            'Last_Eruption': {
                '$regex': 'BCE*'
            }
        }
    }
]
)


# In[36]:


db.create_collection("pipeline_4",viewOn=" volcanoes",pipeline=Pipeline4)


# In[37]:


Pipeline5=([
    {
        '$match': {
            'elevation_meters': {
                '$ne': None
            }
        }
    }, {
        '$group': {
            '_id': {
                'Volcano_Type': '$Volcano_Type'
            }, 
            'max': {
                '$max': '$elevation_meters'
            }, 
            'min': {
                '$min': '$elevation_meters'
            }
        }
    }
]
)


# In[38]:


db.create_collection("pipeline_5",viewOn=" volcanoes",pipeline=Pipeline5)

