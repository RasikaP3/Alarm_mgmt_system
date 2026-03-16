from pymongo import MongoClient

def connect_db():
    client = MongoClient("mongodb://localhost:27017/")
    
    db = client["noc_alarm_system"]
    
    alarms_collection = db["alarms"]
    
    print("Connected to MongoDB")

    return alarms_collection