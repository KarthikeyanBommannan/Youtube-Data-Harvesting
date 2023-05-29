from pymongo import MongoClient
from DataHarvesting import generate_output


def upload_to_mongodb(API_Key,channel_ids):
    conn = MongoClient("mongodb+srv://karthikeyan:karthi007@clusters.ayv02is.mongodb.net/?retryWrites=true&w=majority")
    if conn:
        print("Connection Established Successfully")
        
    upload = generate_output(API_Key,channel_ids)   
    database = conn["Youtube"]
    collection = database["Channel_Data"]
    try:
        collection.replace_one({"_Channel_Id": channel_ids}, upload, upsert=True)
        print("Data Uploaded Successfully")
    except Exception as e:
        print(f"Error Occurred while uploading the data: {str(e)}")
        
    conn.close()
    
    
def migrate_to_postgresSQL():
    conn = MongoClient("mongodb+srv://karthikeyan:karthi007@clusters.ayv02is.mongodb.net/?retryWrites=true&w=majority")
    database = conn["Youtube"]
    collection = database["Channel_Data"]
    channel_names =[]
    for name in collection.find({}):
        channel_name = name["Channel_Name"]["Channel_Name"]
        channel_names.append(channel_name)            
    conn.close() 
    return channel_names



    
    
            



    
    
            


