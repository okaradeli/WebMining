from pymongo import MongoClient
import json

def persist_df_to_mongo(df,database,tablename):
    print("Persisting dataframe to mongo database:"+database+" tablename:"+tablename)
    client = MongoClient('localhost:27017')
    db = client[database]
    records = json.loads(df.T.to_json()).values()
    db[tablename].insert_many(records)