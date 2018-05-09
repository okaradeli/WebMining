from pymongo import MongoClient
import json
import pandas as pd

def set_pandas_settings():
    pd.set_option('display.width', 320)
    pd.set_option('max_rows', 1000)


def persist_df_to_mongo(df,database,tablename):
    print("Persisting dataframe to mongo database:"+database+" tablename:"+tablename)
    client = MongoClient('localhost:27017')
    db = client[database]
    records = json.loads(df.T.to_json()).values()
    db[tablename].insert_many(records)

def load_database_to_df(mongo_database,mongo_table):
    print("Started reading records...")

    print("Initializing Mongo connection...")
    client = MongoClient('localhost:27017')
    db = client[mongo_database]
    print("Initializing Mongo connection success. Reading table:"+mongo_table)
    scrapped_tweets = db[mongo_table].find()#.limit(1000)

    df = pd.DataFrame(list(scrapped_tweets))

    # Delete the _id column
    del df['_id']



    print("Converting records to pandas df...")
    print("Number of lines:"+str(len(df)))

    print("Sanity check on columns if anyone has NaN")
    print(df.isnull().sum())

    print("Drop NaN rows")
    df = df.dropna(axis=0, how='any')
    print("Final number of lines :" + str(len(df)))

    return df
