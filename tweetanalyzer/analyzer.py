import pymongo
from pymongo import MongoClient
import operator
import pandas as pd
import pickle
import json
import tweetanalyzer.tokenizer as tokenizer

READ_FROM_PICKLE=False
MONGO_DATABASE='twitter_zeynep'
MONGO_TABLE= 'userTweets_sample'
#MONGO_DATABASE='userTweets_test'



def read_from_mongo():
    print("Initializing Mongo connection...")
    client = MongoClient('localhost:27017')
    db = client[MONGO_DATABASE]
    print("Initializing Mongo connection success.")

    counter_general = 0
    counter_new = 0

    #scrapped_tweets = db.MONGO_DATABASE.find()
    scrapped_tweets = db[MONGO_TABLE].find()
    hashtag_dic ={}
    item_count=0
    for tweet in scrapped_tweets:
        item_count+=1
        if(item_count % 10000==0):
            print("Processing item:"+str(item_count))

        ##try:
        #print("Processing tweet with ID:"+str(tweet['_id']))
        if "hashtags" not in tweet.keys() or tweet['hashtags'] is None or len(tweet['hashtags']) < 1:
            continue
        #tokenize hashtag
        hashtags = tweet['hashtags']

        #Get hashtag properties
        hashtag_properties =tokenizer.get_hashtag_properties(hashtags)

        for hashtag in hashtags:
            if hashtag in hashtag_dic:
                tuple=(hashtag_dic[hashtag][0]+1,hashtag_dic[hashtag][1]+tweet['retweetCount'])
                hashtag_dic[hashtag] =tuple
            else:
                hashtag_dic[hashtag]=(1,tweet['retweetCount'])
        ##except:
        ##    print("Something wrong with this tweet"+tweet)

    df = pd.DataFrame.from_dict(hashtag_dic, orient='index').reset_index()
    df.columns = ['hashtag', 'hashtag_count', 'retweet_count']

    #Add new retweet ratio column
    df['retweet_ratio']= df['retweet_count']/(df['hashtag_count']+0.001)

    print("Number of tweets read from DB:"+str(item_count))

    return df



def stat_frequency():

    #Save DF to disk for quicker access
    if READ_FROM_PICKLE:
        print("Reading from pickle... ")
        df = pickle.load(open("tweet_pickle.p", "rb"))
    else:
        print("Reading from mongodb... ")
        df = read_from_mongo()
        pickle.dump(df, open("tweet_pickle.p", "wb"))

    print("Number of tweet hashtag loaded "+str(len(df)))


    print("hashtag summary:")
    print("Count of distinct hashtags:"+str(len(df)))
    print("Sum of hashtags:" + str(sum(df['hashtag_count'])))
    print("Sum of retweets:" + str(sum(df['retweet_count'])))

    print("All hashtag count:"+str(len(df)))
    df = df[df['hashtag'].apply(lambda x: only_turkish_chars(x))]
    print("Turkish hashtag count:" + str(len(df)))


    print("DF sorted by retweet_ratio")
    df = df.sort_values(by=['retweet_ratio'], ascending=[False])
    print(df.head(100))

    print("DF sorted by hashtag_count")
    df = df.sort_values(by=['hashtag_count'], ascending=[False])
    print(df.head(100))

    print("DF sorted by retweet_count")
    df = df.sort_values(by=['retweet_count'], ascending=[False])
    print(df.head(100))

    print("Persisting hashtags to mongo")


    df = df.sort_values(by=['hashtag'], ascending=[True])
    print(df.head(100))
    client = MongoClient('localhost:27017')
    db = client.twitter_zeynep
    records = json.loads(df.T.to_json()).values()
    db.hashtags.insert_many(records)


def only_turkish_chars(s):
    try:
        s.encode("iso-8859-9")
        return True
    except:
        return False


def set_pandas_settings():
    pd.set_option('display.width', 320)
    pd.set_option('max_rows', 1000)


if __name__ == "__main__":
    set_pandas_settings()
    stat_frequency()