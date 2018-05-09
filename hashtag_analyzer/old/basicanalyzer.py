import pymongo
from pymongo import MongoClient
import operator
import pandas as pd
import pickle

READ_FROM_PICKLE=True
#MONGO_DATABASE='twitter_zeynep'
MONGO_DATABASE='userTweets_sample'


def read_from_mongo():
    print("Initializing Mongo connection...")
    client = MongoClient('localhost:27017')
    db = client.TweetScraper
    print("Initializing Mongo connection success.")

    counter_general = 0
    counter_new = 0

    #scrapped_tweets = db.MONGO_DATABASE.find()
    scrapped_tweets = db[MONGO_DATABASE].find()
    hashtag_dic ={}
    item_count=0
    for tweet in scrapped_tweets:
        item_count+=1
        if(item_count % 10000==0):
            print("Processing item:"+str(item_count))

        ##try:
            ##print("Processing tweet with ID"+tweet['ID'])
        if "tw_hashtags" not in tweet.keys() or tweet['tw_hashtags'] is None or len(tweet['tw_hashtags']) < 1:
            continue
        #tokenize hashtag
        hashtags = tweet['tw_hashtags'].split(';')
        for hashtag in hashtags:
            if hashtag in hashtag_dic:
                tuple=(hashtag_dic[hashtag][0]+1,hashtag_dic[hashtag][1]+tweet['tw_retweet_count'])
                hashtag_dic[hashtag] =tuple
            else:
                hashtag_dic[hashtag]=(1,tweet['tw_retweet_count'])
        ##except:
        ##    print("Something wrong with this tweet"+tweet)

    df = pd.DataFrame.from_dict(hashtag_dic, orient='index').reset_index()
    df.columns = ['hashtag', 'hashtag_count', 'retweet_count']

    #Add new retweet ratio column
    df['retweet_ratio']=df['retweet_count']/(df['hashtag_count']+0.001)

    print("Number of tweets read from DB:"+item_count)

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

    print("DF sorted by retweet_ratio")
    df = df.sort_values(by=['retweet_ratio'], ascending=[False])
    print(df.head(100))

    print("DF sorted by hashtag_count")
    df = df.sort_values(by=['hashtag_count'], ascending=[False])
    print(df.head(100))

    print("DF sorted by retweet_count")
    df = df.sort_values(by=['retweet_count'], ascending=[False])
    print(df.head(100))


def set_pandas_settings():
    pd.set_option('display.width', 320)
    pd.set_option('max_rows', 1000)


if __name__ == "__main__":
    set_pandas_settings()
    stat_frequency()