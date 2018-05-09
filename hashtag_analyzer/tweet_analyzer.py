import pymongo
from pymongo import MongoClient
import operator
import pandas as pd
import pickle
import json
import hashtag_analyzer.tokenizer as tokenizer
import hashtag_analyzer.hastagger as hashtag_analyzer
import hashtag_analyzer.user_creator as user_creator
import hashtag_analyzer.utils as utils

READ_FROM_PICKLE=False
MONGO_DATABASE='twitter_zeynep'
MONGO_TABLE= 'userTweets_test'
MONGO_TWEET_EFFECT_TABLE= 'processed_1'


tweet_dic={}
tweet_effects=[]

def analyze_tweets():
    print("Initializing Mongo connection...")
    client = MongoClient('localhost:27017')
    db = client[MONGO_DATABASE]
    print("Initializing Mongo connection success.")

    counter_general = 0
    counter_new = 0

    first_time_hashtag={}

    scrapped_tweets = db[MONGO_TABLE].find()
    #scrapped_tweets = db[MONGO_TABLE].find().sort([("yearMonthDay",1)])

    hashtag_dic ={}
    item_count=0

    #First Pass for building dictionary
    print("FIRST PASS: Building token/hashtag effects")
    for tweet in scrapped_tweets:
        item_count+=1
        if(item_count % 100000==0):
            print("Processing item:"+str(item_count))
            #print("Processing tweet with ID:"+str(tweet['_id']))

        #skip tweets without hashtags
        if "hashtags" not in tweet.keys() or tweet['hashtags'] is None or len(tweet['hashtags']) < 1:
            continue

        #skip tweets without retweets
        retweet_count=0
        if "retweetCount" not in tweet.keys() or tweet['retweetCount']<1 or "reTweetedTweetId" in tweet.keys():
            # focus only on Tweets with RetweetCount>0 and also being the original tweet
            continue
        retweet_count = tweet['retweetCount']

        #tokenize hashtag
        hashtags = tweet['hashtags']
        tweeet_id = tweet['_id']

        #Process token properties
        tokenizer.process_tokens(hashtags,retweet_count)

        #Process hashtag properties
        hashtag_analyzer.process_hashtags(hashtags,retweet_count,tweeet_id)


    #Summarize token properties
    token_summary= tokenizer.summarize_token()
    #Summarize hashtag properties
    hashtag_summary = hashtag_analyzer.summarize_hashtags()


    print("Total number of tokens:"+str(len(token_summary)))
    print("Total number of hashtags:" + str(len(hashtag_summary)))

    #Load user stats table
    user_creator.load_user_stats_from_mongo()

    #Second Pass for building tweet effects
    print("SECOND PASS: Calculating Tweeter effects")
    scrapped_tweets = db[MONGO_TABLE].find()
    #scrapped_tweets = db[MONGO_TABLE].find().sort([("yearMonthDay", 1)])
    item_count=0
    for tweet in scrapped_tweets:
        item_count+=1
        if(item_count % 100000==0):
            print("Processing item:"+str(item_count))

        #skip tweets without hashtags
        if "hashtags" not in tweet.keys() or tweet['hashtags'] is None or len(tweet['hashtags']) < 1:
            continue

        #skip tweets without retweets
        retweet_count=0
        if "retweetCount" not in tweet.keys() or tweet['retweetCount']<1 or "reTweetedTweetId" in tweet.keys():
            # focus only on Tweets with RetweetCount>0 and also being the original tweet
            continue
        retweet_count = tweet['retweetCount']

        #tokenize hashtag
        hashtags = tweet['hashtags']
        tweeet_id = tweet['_id']
        user_id = tweet['userid']

        #Calculate the tweet effect object ( row )
        process_tweet(user_id,hashtags,retweet_count,tweeet_id)

    #Convert to Dataframe
    df = pd.DataFrame(tweet_effects,columns = ['token_avg_retweet', 'token_frequency', 'token_char_length','token_is_all_upper',
                  'hashtag_avg_retweet','hashtag_frequency','hashtag_char_length','hashtag_is_all_upper','hashtag_token_count',
                  'tweet_is_first_time_hashtag','tweet_hashtag_count','tweet_total_char_length','tweet_retweet_count',
                  'user_retweet_ratio']
                      )

    #print(tokenizer.token_dic)
    #print(df)
    print("Number of tweets read from DB:"+str(item_count))
    print("Total number of tweet effect processed samples :" + str(len(df)))

    utils.persist_df_to_mongo(df,MONGO_DATABASE,MONGO_TWEET_EFFECT_TABLE)


def process_tweet(user_id,hashtags,retweet_count,tweet_id):
    #Get token level properties
    set1 = tokenizer.get_hashtags_token_summary(hashtags)

    #Get hashtag level properties
    set2 = hashtag_analyzer.get_hashtags_summary(hashtags)


    #Get Tweet propoerties
    is_first_time_hashtag = hashtag_analyzer.is_first_time_hashtag(tweet_id,hashtags)
    hashtag_count = len(hashtags)
    total_char_length = sum(int(len(key)) for key in hashtags)
    set3=(is_first_time_hashtag,hashtag_count,total_char_length,retweet_count)

    #Get user level properties
    set4 = (user_creator.get_user_stats(user_id),)

    #combine them all to get the whole tweeter effect object
    tweet_effect = set1 + set2 + set3 + set4
    tweet_effects.append(tweet_effect)




def stat_frequency():

    #Save DF to disk for quicker access
    if READ_FROM_PICKLE:
        print("Reading from pickle... ")
        df = pickle.load(open("tweet_pickle.p", "rb"))
    else:
        print("Reading from mongodb... ")
        df = analyze_tweets()
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

    print("Persisting hashtags to mongo"),

    #persist stats to db
    df = df.sort_values(by=['hashtag'], ascending=[True])
    print(df.head(100))



def is_hashtag_used_firsttime(hashtag,tweet_time):
    print ("hello")


def only_turkish_chars(s):
    try:
        s.encode("iso-8859-9")
        return True
    except:
        return False


if __name__ == "__main__":
    utils.set_pandas_settings()
    analyze_tweets()