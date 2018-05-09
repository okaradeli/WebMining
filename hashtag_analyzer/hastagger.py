import numpy as np
import hashtag_analyzer.tokenizer as tokenizer

#
# This module contains methods to analyze the nature of a single/multiple Hashtags
# i.e. if it is long,short, too much tokens in it, frequency...
#


hashtag_dic={}
hashtag_summary={}
hashtag_first_occurence={}

#utulity method to append/update hashtag_dic
def add_to_hashtag_dic(hashtag, hashtag_properties,tweet_id):

    if hashtag in hashtag_dic:
        hashtag_entry=hashtag_dic[hashtag]
    else:
        hashtag_entry = []
        hashtag_dic[hashtag]=hashtag_entry
        hashtag_first_occurence[hashtag]=tweet_id

    hashtag_entry.append(hashtag_properties)


#Process each hashtag along with their success score
def process_hashtags(hashtags, retweet_count,tweet_id):

    for hashtag in hashtags:
        hashtag_properties = (retweet_count,)#tuple syntax

        add_to_hashtag_dic(hashtag, hashtag_properties,tweet_id)



def summarize_hashtags():
    for hashtag in hashtag_dic:
        avg_retweet=int(np.ceil(np.mean(hashtag_dic[hashtag])))
        frequency = len(hashtag_dic[hashtag])
        char_length=len(hashtag)
        is_all_upper = hashtag.isupper()

        #number of tokens in the hashtag
        tokens=tokenizer.tokenize(hashtag)
        token_count=len(tokens)

        #case sensivitiy of hashtag
        is_all_upper = hashtag.isupper()

        hashtag_summary[hashtag]=(avg_retweet,frequency,char_length,is_all_upper,token_count)

    return hashtag_summary


def get_hashtags_summary(hashtags):
    hashtag_stats_avg_retweet = []
    hashtag_stats_frequency = []
    hastag_stats_char_length = []
    hashtag_stats_is_all_upper = []
    hashtag_stats_token_count = []
    for hashtag in hashtags:
        hashtag_stats_avg_retweet.append(hashtag_summary[hashtag][0])
        hashtag_stats_frequency.append(hashtag_summary[hashtag][1])
        hastag_stats_char_length.append(hashtag_summary[hashtag][2])
        hashtag_stats_is_all_upper.append(hashtag_summary[hashtag][3])
        hashtag_stats_token_count.append(hashtag_summary[hashtag][4])

    # summarize
    avg_retweet = np.mean(hashtag_stats_avg_retweet)
    frequency = np.mean(hashtag_stats_frequency)
    char_length = np.mean(hastag_stats_char_length)
    is_all_upper = np.mean(hashtag_stats_is_all_upper)
    token_count = np.mean(hashtag_stats_token_count)

    return (avg_retweet, frequency, char_length, is_all_upper,token_count)

#if one hashtag in hastags is used for the first time return True, False otherwise.
def is_first_time_hashtag(tweet_id, hashtags):

    for hashtag in hashtags:
        if hashtag_first_occurence[hashtag]== tweet_id:
            return True
    return False