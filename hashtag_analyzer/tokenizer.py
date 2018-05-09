#coding:utf8

import re
import functools
import numpy as np

first_time_token={}
token_dic={}
token_summary={}

def old_camel_case_split(identifier):
    matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return [m.group(0) for m in matches]

def tokenize (camel_input):
    matches = re.finditer(r'[A-ZÇĞİÖŞÜ]?[a-zğçıüş]+|[A-ZÇĞİÖŞÜ]{2,}(?=[A-ZÇĞİÖŞÜ][a-zğçıüş]|\d|\W|$)|\d+', camel_input)
    return [m.group(0) for m in matches]

def get_token_count(input):
    tokens = tokenize(input)
    return len(tokens)

def get_hashtag_length(input):
    return len(input)

def get_hashtag_count(hashtags):
    return len(hashtags)


def unit_test():
    #y = tokenize("TestÖyküEdiyorumMKNe1234Diyorsun")
    y = tokenize("ŞeyhoğluSatıkBuğraHan")
    print ( y)

def get_token_properties(token):

    #token_count = functools.reduce (lambda x,y: get_token_count(x)+get_token_count(y), hashtags)
    char_lenght =len(token)

    first_time_token = 0 if token in token_dic else 1

    #TODO is stemmed here
    #TODO is synatactically correct here
    return (char_lenght,first_time_token)

if __name__ == "__main__":
    #unit_test()
    x = get_token_properties(["Beşiktaşk", "BeşiktaştaAşkBaşkadır", "ŞeyhoğluSatıkBuğraHan"])
    print (x)


def add_to_token_dic(token, token_properties):

    if token in token_dic:
        token_entry=token_dic[token]
    else:
        token_entry = []
        token_dic[token]=token_entry

    token_entry.append(token_properties)


def process_tokens(hashtags, retweet_count):
    for hashtag in hashtags:
        tokens=tokenize(hashtag)
        for token in tokens:
            #token_properties = get_token_properties(token)
            #token_properties+=(retweet_count,)
            token_properties = retweet_count
            add_to_token_dic(token, token_properties)

def summarize_token():
    for token in token_dic:
        avg_retweet=int(np.ceil(np.mean(token_dic[token])))
        frequency = len(token_dic[token])
        char_length=len(token)
        is_all_upper = token.isupper()
        token_summary[token]=(avg_retweet,frequency,char_length,is_all_upper)
    return token_summary


def get_hashtags_token_summary(hashtags):

    token_stats_avg_retweet=[]
    token_stats_frequency = []
    token_stats_char_length = []
    token_stats_is_all_upper= []
    for hashtag in hashtags:
        tokens = tokenize(hashtag)
        for token in tokens:
            token_stats_avg_retweet.append(token_summary[token][0])
            token_stats_frequency.append(token_summary[token][1])
            token_stats_char_length.append(token_summary[token][2])
            token_stats_is_all_upper.append(token_summary[token][3])

    #summarize
    avg_retweet = np.mean(token_stats_avg_retweet)
    frequency = np.mean(token_stats_frequency)
    char_length = np.mean(token_stats_char_length)
    is_all_upper = np.mean(token_stats_is_all_upper)

    return (avg_retweet,frequency,char_length,is_all_upper)





