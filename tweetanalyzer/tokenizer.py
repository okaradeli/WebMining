#coding:utf8

import re
import functools

def old_camel_case_split(identifier):
    matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return [m.group(0) for m in matches]

def tokenize (camel_input):
    #words = re.findall(r'[A-Z]?[a-z]+|[A-Z]{2,}(?=[A-Z][a-z]|\d|\W|$)|\d+', camel_input)
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

def get_hashtag_properties(hashtags):
    hashtag_count = len(hashtags)

    #token_count = functools.reduce (lambda x,y: get_token_count(x)+get_token_count(y), hashtags)

    token_count=0
    for hashtag in hashtags:
        token_count+=get_token_count(hashtag)

    char_lenght=0
    for hashtag in hashtags:
        char_lenght+=len(hashtag)

    return char_lenght

if __name__ == "__main__":
    #unit_test()
    x = get_hashtag_properties(["Beşiktaşk","BeşiktaştaAşkBaşkadır","ŞeyhoğluSatıkBuğraHan"])
    print (x)


