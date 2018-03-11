#Tweet class holding the Tweet details of Tweeter API
#Knows how to parse the json response object

class Tweet:
    tw_full_text = None
    tw_retweet_count = None
    tw_favorite_count = None
    tw_favorited = None
    tw_hashtags = None
    tw_source =None
    tw_geo =None
    tw_coordinates = None
    tw_loc_country = None
    tw_loc_fullname = None
    tw_loc_name = None
    tw_loc_type = None
    tw_loc_country = None
    tw_loc_fullname = None
    tw_loc_name = None
    tw_loc_type = None
    tw_lang = None
    user_location = None
    user_profile_image_url = None
    user_lang = None
    user_description = None
    user_url = None
    user_friends_count = None
    user_followers_count = None
    user_name = None
    user_screen_name = None
    user_listed_count = None
    user_favourites_count = None
    user_statuses_count = None
    user_created_at = None
    user_verified = None
    user_utc_offset = None
    user_timezone = None
    user_geo_enabled = None
    user_default_profile = None



    ##Initializes Tweet with json response from Twitter API
    def __init__(self,t):
        self.tw_full_text = t.full_text.replace('|', ' ').replace('\n', ' ')
        self.tw_retweet_count = t.retweet_count
        self.tw_favorite_count = t.favorite_count
        self.tw_favorited = t.favorited
        self.tw_hashtags = ""
        hashtags = list(t.entities.values())
        for hashtag_entity in hashtags:
            for hashtag_dict in hashtag_entity:
                tag = hashtag_dict.get('text')
                if (tag != None):
                    self.tw_hashtags += tag
                    self.tw_hashtags += ';'
        self.tw_hashtags = self.tw_hashtags[:-1]
        self.tw_source = t.source
        self.tw_geo = t.geo
        self.tw_coordinates = t.coordinates
        self.tw_loc_country = None
        self.tw_loc_fullname = None
        self.tw_loc_name = None
        self.tw_loc_type = None
        if (t.place != None):
            self.tw_loc_country = t.place.country
            self.tw_loc_fullname = t.place.full_name
            self.tw_loc_name = t.place.name
            self.tw_loc_type = t.place.place_type
        self.tw_lang = t.lang
        self.user_location = t.author.location
        self.user_profile_image_url = t.author.profile_image_url
        self.user_lang = t.author.lang
        self.user_description = t.author.description
        self.user_url = t.author.url
        self.user_friends_count = t.author.friends_count
        self.user_followers_count = t.author.followers_count
        self.user_name = t.author.name
        self.user_screen_name = t.author.screen_name
        self.user_listed_count = t.author.listed_count
        self.user_favourites_count = t.author.favourites_count
        self.user_statuses_count = t.author.statuses_count
        self.user_created_at = t.author.created_at.strftime('%Y-%m-%d %H:%M')
        self.user_verified = t.author.verified
        self.user_utc_offset = t.author.utc_offset
        self.user_timezone = t.author.time_zone
        self.user_geo_enabled = t.author.geo_enabled
        self.user_default_profile = t.author.default_profile



    def get_tweet_as_db_json(self):
        json={"api_res":0,
             "tw_full_text": self.tw_full_text,
             "tw_retweet_count": self.tw_retweet_count,
             "tw_favorite_count": self.tw_favorite_count,
             "tw_favorited": self.tw_favorited,
             "tw_hashtags": self.tw_hashtags,
             "tw_source": self.tw_source,
             "tw_geo": self.tw_geo,
             "tw_coordinates": self.tw_coordinates,
             "tw_loc_country": self.tw_loc_country,
             "tw_loc_fullname": self.tw_loc_fullname,
             "tw_loc_name": self.tw_loc_name,
             "tw_loc_type": self.tw_loc_type,
             "tw_lang": self.tw_lang,
             "user_location": self.user_location,
             "user_profile_image_url": self.user_profile_image_url,
             "user_lang": self.user_lang,
             "user_description": self.user_description,
             "user_url": self.user_url,
             "user_friends_count": self.user_friends_count,
             "user_followers_count": self.user_followers_count,
             "user_name": self.user_name,
             "user_screen_name": self.user_screen_name,
             "user_listed_count": self.user_listed_count,
             "user_favourites_count": self.user_favourites_count,
             "user_statuses_count": self.user_statuses_count,
             "user_created_at": self.user_created_at,
             "user_verified": self.user_verified,
             "user_utc_offset": self.user_utc_offset,
             "user_timezone": self.user_timezone,
             "user_geo_enabled": self.user_geo_enabled,
             "user_default_profile": self.user_default_profile}
        return json

