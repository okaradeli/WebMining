import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
import tweetanalyzer.utils as utils
import tweetanalyzer.tweet_analyzer as tweet_analyzer
#import sklearn.model_selection.train_test_split as train_test_split
from sklearn.model_selection import train_test_split
from math import sqrt
from sklearn.preprocessing import StandardScaler, RobustScaler


TWEET_EFFECT_TRAIN_TABLE='processed_1'
TWEET_EFFECT_TEST_TABLE='processed_test'

#feature and target column(s) for the model
feature_columns = ['hashtag_avg_retweet', 'hashtag_char_length', 'hashtag_frequency',
                   'hashtag_is_all_upper', 'hashtag_token_count', 'token_avg_retweet',
                   'token_char_length', 'token_frequency', 'token_is_all_upper',
                   'tweet_hashtag_count', 'tweet_is_first_time_hashtag',
                   'tweet_total_char_length', 'user_retweet_ratio']
target_column = 'tweet_retweet_count'


#This prediction is used to have a base model.
#The target ( retweet_count ) is equal to the (user_retweet_ratio) of the user
def base_model_pred():
    print("Running base model v1")
    tweets_predict = utils.load_database_to_df(tweet_analyzer.MONGO_DATABASE, tweet_analyzer.MONGO_TWEET_EFFECT_TABLE)

    tweets_Y = tweets_predict[target_column]
    tweets_y_pred = tweets_predict['user_retweet_ratio']

    print("Root Mean squared error: %.2f" % sqrt(mean_squared_error(tweets_Y, tweets_y_pred)))

    # Explained variance score: 1 is perfect prediction
    print('Variance score: %.2f' % r2_score(tweets_Y, tweets_y_pred))


#The deviced model with inputs related with hashtags and trying to predict the retweet_count of a tweet
def linear_regression_pred():
    print("Running linear regression model v1")

    # Load the tweets dataset
    tweets_train = utils.load_database_to_df(tweet_analyzer.MONGO_DATABASE, TWEET_EFFECT_TRAIN_TABLE)
    tweets_predict = utils.load_database_to_df(tweet_analyzer.MONGO_DATABASE, TWEET_EFFECT_TEST_TABLE)

    tweets_X = tweets_train[feature_columns]
    tweets_Y= tweets_train[target_column]

    X_train,X_test,Y_train,Y_test = train_test_split(tweets_X,tweets_Y,test_size=0.25,random_state=42)

    X_pred=tweets_predict[feature_columns]
    Y_pred = tweets_predict[target_column]

    # Create linear regression object
    regr = linear_model.LinearRegression()

    # Train the model using the training sets
    regr.fit(X_train, Y_train)

    # Make predictions using the testing set
    tweets_y_pred = regr.predict(X_test)

    # TRAIN/VALIDATION coefficients
    print('TRAIN Coefficients: \n', regr.coef_)
    # The root mean squared error
    #print("Mean squared error: %.2f" % mean_squared_error(Y_test, tweets_y_pred))
    print("TRAIN Root Mean squared error: %.2f" % sqrt(mean_squared_error(Y_test, tweets_y_pred)))
    # Explained variance score: 1 is perfect prediction
    print('TRAIN Variance score: %.2f' % r2_score(Y_test, tweets_y_pred))

    # Train the model using the training sets

    # Make predictions using the testing set
    tweets_y_pred = regr.predict(X_pred)

    # TEST coefficients
    print('TEST Coefficients: \n', regr.coef_)
    # The root mean squared error
    #print("Mean squared error: %.2f" % mean_squared_error(Y_test, tweets_y_pred))
    print("TEST Root Mean squared error: %.2f" % sqrt(mean_squared_error(Y_pred, tweets_y_pred)))
    # Explained variance score: 1 is perfect prediction
    print('TEST Variance score: %.2f' % r2_score(Y_pred, tweets_y_pred))





if __name__ == "__main__":
    utils.set_pandas_settings()
    linear_regression_pred()
    #base_model_pred()



