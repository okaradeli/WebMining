import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error,mean_absolute_error, r2_score
import hashtag_analyzer.utils as utils
import hashtag_analyzer.tweet_analyzer as tweet_analyzer
#import sklearn.model_selection.train_test_split as train_test_split
from sklearn.model_selection import train_test_split
from math import sqrt
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.linear_model import Ridge
from sklearn.linear_model import Lasso


TWEET_EFFECT_TRAIN_TABLE='processed_1'
TWEET_EFFECT_TEST_TABLE='processed_test'

#feature and target column(s) for the model
feature_columns = ['hashtag_avg_retweet', 'hashtag_char_length', 'hashtag_frequency',
                   'hashtag_is_all_upper', 'hashtag_token_count', 'token_avg_retweet',
                   'token_char_length', 'token_frequency', 'token_is_all_upper',
                   'tweet_hashtag_count', 'tweet_is_first_time_hashtag',
                   'tweet_total_char_length', 'user_retweet_ratio']
target_column = 'tweet_retweet_count'


#This prediction is used to have a base model for comparison purposes.
#The target ( retweet_count ) is equal to the (user_retweet_ratio) of the user
def base_model_pred():
    print("Running base model v1")
    tweets_predict = utils.load_database_to_df(tweet_analyzer.MONGO_DATABASE, TWEET_EFFECT_TRAIN_TABLE)
    tweets_X = tweets_predict[feature_columns]
    tweets_Y = tweets_predict[target_column]

    X_train,X_true,Y_train,Y_true = train_test_split(tweets_X,tweets_Y,test_size=0.25,random_state=42)
    X_train, X_val, Y_train, Y_val= train_test_split(X_train, Y_train, test_size=0.20, random_state=1)

    tweets_y_pred = X_true['user_retweet_ratio']

    print("BASE number of predicted samples:" + str(len(Y_true)))
    print("BASE Model Root Mean squared error: %.2f" % sqrt(mean_squared_error(Y_true, tweets_y_pred)))
    # Explained variance score: 1 is perfect prediction
    print('BASE Model Variance score: %.2f' % r2_score(Y_true, tweets_y_pred))


#The deviced model with inputs related with hashtags and trying to predict the retweet_count of a tweet
def linear_regression_pred():
    print("Running linear regression model v1")

    # Load the tweets dataset
    tweets_train = utils.load_database_to_df(tweet_analyzer.MONGO_DATABASE, TWEET_EFFECT_TRAIN_TABLE)
    #tweets_predict = utils.load_database_to_df(tweet_analyzer.MONGO_DATABASE, TWEET_EFFECT_TEST_TABLE)

    tweets_X = tweets_train[feature_columns]
    tweets_Y= tweets_train[target_column]

    X_train,X_pred,Y_train,Y_pred = train_test_split(tweets_X,tweets_Y,test_size=0.25,random_state=42)
    X_train, X_val, Y_train, Y_val= train_test_split(X_train, Y_train, test_size=0.20, random_state=1)

    robust_scaler = RobustScaler()
    #Scale features with robust scaler
    #print("Scaling with Train/Test data with Robust Scaler")
    #X_train = robust_scaler.fit_transform(X_train)
    #X_val = robust_scaler.transform(X_val)
    #X_pred = robust_scaler.transform(X_pred)


    # Create linear regression object

    regr = linear_model.LinearRegression()
    #regr = linear_model.Ridge(alpha=0.1)
    #regr = Lasso(alpha=1e-10,max_iter=1e5)
    regr.fit(X_train, Y_train)

    # Make predictions using the validation set
    tweets_y_val = regr.predict(X_val)

    # ------------------
    # TRAIN/VALIDATION
    # ------------------
    print('TRAIN Coefficients: \n', list(zip(regr.coef_, feature_columns)))
    print('TRAIN Intercept: ', regr.intercept_)
    # The root mean squared error
    #print("Mean squared error: %.2f" % mean_squared_error(Y_test, tweets_y_pred))
    print("TRAIN Root Mean squared error: %.2f" % sqrt(mean_squared_error(Y_val, tweets_y_val)))
    print('TRAIN Variance score: %.2f' % r2_score(Y_val, tweets_y_val))
    print('\n')


    #-------------------
    # TEST
    # -------------------
    print("TEST Number of predicted samples:"+str(len(Y_pred)))
    tweets_y_pred = regr.predict(X_pred)
    print("TEST Root Mean squared error: %.2f" % sqrt(mean_squared_error(Y_pred, tweets_y_pred)))
    print('TEST Variance score: %.2f' % r2_score(Y_pred, tweets_y_pred))

if __name__ == "__main__":
    utils.set_pandas_settings()
    linear_regression_pred()
    #base_model_pred()



