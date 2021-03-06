import argparse as ap
import numpy as np
import pandas as pd
import math
from scipy.stats import norm
from pymongo import MongoClient

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline

from bert_sklearn import BertClassifier

bert_params={}
random_state=0

target_fields = ['body', 'grade', 'sentiment_score', 'review_length', 'biased']

def split_data(df, p=0.8, rstate=random_state):
    '''Splits DF into a training and testing set where the
    training set has P samples and the testing set has 1 - P
    samples.

    Args:
        df: The dataframe to split.
        p: The percentage of samples in the training set.
    '''
    training = df.sample(frac=p, random_state=rstate)
    testing = df.sample(frac=1-p, random_state=rstate)
    return training, testing


def biased(x, mu, std):
    if x < mu - math.sqrt(2) * std:
        return True
    if x > mu + math.sqrt(2) * std:
        return True
    else:
        return False


def map_sentiment(sentiment):
    if sentiment == 'Very negative':
        return -2
    elif sentiment == 'Negative':
        return -1
    elif sentiment == 'Neutral':
        return 0
    elif sentiment == 'Positive':
        return 1
    elif sentiment == 'Very positive':
        return 2
    else:
        return 0

def convert_sentiments(sentiments):
    import statistics
    return statistics.mean(list(map(map_sentiment, sentiments)))


# setup argparse
parser = ap.ArgumentParser()
parser.add_argument('mongo_uri', help='The MongoDB uri for the database server.')
parser.add_argument('database', help='The database to retrieve review records from.')
parser.add_argument('collection', help='The collection to retrieve review records from.')
args = parser.parse_args()

# setup the database
client = MongoClient(args.mongo_uri)
database = client[args.database]
collection = database[args.collection]

# read the critic reviews dataset
critic_reviews = collection.find({}).limit(10000)
reviews_frame = pd.DataFrame(list(critic_reviews))

# preprocess the dataset
reviews_frame['sentiment_score'] = reviews_frame['snlp_sentiments'].apply(convert_sentiments)
reviews_frame['grade'] = reviews_frame['grade'].apply(pd.to_numeric)

# generate the reviews length field
reviews_frame['review_length'] = reviews_frame['cleaned'].apply(lambda s : len(s.split(' ')))

# generate bias labels using the chebyshev inequality
sscore_mean = reviews_frame['sentiment_score'].mean()
sscore_std = reviews_frame['sentiment_score'].std()
reviews_frame['biased'] = np.vectorize(biased)(reviews_frame['sentiment_score'], sscore_mean, sscore_std)

# generate the sampling frame
sample_frame = reviews_frame[target_fields]

# generate the testing and training sets
train_df, test_df = split_data(sample_frame)

# generate the training inputs and output
train_x = train_df['body']
train_y = train_df['biased']

# generate the testing inputs and outputs
test_x = test_df['body']
test_y = test_df['biased']

def bert(train_x, train_y, test_x, test_y):
    bert = BertClassifier(**bert_params)
    bert.fit(train_x, train_y.values.ravel())
    print('BERT Accuracy:', bert.score(test_x, test_y.values.ravel()))

bert(train_x, train_y, test_x, test_y)
