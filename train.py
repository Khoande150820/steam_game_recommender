from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import requests, json, os, sys, time, re
from sklearn.metrics.pairwise import linear_kernel, cosine_similarity

spark = SparkSession.builder.appName('spark-rec').config("configuration_key",
                                                         "configurable_value").enableHiveSupport().getOrCreate()

sc = spark.sparkContext

ratings_raw_data = sc.textFile("data/user_game_rating.csv")
ratings_raw_data_header = ratings_raw_data.take(1)[0]
ratings_data = ratings_raw_data.filter(lambda line: line != ratings_raw_data_header) \
    .map(lambda line: line.split(",")).map(
    lambda tokens: (int(tokens[1]), int(tokens[2]), int(float(tokens[3])))).cache()
# we should assume each column here, by position [1],[2],[3] and data characters map() function demands every input
# must return a value，flatMapValues expand the output result values set of input, put those into a bigger RDD
rddTraining, rddValidating, rddTesting = ratings_data.randomSplit([6, 2, 2], seed=1001)
#   Split Training data, Validation data and Testing data by portion of 60%, 20%, 20%

# Build the Spark ALS model
rank = 10
# The size of the feature vector used; the minimum value is 10, the minimum value of the feature vector,
#the better the model produced, but it also costs more calculation cost
numIterations = 10
#Iteration numbers
alpha=0.01
#Confidence values in ALS，default 1.0

#lambda
#Regularization parameter，DEFAULT 0.01

############################################################################################
# Build the recommendation model using Alternating Least Squares based on implicit ratings #
############################################################################################

model = ALS.trainImplicit(rddTraining, 10, 10,alpha=0.01)
model.save(sc, "model/ALS_model.model")

