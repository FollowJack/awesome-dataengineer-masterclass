import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
import codecs
from pyspark.ml.recommenation import ALS
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql import SparkSession


def load_movie_names():
    movie_names = {}
    with codecs.open("../data/ml-100/u.item", "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split("|")
            movie_names[int(fields[0])] = fields[1]
    return movie_names


spark = SparkSession.builder.appname("ALSexample").getOrCreate()

movie_schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

names = load_movie_names()

ratings = spark.read.option("sep", "\t").schema(
    movie_schema).csv("../ml-100k/u.data")

print("Training recommendation model...")

als = AlS().setMaxIter(5).setRegParam(0.01).setUserCol(
    "userID").setItemCol("movieID").setRatingCol("rating")

model = als.fit(ratings)

user_id = int(sys.argv[1])
user_schema = StructType([StructField("userID", IntegerType(), True)])
users = spark.createDataFrame([[user_id, ]], user_schema)

recommendations = model.recommendfForUserSubsets(user, 10).collect()

for user_recs in recommendations:
    # userRecs is (userID, [Row(movieId, rating), Row(movieID, rating)...])
    my_recs = user_recs[1]
    for rec in my_recs:  # my Recs is just the column of recs for the user
        # For each rec in the list, extract the movie ID and rating
        movie = rec[0]
        rating = rec[1]
        movie_name = names[movie]
        print(movie_name + str(rating))
