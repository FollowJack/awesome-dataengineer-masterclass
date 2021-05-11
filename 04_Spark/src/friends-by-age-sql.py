from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
# conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
# sc = SparkContext(conf=conf)

spark = SparkSession.builder.appName("FriendsByAgeSQL").getOrCreate()

friends = spark.read.option("header", "true").option(
    "inferSchema", "true").csv("../data/fakefriends-header.csv")

friends.createOrReplaceTempView("friends")

averagesByAge = spark.sql(
    "SELECT age,AVG(friends) FROM friends GROUP BY age ORDER BY age").show()

spark.stop()

# rdd = lines.map(parseLine)
# totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(
#     lambda x, y: (x[0] + y[0], x[1] + y[1]))
# averagesByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])
# results = averagesByAge.collect()
# for result in results:
#     print(result)
