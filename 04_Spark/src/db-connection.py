from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F

# conf = SparkConf().setMaster("local").setAppName("db-connection")
# sc = SparkContext(conf=conf)

spark = SparkSession.builder.appName(
    "Python Spark SQL basic example").getOrCreate()

jdbcDF = spark.read.format("jdbc").options(
    # jdbc:postgresql://<host>:<port>/<database>
    url='jdbc:postgresql://liveai.cd94ktqj8jv1.eu-central-1.rds.amazonaws.com:5432/property_listings',
    dbtable="(SELECT * FROM live_ai_v2.locations WHERE state='Hessen') as locations",
    user='postgres',
    password='liveAI123',
    driver='org.postgresql.Driver').load()

jdbcDF.show()
