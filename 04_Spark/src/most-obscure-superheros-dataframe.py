from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)])

names = spark.read.schema(schema).option(
    "sep", " ").csv("../data/Marvel-names.txt")

lines = spark.read.text("../data/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

min_connection_count = connections.agg(func.min("connections")).first()[0]

min_connections = connections.filter(
    func.col('connections') == min_connection_count)

# mostPopular = single_connections.sort(func.col("connections").desc()).first()

# mostPopularName = names.filter(
#     func.col("id") == mostPopular[0]).select("name").collect()

# join with names
min_connections_with_names = min_connections.join(names, "id")

min_connections_with_names.select("name").show()
# print(mostPopularName[0] + " is the most popular superhero with " +
#       str(mostPopular[1]) + " co-appearances.")
