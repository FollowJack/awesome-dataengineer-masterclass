
from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))


lines = spark.sparkContext.textFile("../data/fakefriends.csv")
people = lines.map(mapper)

# converts from RDD to dataframe + cache to keep it in memory
schemaPeople = spark.createDataFrame(people).cache()
# in order to query through the dataframe
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
