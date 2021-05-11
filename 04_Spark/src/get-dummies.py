import pyspark.sql.functions as F
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.createDataFrame([
    (1, "A", "X1"),
    (2, "B", "X2"),
    (3, "B", "X3"),
    (1, "B", "X3"),
    (2, "C", "X2"),
    (3, "C", "X2"),
    (1, "C", "X1"),
    (1, "B", "X1"),
], ["ID", "TYPE", "CODE"])

types = df.select("TYPE").distinct().rdd.flatMap(lambda x: x).collect()
codes = df.select("CODE").distinct().rdd.flatMap(lambda x: x).collect()
types_expr = [F.when(F.col("TYPE") == ty, 1).otherwise(
    0).alias("e_TYPE_" + ty) for ty in types]
codes_expr = [F.when(F.col("CODE") == code, 1).otherwise(
    0).alias("e_CODE_" + code) for code in codes]
df = df.select("ID", "TYPE", "CODE", *types_expr+codes_expr)
df.show()
