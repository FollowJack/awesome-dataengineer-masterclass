import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row

from pyspark.sql.functions import *

# conf = SparkConf().setMaster("local").setAppName("Procurement")

spark = SparkSession.builder.appName("Procurement").getOrCreate()
# spark.sparkContext.setLogLevel('WARN')

print("myspark", spark)
# Always 10 partitions
spark.conf.set("spark.sql.shuffle.partitions", 8)

# GOAL:
# procurements.lot_cpv --> 30237460-1_Computer keyboards
# suppliers.lot_procur_type --> Filter Below Threshold

# PLAN
# Cache frst to see size and repartition with coalesce
# FILTER FIRST
# Filter null values where, filter
# LOWER funtion for lot_procur_type

# read csv
# sc.textfile --> creates RDD
# spark.read.format('csv').load(name='rawdata.csv', schema=peopleSchema) --> for all formats to create dataframe
# spark.read.csv() --> for csv only creates dataframe
df_suppliers = spark.read.csv("../data/big_data/Suppliers.csv",
                              header=True).coalesce(1)  # --> infers schema


# df.show(5, truncate=False) # does not truncate
df_procurements = spark.read.csv(
    "../data/big_data/Competitive_procurements.csv", header=True).coalesce(3)

df_procurements_supplier = df_procurements.join(
    df_suppliers, df_procurements.lot_id == df_suppliers.lot_id, how="left")

# ID 2-4 ACTION join and SHOW supplier file
df_procurements_supplier.show(5, False)

df_suppliers_filtered = df_suppliers.filter(
    lower(col("lot_procur_type")) == "below threshold")
# ID 5 ACTION Filter and SHOW filtered suppliers
df_suppliers_filtered.show(5, False)

# df_procurements_new = df_procurements.withColumn(
#     "lot_cpv_id", df_procurements.select("lot_cpv").rdd.map(lambda cpv: cpv.split("-")[0]))
df_procurements_new = df_procurements.filter(col("lot_cpv").isNotNull()).withColumn(
    "lot_cpv_id", split(col("lot_cpv"), "-")[0]).orderBy("lot_cpv_id")
# ID 6 ACTION filter, withColumn and SHOW new id
df_procurements_new.select("lot_cpv_id", "lot_cpv").show()

# ID 7 ACTION COUNT
suppliers_count = df_suppliers.count()
# ID 8 Action count
suppliers_filtered_count = df_suppliers_filtered.count()
suppliers_difference = suppliers_count - suppliers_filtered_count

print("COUNT_MY", suppliers_count, suppliers_filtered_count, suppliers_difference)

time.sleep(600)  # Sleep for 3 seconds
print("done")
