from pyspark import SparkConf, SparkContext


def parseLine(line):
    fields = line.split(",")
    id = int(fields[0])
    amount = float(fields[2])
    return (id, amount)


conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf=conf)

lines = sc.textFile("../data/customer-orders.csv")

rdd = lines.map(parseLine)
totalAmountSpendByCustomer = rdd.reduceByKey(lambda x, y: x+y)
totalAmountSpendByCustomerSorted = totalAmountSpendByCustomer.map(
    lambda x: (x[1], x[0])).sortByKey().collect()
for result in totalAmountSpendByCustomerSorted:
    print(result)
