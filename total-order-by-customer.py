from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(',')
    custID = int(fields[0])
    orderAmount = float(fields[2])    
    return (custID, orderAmount)

# create rdds
conf = SparkConf().setMaster("local").setAppName("OrdersByCust")
sc = SparkContext(conf = conf)

# read file set as lines rdd
lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
# new rdd calls parseLine
rdd = lines.map(parseLine)

# reduce by key while adding values up for each unique key
orderTotals = rdd.reduceByKey(lambda x, y: x + y)
# round the values to two decimal points
orderTotals = orderTotals.mapValues(lambda x: round(x, 2))
# flip the key value pair and sort, so order totals are sorted from lowest to highest
orderTotalsSorted = orderTotals.map(lambda x: (x[1], x[0])).sortByKey()

# collect the results
results = orderTotalsSorted.collect()
# loop through each result and print
for result in results:
    print(result)
