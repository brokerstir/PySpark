from pyspark import SparkConf, SparkContext

# create objects
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# define function to later parse through data
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# read file set as lines rdd
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
# new rdd calls parseLine
rdd = lines.map(parseLine)

# combine operations, first mapValues to set value as a tuple of number of friends and value 1, the key is still age
# then reduceByKey to aggregate by age, and end up with new rdd
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# compute average
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# round the average to nearest int
averagesByAge = averagesByAge.mapValues(lambda x: round(x))
# collect results and print
results = averagesByAge.collect()
for result in results:
    print(result)
