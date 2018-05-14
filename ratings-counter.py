from pyspark import SparkConf, SparkContext
import collections

# create objects that allow creation and use of RDD
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# read file
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

# extract the rating from each line in index 2
ratings = lines.map(lambda x: x.split()[2])
# group the ratings as a tuple (rating, count)
result = ratings.countByValue()

#sort rating
sortedResults = collections.OrderedDict(sorted(result.items()))
# loop through each rating and print key vlaue pair
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
