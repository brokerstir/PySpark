import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# create rdds
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# read file
input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)

# transform every word to key value pair, and then reduce by unique words
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# flip the key value pair
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
# collect the results
results = wordCountsSorted.collect()

# loop through each result and print
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
