from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

# returns key value rdd where key is id and value is number of times id occurs with other heroes
def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1) # -1 to subtract key id

# function that returns tuple (id, name) form names file
def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

# read names file and create rdd that calls function
names = sc.textFile("file:///SparkCourse/marvel-names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("file:///SparkCourse/marvel-graph.txt")

# call function
pairings = lines.map(countCoOccurences)
# hero id is not unique per line, so reduce by key to get unique key and total cooccurence
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
# need to flip so count is key, because we want max count
#flipped = totalFriendsByCharacter.map(lambda (x,y) : (y,x))
flipped = totalFriendsByCharacter.map( lambda x : (x[1], x[0]))

# extract the max or most popular
mostPopular = flipped.max()

# Use namesRDD to extract the name assoicate with ID in field 1
mostPopularName = namesRdd.lookup(mostPopular[1])[0]

#print(mostPopularName + " is the most popular superhero, with " + \
    #str(mostPopular[0]) + " co-appearances.")
    
print(mostPopularName , " is the most popular superhero, with " , \
    str(mostPopular[0]) , " co-appearances.")
