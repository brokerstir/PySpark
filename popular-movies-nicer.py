# this script will use introduce broadcase variables for looking up the movie name
# broadcast variables allow for efficient performance becasue they keep look up data on the cluster
from pyspark import SparkConf, SparkContext

# function that parses file to put movie names in python dictionary
# maps movie IDs to names
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# boilerplate
conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

# object that returns the broadcast on cluster
nameDict = sc.broadcast(loadMovieNames())

# import the data, map movie IDs and reduce by key while counting occurence of each movie
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

# flipp the tuple from (id, count) to (count, id) and sort
flipped = movieCounts.map( lambda x : (x[1], x[0]))
sortedMovies = flipped.sortByKey()

# use the broadcast object nameDict to transform each line to (name, count)
#sortedMoviesWithNames = sortedMovies.map(lambda (count, movie) : (nameDict.value[movie], count))
sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print (result)
