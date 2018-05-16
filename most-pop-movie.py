from pyspark import SparkConf, SparkContext

# create objects
conf = SparkConf().setMaster("local").setAppName("MostPopMovie")
sc = SparkContext(conf = conf)

# define function 
def parseLine(line):
    fields = line.split(',')
    movieID = fields[1]
    return (movieID)

# import the data
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
# parse data by calling parseLine function
movies = lines.map(lambda x: (int(x.split()[1]), 1))

mostWatched = movies.reduceByKey(lambda x, y: x + y)


results = mostWatched.collect();

for result in results:
    print(result)
