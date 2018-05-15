from pyspark import SparkConf, SparkContext

# create objects
conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

# define function to parse through each line extracting station ID, type, and temp converted to F
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

# import the data
lines = sc.textFile("file:///SparkCourse/1800.csv")
# parse data by calling parseLine function
parsedLines = lines.map(parseLine)

# filter to lines to exclude everything except lines with TMAX as type
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])

# every line has type TMAX now, so we don't need that column anymore
# create new rdd with only station ID and temp as the two columns
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

# create rdd with only one line for each station ID that contains max temp recorded for that station
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))

# collect the results and print out, formatted to two decimals
results = maxTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
