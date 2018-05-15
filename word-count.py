from pyspark import SparkConf, SparkContext

# create initial context
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# read text file
input = sc.textFile("file:///sparkcourse/book.txt")
# split text file into words based on white space
words = input.flatMap(lambda x: x.split())
# count occurence of each word
wordCounts = words.countByValue()

# loop through each word, print and its count
for word, count in wordCounts.items():
    # py trick: encode in ascii
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))

# code should be improved because words with caps are counted unique as same word in no caps
# punctuation is included that occurs at end of word, but it should not be
# words should be sorted by count