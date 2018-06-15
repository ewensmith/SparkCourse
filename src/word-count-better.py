import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normaliseWords(text):
	return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("file:///Users/ewensmith/PythonWork/sparkcourse/book.txt")
words = input.flatMap(normaliseWords)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x,y:x+y)
wordCountSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

results = wordCountSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print word + ":\t\t" + count
