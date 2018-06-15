from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSalesByCustomer")
sc = SparkContext(conf = conf)

def parseLines(line):
	fields = line.split()
	movie = int (fields[1])
	rating = float(fields[2])
	return (movie, rating)

lines = sc.textFile("file:/Users/ewensmith/PythonWork/SparkCourse/ml-100k/u.data")

input = lines.map(parseLines)

totals = input.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[0]))
avgByMovie = totals.mapValues(lambda x: (x[0]/x[1])).map(lambda x: (x[1],x[0])).sortByKey()

results = avgByMovie.collect()

for result in results:
	print(result)