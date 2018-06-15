from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSalesByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
	fields = line.split(',')
	cust = int(fields[0])
	spend = float(fields[2])
	return (cust, spend)

lines = sc.textFile("file:/Users/ewensmith/PythonWork/SparkCourse/customer-orders.csv")

rdd = lines.map(parseLine)

totals = rdd.reduceByKey(lambda x, y: (x + y)).sortByKey()

results = totals.collect()

for result in results:
	custId = str(result[0])
	totalSpend = str(result[1])
	print custId + ':\t\t' + totalSpend