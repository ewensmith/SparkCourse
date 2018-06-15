#-------------------------------------------------------------------------
from pyspark import SparkConf, SparkContext
import collections

#-------------------------------------------------------------------------
# INITIALIZE SPARK

# create an object to configure spark
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

# create a spark context
sc = SparkContext(conf = conf)
#-------------------------------------------------------------------------
# SPARK OBJECT MANIPULATION

# create an RDD called lines using the text file method within the spark context
lines = sc.textFile("file:/Users/ewensmith/PythonWork/SparkCourse/ml-100k/u.data")

# map the lines RDD to a new ratings RDD and split values into columns, selecting column 2
ratings = lines.map(lambda x: x.split()[2])

# count the number of times each distinct value occurs
result = ratings.countByValue()

#-------------------------------------------------------------------------
# PYTHON OBJECT MANIPULATION

# sort the results list
sortedResults = collections.OrderedDict(sorted(result.items()))
# print the results
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
