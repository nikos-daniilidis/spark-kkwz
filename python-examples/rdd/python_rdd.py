from pyspark import SparkConf, SparkContext
import time


def main():
	"""Running this code: 
	1. Go to the unzipped spark directory 
	2. In the spark directory look inside the bin directory, find the spark-submit executable
	3. Run spark-submit with linecount.py as argument
	"""
	lines = sc.textFile("/home/nikos/spark-1.3.1-bin-hadoop2.4/README.md")	# create an RDD
	andLines = lines.filter(lambda line: "and" in line)						# transform the RDD using filter()
	andLines.cache()														# persist the RDD using persist() or cache()
	print("Persisting the andLines RDD.")
	firstAndLine = andLines.first()											# act on the RDD to get the first element (remember lazy evauation) 	
	print("The first line containing 'and' is: %s" % firstAndLine)
	t0 = time.time()
	numAndLines = andLines.count()											# act on the RDD to get the number of elements 
	print("There are %d lines containing 'and'" % numAndLines)
	print("Elapsed counting time with persist(): %4.3f s" % (time.time()-t0))

	lines = sc.textFile("/home/nikos/spark-1.3.1-bin-hadoop2.4/README.md")	# create an RDD
	andLines = lines.filter(lambda line: "and" in line)						# transform the RDD using filter()
	# this time do not persist the RDD
	firstAndLine = andLines.first()											# act on the RDD to get the first element (remember lazy evauation) 	
	print("The first line containing 'and' is: %s" % firstAndLine)
	t0 = time.time()
	numAndLines = andLines.count()											# act on the RDD to get the number of elements 
	print("There are %d lines containing 'and'" % numAndLines)
	print("Elapsed counting time without persist(): %4.3f s" % (time.time()-t0))


if __name__=="__main__":
	conf = SparkConf().setMaster("local").setAppName("Chapter 3: RDDs")
	sc = SparkContext(conf=conf)
	main()
