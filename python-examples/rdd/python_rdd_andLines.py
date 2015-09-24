from pyspark import SparkConf, SparkContext
import time


def main():
	"""
	This script will not run if the spark-submit executable, the python code, and the file you are reading in textFile are not in the same partition. 
	Running this code: 
	1. Go to the directory where the file exists
	2. In the spark directory look inside the bin directory, find the spark-submit executable
	3. From the directory of this Python file, run /path/to/spark-submit thisScriptName.py (i.e. with script name as argument)
	"""
	lines = sc.textFile("/media/data/hacks/github-pages-jekyll.md")	# create an RDD
	andLines = lines.filter(lambda line: "and" in line)						# transform the RDD using filter()
	andLines.cache()														# persist the RDD using persist() or cache()
	print("Persisting the andLines RDD.")
	firstAndLine = andLines.first()											# act on the RDD to get the first element (remember lazy evauation) 	
	print("The first line containing 'and' is: %s" % firstAndLine)
	t0 = time.time()
	numAndLines = andLines.count()											# act on the RDD to get the number of elements 
	print("There are %d lines containing 'and'" % numAndLines)
	print("Elapsed counting time with persist(): %4.3f s" % (time.time()-t0))

	lines = sc.textFile("/media/data/hacks/github-pages-jekyll.md")	# create an RDD
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
