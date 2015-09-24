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
	lines = sc.textFile("/media/data/hacks/github-pages-jekyll.md")	# create an RDD, the iterators are lines
	splitLines = lines.map(lambda line: line.split(" "))		# transform the RDD, the iterators are lists (each list is a list of words)
	splitWords = lines.flatMap(lambda line: line.split(" "))		# transform the RDD using filter()
	print("______")
	print("\    /")
	print(" \  / ")
	print("  \/  ")
	print("")
	print("The first line is: %s" % splitLines.first())
	print("")
	print("The first word is: %s" % splitWords.filter(lambda wrd: len(wrd) > 4).first())




if __name__=="__main__":
	conf = SparkConf().setMaster("local").setAppName("Chapter 3: RDDs")
	sc = SparkContext(conf=conf)
	main()
