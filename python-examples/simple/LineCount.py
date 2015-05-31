from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

def main():
	"""Running this code: 
	1. Go to the unzipped spark directory 
	2. In the spark directory look inside the bin directory, find the spark-submit executable
	3. Run spark-submit with linecount.py as argument
	"""
	fpath = "/home/nikos/spark-1.3.1-bin-hadoop2.4/README.md"
	lines = sc.textFile(fpath)
	print "There are %d lines in %s" % (lines.count(), fpath)

if __name__=="__main__":
	main()
