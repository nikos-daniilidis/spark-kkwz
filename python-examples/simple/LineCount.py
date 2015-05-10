from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

def main():
	fpath = "/home/nikos/spark-1.3.1-bin-hadoop2.4/README.md"
	lines = sc.textFile(fpath)
	print "There are %d lines in %s" % (lines.count(), fpath)

if __name__=="__main__":
	main()
