from pyspark import SparkContext

def main():
	logFile = "/media/data/spark-1.3.1-bin-hadoop2.6/README.md"
	sc = SparkContext("local", "Simple App")
	logData = sc.textFile(logFile).cache()

	numAs = logData.filter(lambda s: "a" in s).count()
	numBs = logData.filter(lambda s: "b" in s).count()

	print "Lines containing 'a': %d\nLines containing 'b': %d" % (numAs, numBs)

if __name__=="__main__":
	main()
