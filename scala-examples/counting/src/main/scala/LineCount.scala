import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object LineCount {
	def main(args: Array[String]) {
		val inputFile = args(0)
		val outputFile = args(1)

		val conf = new SparkConf().setAppName("LineCount")
		val sc = new SparkContext(conf)

		val input = sc.textFile(inputFile)
		val numl = input.count()
		val fl = input.first()

		println("There are %s lines in file %s".format(numl, inputFile))
	}
}
