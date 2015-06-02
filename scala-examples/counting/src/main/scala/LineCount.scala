import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object LineCount {
	// To run this program as an application:
	// 1. Create a build.sbt file with:
	//		- name and version of the application
	//		- scala version
	//		- spark version
	// 2. Directory  structure should be:
	//		application/
	//			build.sbt
	//			/src/main/scala/yourApplicationScript.scala
	// 3. Build the application using sbt. From the application directory:
	// 		sbt clean package
	//	  sbt will build a package and print the path on terminal, e.g.:
	//		~/code/spark-kkwz/scala-examples/counting-test/target/scala-2.10/learning-spark-mini-example_2.10-0.0.1.jar
	// 4. Run the application using spark-submit
	//		$SPARK_HOME/bin/spark-submit --class "AppName" ./target/...(as above) ./firstArgument ./secondArgument
	//	  e.g. 
	//		~/spark-1.3.1-bin-hadoop2.4/bin/spark-submit --class "LineCount" ./target/scala-2.10/learning-spark-mini-example_2.10-0.0.1.jar ./build.sbt ./linecounts
	//	  Output is: 
	//		There are 10 lines in file ./build.sbt     
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
