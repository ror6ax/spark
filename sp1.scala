import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Sp1 {
  def main(args: Array[String]) {

	val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
	val sc = new SparkContext(conf)

	val textFile = sc.textFile("data.txt")
	val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey((x,y) => x+y)
	counts.collect().foreach(println)

	sc.stop
  }
}
