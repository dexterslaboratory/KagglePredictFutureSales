/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession

object ML {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("ML").getOrCreate()
    val logData = spark.read.textFile(logFile)
    val count = logData.count()
    print(s"Hello the count is: $count")
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    println("Hello")
    spark.stop()
  }
}