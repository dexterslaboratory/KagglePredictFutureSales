/* SimpleApp.scala */

import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ML {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ML").getOrCreate()
    import spark.implicits._
    val df = trainingSet(spark)
    print(s"Hello:  ${df.count()}")
    spark.stop()
  }

  def count(df: DataFrame): Long = {
    df.count()
  }

  def trainingSet(spark: SparkSession): DataFrame = {
    return spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("sales_train.csv")
  }
}