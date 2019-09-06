import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Sales(date: String, date_block_num: Int, shop: Int, item_id: Int, item_price: Double, item_cnt_day: Double)

object ML {

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ML")
      .getOrCreate()
    val df:Dataset[Sales] = trainingSet(spark)



    spark.stop()
  }

  def trainingSet(spark: SparkSession): Dataset[Sales] = {
    import spark.implicits._
    return spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("sales_train.csv")
      .as[Sales]
  }
}