import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.when

case class Sales(date: String, date_block_num: Int, shop_id: Int, item_id: Int, item_price: Double, item_cnt_day: Double)

case class Items(item_id: Int, item_category_id: Int)

object ML {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("ML")
    .getOrCreate()

  import spark.implicits._
  def main(args: Array[String]) {


    val salesDf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("datasets/sales_train.csv")
      .as[Sales]

    val itemsDf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("datasets/items.csv")
      .select($"item_id".cast("int"), $"item_category_id".cast("int"))
      .as[Items]

    val trainingSet = createTrainingSet(salesDf, itemsDf)

    spark.stop()
  }

  def createTrainingSet(salesDf: Dataset[Sales], itemsDf: Dataset[Items]): DataFrame = {
    salesDf.alias("left").join(
      itemsDf.alias("right"),
      $"left.item_id" === $"right.item_id",
      "left_outer")
      .drop($"right.item_id")
      .withColumn("item_category_id_new", when($"item_category_id".isNull, -1)
        .otherwise($"item_category_id"))
      .drop($"item_category_id")
      .withColumnRenamed("item_category_id_new", "item_category_id")
  }
}