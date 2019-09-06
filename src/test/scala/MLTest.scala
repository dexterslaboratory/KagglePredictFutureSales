import org.scalatest.FunSuite
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}

case class Sales(date: String, date_block_num: Int, shop_id: Int, item_id: Int, item_price: Double, item_cnt_day: Double)

case class Items(item_id: Int, item_category_id: Int)

class MLTest extends FunSuite with DataFrameComparer with SparkSessionTestWrapper {

  import spark.implicits._

  test("createTrainingSet should join sales and item datasets using item_id") {
    val salesDf = Seq(
      Sales("1/1/2013", 0, 111, 1022, 20.5, 12.0),
      Sales("1/2/2013", 1, 112, 1000, 20.5, 12.0)
    ).toDS()

    val itemsDf = Seq(
      Items(1022, 22)
    ).toDS()

    val expectedDs = Seq(
      ("1/1/2013", 0, 111, 1022, 20.5, 12.0, 22),
      ("1/2/2013", 1, 112, 1000, 20.5, 12.0, -1)
    ).toDF()

    val actualDs = ML.createTrainingSet(salesDf, itemsDf)
    assertSmallDataFrameEquality(actualDs, expectedDs, ignoreNullable = true, ignoreColumnNames = true)
  }

  test("groupByBlock should sum over daily sales") {
    val trainingSet = Seq(
      ("1/1/2013", 0, 111, 1022, 20.5, 12.0, 22),
      ("4/1/2013", 0, 111, 1022, 20.5, 12.0, 22),
      ("4/1/2013", 0, 112, 1022, 22.5, 1.0, 5)
    ).toDF("date", "date_block_num", "shop_id", "item_id", "item_price", "item_cnt_day", "item_category_id")

    val expectedDs = Seq(
      (0, 111, 1022, 20.5, 24.0, 22),
      (0, 112, 1022, 22.5, 1.0, 5)
    ).toDF()

    val actualDs = ML.groupByBlock(trainingSet)
    assertSmallDataFrameEquality(actualDs, expectedDs, ignoreNullable = true, ignoreColumnNames = true)
  }
}
