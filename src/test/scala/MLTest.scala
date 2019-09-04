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

class MLTest extends FunSuite with DataFrameComparer with SparkSessionTestWrapper {
  import spark.implicits._
  test("sample test to check count"){
    val df = Seq(
      ("ro","w1"),
      ("ro","w1")
    ).toDF("col1", "col2")

    val dfCount = ML.count(df)
   assert(dfCount == 2)
  }
}
