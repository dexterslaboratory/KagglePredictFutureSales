package ml.createmodel

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Sales(date: String, date_block_num: Int, shop_id: Int, item_id: Int, item_price: Double, item_cnt_day: Double)

case class Items(item_id: Int, item_category_id: Int)

object ML {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("ML")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]) {
    val salesDf = readCsv
      .load("datasets/sales_train.csv")
      .as[Sales]

    val itemsDf = readCsv.load("datasets/items.csv")
      .select($"item_id".cast("int"), $"item_category_id".cast("int"))
      .as[Items]

    val trainingSet = createTrainingSet(salesDf, itemsDf)
    val atMonthlyLevel = groupByBlock(trainingSet)

    val assembler = new VectorAssembler()
      .setInputCols(Array("date_block_num", "shop_id", "item_id", "item_price", "item_category_id"))
      .setOutputCol("features")

    val withFeatures = assembler.transform(atMonthlyLevel)
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(gbt))

    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxIter, Array(100, 200))
      .addGrid(gbt.stepSize, Array(0.1, 0.001))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(2)
    withFeatures.cache()
    withFeatures.count()
    val cvModel = cv.fit(withFeatures)
    cvModel.save("BoostRegressionModel")

    //    print(cvModel.bestModel.asInstanceOf[PipelineModel].stages(0).asInstanceOf[GBTRegressionModel].getStepSize)
    //    print(cvModel.bestModel.asInstanceOf[PipelineModel].stages(0).asInstanceOf[GBTRegressionModel].getMaxIter)

    val model = CrossValidatorModel.load("BoostRegressionModel")

    val testSet = readCsv.load("datasets/test.csv")
    val testSetComplete = prepareTestSet(testSet, atMonthlyLevel, itemsDf)
    val testWithFeatures = assembler.transform(testSetComplete)

    val predictions = model.transform(testWithFeatures)

    predictions
      .withColumn("ID_new", coalesce($"ID", lit(-1)))
      .drop("ID")
      .withColumnRenamed("ID_new", "ID")
      .withColumn("item_cnt_month", when($"prediction" <= 20, $"prediction").otherwise(lit(20)))
      .select($"ID", $"item_cnt_month")
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("kaggleSubmission.csv")

    spark.stop()
  }

  def prepareTestSet(testSet: DataFrame, atMonthlyLevel: DataFrame, itemsDf: Dataset[Items]): DataFrame = {
    val avgPriceByItem = atMonthlyLevel.groupBy("item_id")
      .agg(avg($"item_price").as("item_price"))

    val avgPriceByCategory = atMonthlyLevel.groupBy("item_category_id")
      .agg(avg($"item_price").as("item_price_by_cat"))

    val withCatId = testSet.join(itemsDf, Seq("item_id"), "left_outer")

    val avgPriceAllItems = atMonthlyLevel
      .select($"item_price").agg(avg("item_price"))
      .first().getDouble(0)

    // coalesce with lit(-1) has been done to change nullable to false
    // this is needed for the vector assembler to work,
    // could also use assembler.setHandleInvalid("skip") (spark 2.4)
    val withPrice = withCatId.join(avgPriceByItem, Seq("item_id"), "left_outer")
      .join(avgPriceByCategory, Seq("item_category_id"), "left_outer")
      .withColumn("estimated_price",
        coalesce($"item_price", $"item_price_by_cat",
          lit(avgPriceAllItems)).as("item_price"))
      .drop("item_price", "item_price_by_cat")
      .withColumnRenamed("estimated_price", "item_price")
      .withColumn("date_block_num", lit(34))
      .withColumn("item_category_id_new", coalesce($"item_category_id", lit(-1)))
      .withColumn("item_id_new", coalesce($"item_id", lit(-1)))
      .withColumn("ID_new", coalesce($"ID", lit(-1)))
      .withColumn("shop_id_new", coalesce($"shop_id", lit(-1)))
      .drop("item_category_id", "item_id", "ID", "shop_id")
      .withColumnRenamed("item_category_id_new", "item_category_id")
      .withColumnRenamed("item_id_new", "item_id")
      .withColumnRenamed("ID_new", "ID")
      .withColumnRenamed("shop_id_new", "shop_id")
    withPrice
  }

  def readCsv = {
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
  }

  def groupByBlock(trainingSet: DataFrame) = {
    trainingSet.groupBy($"date_block_num", $"shop_id", $"item_id")
      .agg(avg($"item_price").as("item_price"),
        sum($"item_cnt_day").as("label"),
        max($"item_category_id").as("item_category_id"))
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