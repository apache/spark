package org.apache.spark.ml.regression

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.{LogisticRegressionModel, LogisticRegression}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.classification.LogisticRegressionSuite._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class IsotonicRegressionSuite extends SparkFunSuite with MLlibTestSparkContext {
  private val schema = StructType(
    Array(
      StructField("label", DoubleType),
      StructField("features", DoubleType),
      StructField("weight", DoubleType)))

  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val data = sc.parallelize(
      Seq(
        Row(1d, 0d, 1d),
        Row(2d, 1d, 1d),
        Row(3d, 2d, 1d),
        Row(1d, 3d, 1d),
        Row(6d, 4d, 1d),
        Row(17d, 5d, 1d),
        Row(16d, 6d, 1d),
        Row(17d, 7d, 1d),
        Row(18d, 8d, 1d)))

    dataset = sqlContext.createDataFrame(data, schema)
  }

  test("isotonic regression") {
    val trainer = new IsotonicRegression()
      .setIsotonicParam(true)

    val model = trainer.fit(dataset)

    val predictions = model
      .transform(dataset)
      .select("prediction").map {
        case Row(pred) => pred
      }
      .collect()

    assert(predictions === Array(1, 2, 2, 2, 6, 16.5, 16.5, 17, 18))

    assert(model.model.boundaries === Array(0, 1, 3, 4, 5, 6, 7, 8))
    assert(model.model.predictions === Array(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0))
    assert(model.model.isotonic)
  }

  test("params") {
    val ir = new IsotonicRegression
    ParamsSuite.checkParams(ir)
    val model = ir.fit(dataset)
    ParamsSuite.checkParams(model)
  }

  test("isotonic regression: default params") {
    val ir = new IsotonicRegression()
    assert(ir.getLabelCol === "label")
    assert(ir.getFeaturesCol === "features")
    assert(ir.getWeightCol === "weight")
    assert(ir.getPredictionCol === "prediction")
    assert(ir.getIsotonicParam === true)

    val model = ir.fit(dataset)
    model.transform(dataset)
      .select("label", "features", "prediction", "weight")
      .collect()

    assert(model.getLabelCol === "label")
    assert(model.getFeaturesCol === "features")
    assert(model.getWeightCol === "weight")
    assert(model.getPredictionCol === "prediction")
    assert(model.getIsotonicParam === true)
    assert(model.hasParent)
  }
}
