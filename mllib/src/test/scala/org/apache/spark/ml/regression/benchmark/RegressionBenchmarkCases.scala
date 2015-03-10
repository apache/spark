package org.apache.spark.ml.regression.benchmark

import org.apache.spark.SparkContext
import org.apache.spark.ml.regression.{RegressionModel, Regressor}
import org.apache.spark.mllib.classification.LogisticRegressionSuite._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.FunSuite

object RegressionBenchmarkCases extends FunSuite {

  def testLogisticInput(regressor: Regressor[_, _, _ <: RegressionModel[_, _]],
                        sc: SparkContext,
                        expectedError: Double = 0.7): Unit = {
    val input = generateLogisticInput(1.0, 1.0, nPoints = 100, seed = 42)
    testRegressor(regressor, input, sc, expectedError)
  }

  // http://archive.ics.uci.edu/ml/datasets/Computer+Hardware
  def testMachineData(regressor: Regressor[_, _, _ <: RegressionModel[_, _]],
                      sc: SparkContext,
                      expectedError: Double = 5000): Unit = {
    val input = fromFile("machine.data").map(row => {
      new LabeledPoint(row.last.toDouble, Vectors.dense(row.slice(2, 8).map(_.toDouble)))
    })
    testRegressor(regressor, input, sc, expectedError)
  }


  def fromFile(name: String): List[Array[String]] = {
    scala.io.Source.fromInputStream(getClass.getResourceAsStream("/machine.data")).getLines().map(_.split(",")).toList
  }

  def testRegressor(regressor: Regressor[_, _, _ <: RegressionModel[_, _]],
                    input: Seq[LabeledPoint], sc: SparkContext,
                    expectedError: Double) = {
    val dataset = new SQLContext(sc).createDataFrame(input)
    val model = regressor.fit(dataset)
    val result = model
      .transform(dataset)
      .select("label", "prediction")
    val error = result.map({ case Row(label: Double, prediction: Double) => {
      Math.pow(label - prediction, 2)
    }
    }).mean()
    assert(error < expectedError)
  }

}
