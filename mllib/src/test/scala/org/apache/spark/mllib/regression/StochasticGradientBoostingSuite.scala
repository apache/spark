package org.apache.spark.mllib.regression

import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Variance
import org.apache.spark.mllib.util.{LinearDataGenerator, LocalSparkContext}
import org.apache.spark.rdd.{RDD, DoubleRDDFunctions}
import org.apache.spark.util.Utils
import org.scalatest.FunSuite

class StochasticGradientBoostingSuite extends FunSuite with LocalSparkContext {

  /**
   * Test if we can correctly learn on random data
   */
  test("stochastic gradient boosting") {
    val parsedData = randomLabeledPoints()
    val model = StochasticGradientBoosting.train(parsedData, Algo.Regression, Variance, 3)
    checkModel(parsedData, model)
  }

  test("test serialization") {
    val parsedData = randomLabeledPoints()
    val model = StochasticGradientBoosting.train(parsedData, Algo.Regression, Variance, 3)
    checkModel(parsedData, Utils.deserialize[StochasticGradientBoostingModel](Utils.serialize(model)))
  }

  def checkModel(parsedData: RDD[LabeledPoint], model: RegressionModel) {
    val valuesAndPredictions = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val actualValues = parsedData.map(l => l.label)
    val mean = new DoubleRDDFunctions(actualValues).mean()
    val meanError = new DoubleRDDFunctions(actualValues.map(i => math.pow(i - mean, 2))).mean()
    val MSE = valuesAndPredictions.map { case (v, p) => math.pow(v - p, 2)}
    val error = new DoubleRDDFunctions(MSE).mean()
    assert(meanError / error > 100)
  }

  def randomLabeledPoints() = {
    sc.parallelize(LinearDataGenerator.generateLinearInput(
      3.0, Array(10.0, 10.0), 100, 34), 2).cache()
  }
}
