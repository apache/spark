package org.apache.spark.mllib.regression

import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Variance
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.{DoubleRDDFunctions, RDD}
import org.apache.spark.util.Utils
import org.scalatest.FunSuite
import scala.util.Random

/**
 * Created by olgaoskina on 05/09/14.
 *
 */
class StochasticGradientBoostingSuite extends FunSuite with LocalSparkContext {

  val NUM_ROWS: Long = 1000
  val NUM_COLS: Int = 100
  val NUM_PARTITIONS: Int = 2
  val MAX_ATTEMPTS: Int = 3
  val MEAN_ERROR_CORRELATION_COEFFICIENT: Double = 1.5

  def randomRdd(): RDD[LabeledPoint] = {
    var seed = Random.nextLong()
    //        sc.parallelize(1 to NUM_ROWS, 10).map(i => {
    //            val features = (1 to NUM_COLS).map(_ => Random.nextDouble()).toArray
    //            val result = features.reduceLeft((a, b) => {
    //            if (seed % 3 == 0) {
    //              seed = seed / 3
    //              a * b
    //            } else {
    //              seed = seed / 2
    //              a + b
    //            }
    //          })
    //          new LabeledPoint(result, Vectors.dense(features))
    //        })
    val dataX = RandomRDDs.normalVectorRDD(sc, NUM_ROWS, NUM_COLS, NUM_PARTITIONS).cache()
    val dataY = RandomRDDs.uniformRDD(sc, NUM_ROWS, NUM_PARTITIONS)
    dataY.zip(dataX).map { case (d, v) => new LabeledPoint(d, v)}
  }

  /**
   * Test if we can correctly learn on random data
   */
  test("stochastic gradient boosting") {
    var testIsOk = false
    var i = 0
    while (i < MAX_ATTEMPTS && !testIsOk){
      var parsedData: RDD[LabeledPoint] = randomRdd()
      val model = StochasticGradientBoosting.train(parsedData, Algo.Regression, Variance, 3)
      parsedData = randomRdd()
      val valuesAndPreds = parsedData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      val dataY: RDD[Double] = parsedData.map(l => l.label)
      val mean = new DoubleRDDFunctions(dataY).mean()
      var meanError = new DoubleRDDFunctions(dataY.map(i => math.pow(i - mean, 2))).mean()
      meanError = meanError * MEAN_ERROR_CORRELATION_COEFFICIENT
      val MSE = valuesAndPreds.map { case (v, p) => math.pow(v - p, 2)}
      val error = new DoubleRDDFunctions(MSE).mean()
      if (error < meanError) {
        testIsOk = true
      } else {
        println("Attempt: " + (i + 1) + " is failed. Calculated mean squared error " + error + " but required " + meanError)
      }
      i += 1
    }
    if (!testIsOk) {
      fail("Failed " + MAX_ATTEMPTS + " attempts")
    }
  }

  test("serialize") {
    var testIsOk = false
    var i = 0
    while (i < MAX_ATTEMPTS && !testIsOk) {
      var parsedData: RDD[LabeledPoint] = randomRdd()
      val model = StochasticGradientBoosting.train(parsedData, Algo.Regression, Variance, 3)
      val deserializeModel: StochasticGradientBoostingModel = Utils.deserialize[StochasticGradientBoostingModel](Utils.serialize(model))
      parsedData = randomRdd()
      val valuesAndPreds = parsedData.map { point =>
        val prediction = deserializeModel.predict(point.features)
        (point.label, prediction)
      }
      val dataY: RDD[Double] = parsedData.map(l => l.label)
      val mean = new DoubleRDDFunctions(dataY).mean()
      var meanError = new DoubleRDDFunctions(dataY.map(i => math.pow(i - mean, 2))).mean()
      meanError = meanError * MEAN_ERROR_CORRELATION_COEFFICIENT
      val MSE = valuesAndPreds.map { case (v, p) => math.pow(v - p, 2)}
      val error = new DoubleRDDFunctions(MSE).mean()
      if (error < meanError) {
        testIsOk = true
      } else {
        println("Attempt: " + (i + 1) + " is failed. Calculated mean squared error " + error + " but required " + meanError)
      }
      i += 1
    }
    if (!testIsOk) {
      fail("Failed " + MAX_ATTEMPTS + " attempts")
    }
  }
}
