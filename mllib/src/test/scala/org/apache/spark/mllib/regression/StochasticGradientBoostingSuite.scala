package org.apache.spark.mllib.regression

import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Variance
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.DoubleRDDFunctions
import org.scalatest.FunSuite
import org.apache.spark.util.Utils

/**
 * Created by olgaoskina on 05/09/14.
 *
 */
class StochasticGradientBoostingSuite extends FunSuite with LocalSparkContext {

  val NUM_ROWS: Long = 1000
  val NUM_COLS: Int = 100
  val REQUIRED_ERROR: Double = 0.5
  val NUM_PARTITIONS: Int = 2

  /**
   * Test if we can correctly learn on random data
   */
  test("stochastic gradient boosting") {
    var dataX = RandomRDDs.normalVectorRDD(sc, NUM_ROWS, NUM_COLS, NUM_PARTITIONS).cache()
    var dataY = RandomRDDs.uniformRDD(sc, NUM_ROWS, NUM_PARTITIONS)
    var parsedData = dataY.zip(dataX).map{case (d, v) => new LabeledPoint(d, v)}
    val model = StochasticGradientBoosting.train(parsedData, Algo.Regression, Variance, 3)

    dataX = RandomRDDs.normalVectorRDD(sc, NUM_ROWS, NUM_COLS, NUM_PARTITIONS).cache()
    dataY = RandomRDDs.uniformRDD(sc, NUM_ROWS, NUM_PARTITIONS)
    parsedData = dataY.zip(dataX).map{case (d, v) => new LabeledPoint(d, v)}

    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map{ case(v, p) => math.pow(v - p, 2)}
    val error = new DoubleRDDFunctions(MSE).mean()

    assert(error < REQUIRED_ERROR,
      s"Calculated mean squared error $error but required $REQUIRED_ERROR.")
  }

  test("serialize") {
    val dataX = RandomRDDs.normalVectorRDD(sc, NUM_ROWS, NUM_COLS, NUM_PARTITIONS).cache()
    val dataY = RandomRDDs.uniformRDD(sc, NUM_ROWS, NUM_PARTITIONS)
    val parsedData = dataY.zip(dataX).map{case (d, v) => new LabeledPoint(d, v)}
    val boostingModel = StochasticGradientBoosting.train(parsedData, Algo.Regression, Variance, 3)
    val deserializeModel: StochasticGradientBoostingModel = Utils.deserialize[StochasticGradientBoostingModel](Utils.serialize(boostingModel))
    assert(deserializeModel.equals(boostingModel))
  }
}
