package org.apache.spark.mllib.regression.baseline

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.RegressionModel
import org.apache.spark.rdd.RDD


class MeanRegressionModel(mean: Double) extends RegressionModel{

  def predict(testData: RDD[Vector]): RDD[Double] = {
    testData.map(v => mean)
  }

  def predict(testData: Vector): Double = {
    mean
  }

}
