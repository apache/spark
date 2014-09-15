package org.apache.spark.mllib.regression.testsuites

import org.apache.spark.mllib.regression.RegressionLearner

trait RegressionSuite {

  def createLearner() : RegressionLearner

}
