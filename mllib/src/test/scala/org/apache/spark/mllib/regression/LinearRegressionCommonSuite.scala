package org.apache.spark.mllib.regression

import org.apache.spark.mllib.regression.testsuites.{GoodRegressionSuite, CommonRegressionSuite}

class LinearRegressionCommonSuite extends CommonRegressionSuite with GoodRegressionSuite {

  def createLearner() : RegressionLearner = {
    LinearRegressionWithSGD.createLearner(10)
  }

}
