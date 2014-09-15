package org.apache.spark.mllib.regression.testsuites

import org.apache.spark.mllib.regression.testcases.BaselineComparation._
import org.apache.spark.mllib.util.LocalSparkContext
import org.scalatest.FunSuite

trait GoodRegressionSuite extends FunSuite with LocalSparkContext with RegressionSuite  {

  test("Good baseline comparation") {
    testCompareToBaseline(createLearner(), randomRegressionLabeledFeatureSet(sc, 1000, 10), 100)
    testCompareToBaseline(createLearner(), randomRegressionLabeledFeatureSet(sc, 1000, 100), 100)
  }
}
