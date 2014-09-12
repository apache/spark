package org.apache.spark.mllib.regression.testsuites

import org.apache.spark.mllib.regression.{RegressionLearner}
import org.apache.spark.mllib.regression.testcases.BaselineComparation._
import org.apache.spark.mllib.util.LocalSparkContext
import org.scalatest.FunSuite


trait CommonRegressionSuite extends FunSuite with LocalSparkContext with RegressionSuite  {

  test("Baseline comparation") {
    testCompareToBaseline(createLearner(), randomRegressionLabeledFeatureSet(sc, 1000, 10), 10)
    testCompareToBaseline(createLearner(), randomRegressionLabeledFeatureSet(sc, 1000, 100), 10)
  }

}
