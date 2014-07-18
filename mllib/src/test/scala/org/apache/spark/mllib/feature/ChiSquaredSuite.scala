package org.apache.spark.mllib.feature

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.LocalSparkContext


import org.scalatest.FunSuite

class ChiSquaredSuite extends FunSuite with LocalSparkContext {

  private class ChiTest(data: RDD[LabeledPoint])
    extends java.io.Serializable with ContingencyTableCalculator {
    val chi = tables(data).map { case ( fIndex, table) =>
      (fIndex, ChiSquared(table)) }.collect()
  }

  lazy val labeledDiscreteData = sc.parallelize(
    Seq( new LabeledPoint(0.0, Vectors.dense(Array(8.0, 7.0, 0.0))),
      new LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 6.0))),
      new LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0))),
      new LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 5.0)))
    ), 2)

  /*
   *  Contingency tables
   *  feature0 = {8.0, 0.0}
   *  class  0 1 2
   *    8.0||1|0|1|
   *    0.0||0|2|0|
   *
   *  feature1 = {7.0, 9.0}
   *  class  0 1 2
   *    7.0||1|0|0|
   *    9.0||0|2|1|
   *
   *  feature2 = {0.0, 6.0, 8.0, 5.0}
   *  class  0 1 2
   *    0.0||1|0|0|
   *    6.0||0|1|0|
   *    8.0||0|1|0|
   *    5.0||0|0|1|
   *
   *  Use chi-squared calculator from Internet
   */

  test("Chi Squared values and contingency tables test") {
    val preComputedChi2 = Map( (0 -> 4.0), (1 -> 4.0), (2 -> 8.0))
    val computedChi2 = new ChiTest(labeledDiscreteData).chi
    val delta = 0.000001
    assert(computedChi2.forall{ case (featureIndex, chi2) =>
      (preComputedChi2(featureIndex) - chi2) <= delta})

  }

  test("Chi Squared feature selection test") {
    val preFilteredData =
      Set( new LabeledPoint(0.0, Vectors.dense(Array(0.0))),
        new LabeledPoint(1.0, Vectors.dense(Array(6.0))),
        new LabeledPoint(1.0, Vectors.dense(Array(8.0))),
        new LabeledPoint(2.0, Vectors.dense(Array(5.0)))
      )
    val filteredData = new ChiSquaredFeatureSelection(labeledDiscreteData, 1).filter.collect.toSet
    assert(filteredData == preFilteredData)
  }
}
