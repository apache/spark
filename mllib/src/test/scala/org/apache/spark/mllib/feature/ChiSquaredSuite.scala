package org.apache.spark.mllib.feature

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.LocalSparkContext


import org.scalatest.FunSuite

class ChiSquaredSuite extends FunSuite with LocalSparkContext {

  def labeledData = sc.parallelize(
    Seq( new LabeledPoint(0.0, Vectors.dense(Array(8.0, 9.0, 0.0))),
         new LabeledPoint(1.0, Vectors.dense(Array(0.0, 5.0, 6.0))),
         new LabeledPoint(1.0, Vectors.dense(Array(0.0, 7.0, 8.0))),
         new LabeledPoint(2.0, Vectors.dense(Array(8.0, 4.0, 5.0)))
    ), 2)

  /*
   *  feature#   0    1    2
   *  presence: Y/N  Y/N  Y/N
   *    class_0|1|0||1|0||0|1
   *    class_1|0|2||2|0||2|0
   *    class_2|1|0||1|0||1|0
   *
   *   class#      0    1    2
   *   presence:  Y/N  Y/N  Y/N
   *   feature_0||1|1||0|2||1|1|
   *  ^feature_0||0|2||2|0||0|2|
   *   feature_1||1|2||2|2||1|3|
   *  ^feature_1||0|0||0|0||0|0|
   *   feature_2||0|3||2|1||1|2|
   *  ^feature_2||1|0||0|1||0|1|
   *
   */
  test("Feature class combinations test") {
    val indexByLabelMap = Map((0.0 -> 0), (1.0 -> 1), (2.0 -> 2))
    val featureLabelCombinations = sc.parallelize(
      Seq( (0 -> Array((1, 0), (0, 2), (1, 0))),
           (1 -> Array((1, 0), (2, 0), (1, 0))),
           (2 -> Array((0, 1), (2, 0), (1, 0)))
      ), 2)
    class CombinationsCalculatorTest(labeledData: RDD[LabeledPoint]) extends CombinationsCalculator {
      def combinations = featureLabelCombinations(labeledData)
      def index = indexByLabelMap(labeledData)
    }
    val combinationsTest = new CombinationsCalculatorTest(labeledData)
    assert(indexByLabelMap == combinationsTest.index)
    val flc = featureLabelCombinations.collectAsMap()
    assert(combinationsTest.combinations.collectAsMap().forall {
      case (key, value) => flc(key).deep.sameElements(value)
    })
  }

  private def sqr(x: Int) = x * x

  test("Chi Squared feature selection test") {
    val chi2ValuesByClass = sc.parallelize(
      Seq( ((0, 0.0),
              (1 + 1 + 0 + 2) * sqr(1 * 2 - 1 * 0).toDouble /
              ((1 + 0) * (1 + 1) * (1 + 2) * (0 + 2))),
           ((0, 1.0),
              (0 + 2 + 2 + 0) * sqr(0 * 0 - 2 * 2).toDouble /
              ((0 + 2) * (0 + 2) * (0 + 2) * (0 + 2))),
           ((0, 2.0),
              (1 + 1 + 0 + 2) * sqr(1 * 2 - 1 * 0).toDouble /
              ((1 + 0) * (1 + 1) * (2 + 0) * (2 + 1))),
           ((1, 0.0),
              (1 + 2 + 0 + 0) * sqr(1 * 0 - 2 * 0).toDouble),
           ((1, 1.0),
              (2 + 2 + 0 + 0) * sqr(2 * 0 - 2 * 0).toDouble),
           ((1, 2.0),
              (1 + 3 + 0 + 0) * sqr(1 * 0 - 3 * 0).toDouble),
           ((2, 0.0),
              (0 + 3 + 1 + 0) * sqr(0 * 0 - 3 * 1).toDouble /
              ((0 + 1) * (0 + 3) * (0 + 1) * (0 + 3))),
           ((2, 1.0),
              (2 + 1 + 1 + 0) * sqr(2 * 1 - 1 * 0).toDouble /
              ((2 + 0) * (2 + 1) * (1 + 0) * (1 + 1))),
           ((2, 2.0),
              (1 + 2 + 1 + 0) * sqr(1 * 1 - 2 * 0).toDouble /
              ((1 + 0) * (1 + 2) * (1 + 0) * (1 + 2)))
      ), 2)
    val chiSquared = new ChiSquared(labeledData)
    val c2vbc = chi2ValuesByClass.collectAsMap()
    assert(chiSquared.chiSquaredValues.collectAsMap().forall {
      case (key, value) => c2vbc(key) == value
    })

  }


}
