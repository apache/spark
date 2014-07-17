package org.apache.spark.mllib.feature

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.LocalSparkContext


import org.scalatest.FunSuite

class ChiSquaredSuite extends FunSuite with LocalSparkContext {

  def labeledData = sc.parallelize(
    Seq( new LabeledPoint(0.0, Vectors.dense(Array(8.0, 7.0, 0.0))),
         new LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 6.0))),
         new LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0))),
         new LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 5.0)))
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

  test("Chi Squared test") {

    val chi = new ChiSquaredFeatureSelection(labeledData, 2)
    chi.filter.foreach(println)

  }


}
