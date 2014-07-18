package org.apache.spark.mllib.feature

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.{MLUtils, LocalSparkContext}


import org.scalatest.FunSuite

class ChiTest(data: RDD[LabeledPoint]) extends java.io.Serializable
with ContingencyTableCalculator {
  val chi = tables(data).map { case ( fIndex, table)
    => (fIndex, ChiSquared(table)) }.collect().sortBy(-_._2)
}

class ChiSquaredSuite extends FunSuite with LocalSparkContext {

  def labeledData = sc.parallelize(
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
   */

  test("Chi Squared test") {

    val chi = new ChiSquaredFeatureSelection(labeledData, 2)
    chi.filter.foreach(println)

  }


  test("Big test") {
    val dData = MLUtils.loadLibSVMFile(sc, "c:/ulanov/res/indigo/data-test-spark.libsvm", true)
    val chiTest = new ChiTest(dData)
    chiTest.chi.foreach(println)
  }


}
