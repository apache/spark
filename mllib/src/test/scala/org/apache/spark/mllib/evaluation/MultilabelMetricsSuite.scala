package org.apache.spark.mllib.evaluation

import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite


class MultilabelMetricsSuite extends FunSuite with LocalSparkContext {
  test("Multilabel evaluation metrics") {
    /*
    * Documents true labels (5x class0, 3x class1, 4x class2):
    * doc 0 - predict 0, 1 - class 0, 2
    * doc 1 - predict 0, 2 - class 0, 1
    * doc 2 - predict none - class 0
    * doc 3 - predict 2 - class 2
    * doc 4 - predict 2, 0 - class 2, 0
    * doc 5 - predict 0, 1, 2 - class 0, 1
    * doc 6 - predict 1 - class 1, 2
    *
    * predicted classes
    * class 0 - doc 0, 1, 4, 5 (total 4)
    * class 1 - doc 0, 5, 6 (total 3)
    * class 2 - doc 1, 3, 4, 5 (total 4)
    *
    * true classes
    * class 0 - doc 0, 1, 2, 4, 5 (total 5)
    * class 1 - doc 1, 5, 6 (total 3)
    * class 2 - doc 0, 3, 4, 6 (total 4)
    *
    */
    val scoreAndLabels:RDD[(Set[Double], Set[Double])] = sc.parallelize(
      Seq((Set(0.0, 1.0), Set(0.0, 2.0)),
        (Set(0.0, 2.0), Set(0.0, 1.0)),
        (Set(), Set(0.0)),
        (Set(2.0), Set(2.0)),
        (Set(2.0, 0.0), Set(2.0, 0.0)),
        (Set(0.0, 1.0, 2.0), Set(0.0, 1.0)),
        (Set(1.0), Set(1.0, 2.0))), 2)
    val metrics = new MultilabelMetrics(scoreAndLabels)
    val delta = 0.00001
    val precision0 = 4.0 / (4 + 0)
    val precision1 = 2.0 / (2 + 1)
    val precision2 = 2.0 / (2 + 2)
    val recall0 = 4.0 / (4 + 1)
    val recall1 = 2.0 / (2 + 1)
    val recall2 = 2.0 / (2 + 2)
    val f1measure0 = 2 * precision0 * recall0 / (precision0 + recall0)
    val f1measure1 = 2 * precision1 * recall1 / (precision1 + recall1)
    val f1measure2 = 2 * precision2 * recall2 / (precision2 + recall2)
    val microPrecisionClass = (4.0 + 2.0 + 2.0) / (4 + 0 + 2 + 1 + 2 + 2)
    val microRecallClass = (4.0 + 2.0 + 2.0) / (4 + 1 + 2 + 1 + 2 + 2)
    val microF1MeasureClass = 2 * microPrecisionClass * microRecallClass / (microPrecisionClass + microRecallClass)

    val macroPrecisionDoc = 1.0 / 7 * (1.0 / 2 + 1.0 / 2 + 0 + 1.0 / 1 + 2.0 / 2 + 2.0 / 3 + 1.0 / 1.0)
    val macroRecallDoc = 1.0 / 7 * (1.0 / 2 + 1.0 / 2 + 0 / 1 + 1.0 / 1 + 2.0 / 2 + 2.0 / 2 + 1.0 / 2)

    println("Ev" + metrics.macroPrecisionDoc)
    println(macroPrecisionDoc)
    println("Ev" + metrics.macroRecallDoc)
    println(macroRecallDoc)
    assert(math.abs(metrics.precisionClass(0.0) - precision0) < delta)
    assert(math.abs(metrics.precisionClass(1.0) - precision1) < delta)
    assert(math.abs(metrics.precisionClass(2.0) - precision2) < delta)
    assert(math.abs(metrics.recallClass(0.0) - recall0) < delta)
    assert(math.abs(metrics.recallClass(1.0) - recall1) < delta)
    assert(math.abs(metrics.recallClass(2.0) - recall2) < delta)
    assert(math.abs(metrics.f1MeasureClass(0.0) - f1measure0) < delta)
    assert(math.abs(metrics.f1MeasureClass(1.0) - f1measure1) < delta)
    assert(math.abs(metrics.f1MeasureClass(2.0) - f1measure2) < delta)

    assert(math.abs(metrics.microPrecisionClass - microPrecisionClass) < delta)
    assert(math.abs(metrics.microRecallClass - microRecallClass) < delta)
    assert(math.abs(metrics.microF1MeasureClass - microF1MeasureClass) < delta)

    assert(math.abs(metrics.macroPrecisionDoc - macroPrecisionDoc) < delta)
    assert(math.abs(metrics.macroRecallDoc - macroRecallDoc) < delta)


  }

}
