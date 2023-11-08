/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.evaluation

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.rdd.RDD

class MultilabelMetricsSuite extends SparkFunSuite with MLlibTestSparkContext {
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
    val scoreAndLabels: RDD[(Array[Double], Array[Double])] = sc.parallelize(
      Seq((Array(0.0, 1.0), Array(0.0, 2.0)),
        (Array(0.0, 2.0), Array(0.0, 1.0)),
        (Array.empty[Double], Array(0.0)),
        (Array(2.0), Array(2.0)),
        (Array(2.0, 0.0), Array(2.0, 0.0)),
        (Array(0.0, 1.0, 2.0), Array(0.0, 1.0)),
        (Array(1.0), Array(1.0, 2.0))), 2)
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
    val sumTp = 4 + 2 + 2
    assert(sumTp == (1 + 1 + 0 + 1 + 2 + 2 + 1))
    val microPrecisionClass = sumTp.toDouble / (4 + 0 + 2 + 1 + 2 + 2)
    val microRecallClass = sumTp.toDouble / (4 + 1 + 2 + 1 + 2 + 2)
    val microF1MeasureClass = 2.0 * sumTp.toDouble /
      (2 * sumTp.toDouble + (1 + 1 + 2) + (0 + 1 + 2))
    val macroPrecisionDoc = 1.0 / 7 *
      (1.0 / 2 + 1.0 / 2 + 0 + 1.0 / 1 + 2.0 / 2 + 2.0 / 3 + 1.0 / 1.0)
    val macroRecallDoc = 1.0 / 7 *
      (1.0 / 2 + 1.0 / 2 + 0 / 1 + 1.0 / 1 + 2.0 / 2 + 2.0 / 2 + 1.0 / 2)
    val macroF1MeasureDoc = (1.0 / 7) *
      2 * ( 1.0 / (2 + 2) + 1.0 / (2 + 2) + 0 + 1.0 / (1 + 1) +
        2.0 / (2 + 2) + 2.0 / (3 + 2) + 1.0 / (1 + 2) )
    val hammingLoss = (1.0 / (7 * 3)) * (2 + 2 + 1 + 0 + 0 + 1 + 1)
    val strictAccuracy = 2.0 / 7
    val accuracy = 1.0 / 7 * (1.0 / 3 + 1.0 /3 + 0 + 1.0 / 1 + 2.0 / 2 + 2.0 / 3 + 1.0 / 2)
    assert(metrics.precision(0.0) ~== precision0 absTol delta)
    assert(metrics.precision(1.0) ~== precision1 absTol delta)
    assert(metrics.precision(2.0) ~== precision2 absTol delta)
    assert(metrics.recall(0.0) ~== recall0 absTol delta)
    assert(metrics.recall(1.0) ~== recall1 absTol delta)
    assert(metrics.recall(2.0) ~== recall2 absTol delta)
    assert(metrics.f1Measure(0.0) ~== f1measure0 absTol delta)
    assert(metrics.f1Measure(1.0) ~== f1measure1 absTol delta)
    assert(metrics.f1Measure(2.0) ~== f1measure2 absTol delta)
    assert(metrics.microPrecision ~== microPrecisionClass absTol delta)
    assert(metrics.microRecall ~== microRecallClass absTol delta)
    assert(metrics.microF1Measure ~== microF1MeasureClass absTol delta)
    assert(metrics.precision ~== macroPrecisionDoc absTol delta)
    assert(metrics.recall ~== macroRecallDoc absTol delta)
    assert(metrics.f1Measure ~== macroF1MeasureDoc absTol delta)
    assert(metrics.hammingLoss ~== hammingLoss absTol delta)
    assert(metrics.subsetAccuracy ~== strictAccuracy absTol delta)
    assert(metrics.accuracy ~== accuracy absTol delta)
    assert(metrics.labels.sameElements(Array(0.0, 1.0, 2.0)))
  }
}
