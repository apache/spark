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

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.util.LocalSparkContext

class MulticlassMetricsSuite extends FunSuite with LocalSparkContext {
  test("Multiclass evaluation metrics") {
    /*
     * Confusion matrix for 3-class classification with total 9 instances:
     * |2|1|1| true class0 (4 instances)
     * |1|3|0| true class1 (4 instances)
     * |0|0|1| true class2 (1 instance)
     */
    val confusionMatrix = Matrices.dense(3, 3, Array(2, 1, 0, 1, 3, 0, 1, 0, 1))
    val labels = Array(0.0, 1.0, 2.0)
    val predictionAndLabels = sc.parallelize(
      Seq((0.0, 0.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0), (1.0, 1.0),
        (1.0, 1.0), (1.0, 1.0), (2.0, 2.0), (2.0, 0.0)), 2)
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val delta = 0.0000001
    val fpRate0 = 1.0 / (9 - 4)
    val fpRate1 = 1.0 / (9 - 4)
    val fpRate2 = 1.0 / (9 - 1)
    val precision0 = 2.0 / (2 + 1)
    val precision1 = 3.0 / (3 + 1)
    val precision2 = 1.0 / (1 + 1)
    val recall0 = 2.0 / (2 + 2)
    val recall1 = 3.0 / (3 + 1)
    val recall2 = 1.0 / (1 + 0)
    val f1measure0 = 2 * precision0 * recall0 / (precision0 + recall0)
    val f1measure1 = 2 * precision1 * recall1 / (precision1 + recall1)
    val f1measure2 = 2 * precision2 * recall2 / (precision2 + recall2)
    val f2measure0 = (1 + 2 * 2) * precision0 * recall0 / (2 * 2 * precision0 + recall0)
    val f2measure1 = (1 + 2 * 2) * precision1 * recall1 / (2 * 2 * precision1 + recall1)
    val f2measure2 = (1 + 2 * 2) * precision2 * recall2 / (2 * 2 * precision2 + recall2)

    assert(metrics.confusionMatrix.toArray.sameElements(confusionMatrix.toArray))
    assert(math.abs(metrics.falsePositiveRate(0.0) - fpRate0) < delta)
    assert(math.abs(metrics.falsePositiveRate(1.0) - fpRate1) < delta)
    assert(math.abs(metrics.falsePositiveRate(2.0) - fpRate2) < delta)
    assert(math.abs(metrics.precision(0.0) - precision0) < delta)
    assert(math.abs(metrics.precision(1.0) - precision1) < delta)
    assert(math.abs(metrics.precision(2.0) - precision2) < delta)
    assert(math.abs(metrics.recall(0.0) - recall0) < delta)
    assert(math.abs(metrics.recall(1.0) - recall1) < delta)
    assert(math.abs(metrics.recall(2.0) - recall2) < delta)
    assert(math.abs(metrics.fMeasure(0.0) - f1measure0) < delta)
    assert(math.abs(metrics.fMeasure(1.0) - f1measure1) < delta)
    assert(math.abs(metrics.fMeasure(2.0) - f1measure2) < delta)
    assert(math.abs(metrics.fMeasure(0.0, 2.0) - f2measure0) < delta)
    assert(math.abs(metrics.fMeasure(1.0, 2.0) - f2measure1) < delta)
    assert(math.abs(metrics.fMeasure(2.0, 2.0) - f2measure2) < delta)

    assert(math.abs(metrics.recall -
      (2.0 + 3.0 + 1.0) / ((2 + 3 + 1) + (1 + 1 + 1))) < delta)
    assert(math.abs(metrics.recall - metrics.precision) < delta)
    assert(math.abs(metrics.recall - metrics.fMeasure) < delta)
    assert(math.abs(metrics.recall - metrics.weightedRecall) < delta)
    assert(math.abs(metrics.weightedFalsePositiveRate -
      ((4.0 / 9) * fpRate0 + (4.0 / 9) * fpRate1 + (1.0 / 9) * fpRate2)) < delta)
    assert(math.abs(metrics.weightedPrecision -
      ((4.0 / 9) * precision0 + (4.0 / 9) * precision1 + (1.0 / 9) * precision2)) < delta)
    assert(math.abs(metrics.weightedRecall -
      ((4.0 / 9) * recall0 + (4.0 / 9) * recall1 + (1.0 / 9) * recall2)) < delta)
    assert(math.abs(metrics.weightedFMeasure -
      ((4.0 / 9) * f1measure0 + (4.0 / 9) * f1measure1 + (1.0 / 9) * f1measure2)) < delta)
    assert(math.abs(metrics.weightedFMeasure(2.0) -
      ((4.0 / 9) * f2measure0 + (4.0 / 9) * f2measure1 + (1.0 / 9) * f2measure2)) < delta)
    assert(metrics.labels.sameElements(labels))
  }
}
