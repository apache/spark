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
import org.apache.spark.ml.linalg.Matrices
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class MulticlassMetricsSuite extends SparkFunSuite with MLlibTestSparkContext {

  private val delta = 1e-7

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
    val tpRate0 = 2.0 / (2 + 2)
    val tpRate1 = 3.0 / (3 + 1)
    val tpRate2 = 1.0 / (1 + 0)
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

    assert(metrics.confusionMatrix.asML ~== confusionMatrix relTol delta)
    assert(metrics.truePositiveRate(0.0) ~== tpRate0 relTol delta)
    assert(metrics.truePositiveRate(1.0) ~== tpRate1 relTol delta)
    assert(metrics.truePositiveRate(2.0) ~== tpRate2 relTol delta)
    assert(metrics.falsePositiveRate(0.0) ~== fpRate0 relTol delta)
    assert(metrics.falsePositiveRate(1.0) ~== fpRate1 relTol delta)
    assert(metrics.falsePositiveRate(2.0) ~== fpRate2 relTol delta)
    assert(metrics.precision(0.0) ~== precision0 relTol delta)
    assert(metrics.precision(1.0) ~== precision1 relTol delta)
    assert(metrics.precision(2.0) ~== precision2 relTol delta)
    assert(metrics.recall(0.0) ~== recall0 relTol delta)
    assert(metrics.recall(1.0) ~== recall1 relTol delta)
    assert(metrics.recall(2.0) ~== recall2 relTol delta)
    assert(metrics.fMeasure(0.0) ~== f1measure0 relTol delta)
    assert(metrics.fMeasure(1.0) ~== f1measure1 relTol delta)
    assert(metrics.fMeasure(2.0) ~== f1measure2 relTol delta)
    assert(metrics.fMeasure(0.0, 2.0) ~== f2measure0 relTol delta)
    assert(metrics.fMeasure(1.0, 2.0) ~== f2measure1 relTol delta)
    assert(metrics.fMeasure(2.0, 2.0) ~== f2measure2 relTol delta)

    assert(metrics.accuracy ~==
      (2.0 + 3.0 + 1.0) / ((2 + 3 + 1) + (1 + 1 + 1)) relTol delta)
    assert(metrics.accuracy ~== metrics.weightedRecall relTol delta)
    val weight0 = 4.0 / 9
    val weight1 = 4.0 / 9
    val weight2 = 1.0 / 9
    assert(metrics.weightedTruePositiveRate ~==
      (weight0 * tpRate0 + weight1 * tpRate1 + weight2 * tpRate2) relTol delta)
    assert(metrics.weightedFalsePositiveRate ~==
      (weight0 * fpRate0 + weight1 * fpRate1 + weight2 * fpRate2) relTol delta)
    assert(metrics.weightedPrecision ~==
      (weight0 * precision0 + weight1 * precision1 + weight2 * precision2) relTol delta)
    assert(metrics.weightedRecall ~==
      (weight0 * recall0 + weight1 * recall1 + weight2 * recall2) relTol delta)
    assert(metrics.weightedFMeasure ~==
      (weight0 * f1measure0 + weight1 * f1measure1 + weight2 * f1measure2) relTol delta)
    assert(metrics.weightedFMeasure(2.0) ~==
      (weight0 * f2measure0 + weight1 * f2measure1 + weight2 * f2measure2) relTol delta)
    assert(metrics.labels === labels)
  }

  test("Multiclass evaluation metrics with weights") {
    /*
     * Confusion matrix for 3-class classification with total 9 instances with 2 weights:
     * |2 * w1|1 * w2         |1 * w1| true class0 (4 instances)
     * |1 * w2|2 * w1 + 1 * w2|0     | true class1 (4 instances)
     * |0     |0              |1 * w2| true class2 (1 instance)
     */
    val w1 = 2.2
    val w2 = 1.5
    val tw = 2.0 * w1 + 1.0 * w2 + 1.0 * w1 + 1.0 * w2 + 2.0 * w1 + 1.0 * w2 + 1.0 * w2
    val confusionMatrix = Matrices.dense(3, 3,
      Array(2 * w1, 1 * w2, 0, 1 * w2, 2 * w1 + 1 * w2, 0, 1 * w1, 0, 1 * w2))
    val labels = Array(0.0, 1.0, 2.0)
    val predictionAndLabelsWithWeights = sc.parallelize(
      Seq((0.0, 0.0, w1), (0.0, 1.0, w2), (0.0, 0.0, w1), (1.0, 0.0, w2),
        (1.0, 1.0, w1), (1.0, 1.0, w2), (1.0, 1.0, w1), (2.0, 2.0, w2),
        (2.0, 0.0, w1)), 2)
    val metrics = new MulticlassMetrics(predictionAndLabelsWithWeights)
    val tpRate0 = (2.0 * w1) / (2.0 * w1 + 1.0 * w2 + 1.0 * w1)
    val tpRate1 = (2.0 * w1 + 1.0 * w2) / (2.0 * w1 + 1.0 * w2 + 1.0 * w2)
    val tpRate2 = (1.0 * w2) / (1.0 * w2 + 0)
    val fpRate0 = (1.0 * w2) / (tw - (2.0 * w1 + 1.0 * w2 + 1.0 * w1))
    val fpRate1 = (1.0 * w2) / (tw - (1.0 * w2 + 2.0 * w1 + 1.0 * w2))
    val fpRate2 = (1.0 * w1) / (tw - (1.0 * w2))
    val precision0 = (2.0 * w1) / (2 * w1 + 1 * w2)
    val precision1 = (2.0 * w1 + 1.0 * w2) / (2.0 * w1 + 1.0 * w2 + 1.0 * w2)
    val precision2 = (1.0 * w2) / (1 * w1 + 1 * w2)
    val recall0 = (2.0 * w1) / (2.0 * w1 + 1.0 * w2 + 1.0 * w1)
    val recall1 = (2.0 * w1 + 1.0 * w2) / (2.0 * w1 + 1.0 * w2 + 1.0 * w2)
    val recall2 = (1.0 * w2) / (1.0 * w2 + 0)
    val f1measure0 = 2 * precision0 * recall0 / (precision0 + recall0)
    val f1measure1 = 2 * precision1 * recall1 / (precision1 + recall1)
    val f1measure2 = 2 * precision2 * recall2 / (precision2 + recall2)
    val f2measure0 = (1 + 2 * 2) * precision0 * recall0 / (2 * 2 * precision0 + recall0)
    val f2measure1 = (1 + 2 * 2) * precision1 * recall1 / (2 * 2 * precision1 + recall1)
    val f2measure2 = (1 + 2 * 2) * precision2 * recall2 / (2 * 2 * precision2 + recall2)

    assert(metrics.confusionMatrix.asML ~== confusionMatrix relTol delta)
    assert(metrics.truePositiveRate(0.0) ~== tpRate0 relTol delta)
    assert(metrics.truePositiveRate(1.0) ~== tpRate1 relTol delta)
    assert(metrics.truePositiveRate(2.0)  ~==  tpRate2 relTol delta)
    assert(metrics.falsePositiveRate(0.0)  ~==  fpRate0 relTol delta)
    assert(metrics.falsePositiveRate(1.0)  ~==  fpRate1 relTol delta)
    assert(metrics.falsePositiveRate(2.0)  ~==  fpRate2 relTol delta)
    assert(metrics.precision(0.0)  ~==  precision0 relTol delta)
    assert(metrics.precision(1.0)  ~==  precision1 relTol delta)
    assert(metrics.precision(2.0)  ~==  precision2 relTol delta)
    assert(metrics.recall(0.0)  ~==  recall0 relTol delta)
    assert(metrics.recall(1.0)  ~==  recall1 relTol delta)
    assert(metrics.recall(2.0)  ~==  recall2 relTol delta)
    assert(metrics.fMeasure(0.0)  ~==  f1measure0 relTol delta)
    assert(metrics.fMeasure(1.0)  ~==  f1measure1 relTol delta)
    assert(metrics.fMeasure(2.0)  ~==  f1measure2 relTol delta)
    assert(metrics.fMeasure(0.0, 2.0)  ~==  f2measure0 relTol delta)
    assert(metrics.fMeasure(1.0, 2.0)  ~==  f2measure1 relTol delta)
    assert(metrics.fMeasure(2.0, 2.0)  ~==  f2measure2 relTol delta)

    assert(metrics.accuracy  ~==
      (2.0 * w1 + 2.0 * w1 + 1.0 * w2 + 1.0 * w2) / tw relTol delta)
    assert(metrics.accuracy  ~==  metrics.weightedRecall relTol delta)
    val weight0 = (2 * w1 + 1 * w2 + 1 * w1) / tw
    val weight1 = (1 * w2 + 2 * w1 + 1 * w2) / tw
    val weight2 = 1 * w2 / tw
    assert(metrics.weightedTruePositiveRate  ~==
      (weight0 * tpRate0 + weight1 * tpRate1 + weight2 * tpRate2) relTol delta)
    assert(metrics.weightedFalsePositiveRate  ~==
      (weight0 * fpRate0 + weight1 * fpRate1 + weight2 * fpRate2) relTol delta)
    assert(metrics.weightedPrecision  ~==
      (weight0 * precision0 + weight1 * precision1 + weight2 * precision2) relTol delta)
    assert(metrics.weightedRecall  ~==
      (weight0 * recall0 + weight1 * recall1 + weight2 * recall2) relTol delta)
    assert(metrics.weightedFMeasure  ~==
      (weight0 * f1measure0 + weight1 * f1measure1 + weight2 * f1measure2) relTol delta)
    assert(metrics.weightedFMeasure(2.0)  ~==
      (weight0 * f2measure0 + weight1 * f2measure1 + weight2 * f2measure2) relTol delta)
    assert(metrics.labels === labels)
  }

  test("MulticlassMetrics supports binary class log-loss") {
    /*
     Using the following Python code to verify the correctness.

     from sklearn.metrics import log_loss
     labels = [1, 0, 0, 1]
     probabilities = [[.1, .9], [.9, .1], [.8, .2], [.35, .65]]
     weights = [1.5, 2.0, 1.0, 0.5]

     >>> log_loss(y_true=labels, y_pred=probabilities, sample_weight=weights)
     0.16145936283256573
     >>> log_loss(y_true=labels, y_pred=probabilities)
     0.21616187468057912
    */

    val labels = Seq(1.0, 0.0, 0.0, 1.0)
    val probabilities = Seq(
      Array(0.1, 0.9),
      Array(0.9, 0.1),
      Array(0.8, 0.2),
      Array(0.35, 0.65))
    val weights = Seq(1.5, 2.0, 1.0, 0.5)

    val rdd = sc.parallelize(labels.zip(weights).zip(probabilities)).map {
      case ((label, weight), probability) =>
        val prediction = probability.indexOf(probability.max).toDouble
        (prediction, label, weight, probability)
    }
    val metrics = new MulticlassMetrics(rdd)
    assert(metrics.logLoss() ~== 0.16145936283256573 relTol delta)

    val rdd2 = rdd.map {
      case (prediction: Double, label: Double, weight: Double, probability: Array[Double]) =>
        (prediction, label, 1.0, probability)
    }
    val metrics2 = new MulticlassMetrics(rdd2)
    assert(metrics2.logLoss() ~== 0.21616187468057912 relTol delta)
  }

  test("MulticlassMetrics supports multi-class log-loss") {
    /*
     Using the following Python code to verify the correctness.

     from sklearn.metrics import log_loss
     labels = [1, 2, 0, 1]
     probabilities = [[.1, .8, .1], [.9, .05, .05], [.8, .2, .0], [.3, .65, .05]]
     weights = [1.5, 2.0, 1.0, 0.5]

     >>> log_loss(y_true=labels, y_pred=probabilities, sample_weight=weights)
     1.3529429766879466
     >>> log_loss(y_true=labels, y_pred=probabilities)
     0.9682005730687164
    */

    val labels = Seq(1.0, 2.0, 0.0, 1.0)
    val probabilities = Seq(
      Array(0.1, 0.8, 0.1),
      Array(0.9, 0.05, 0.05),
      Array(0.8, 0.2, 0.0),
      Array(0.3, 0.65, 0.05))
    val weights = Seq(1.5, 2.0, 1.0, 0.5)

    val rdd = sc.parallelize(labels.zip(weights).zip(probabilities)).map {
      case ((label, weight), probability) =>
        val prediction = probability.indexOf(probability.max).toDouble
        (prediction, label, weight, probability)
    }
    val metrics = new MulticlassMetrics(rdd)
    assert(metrics.logLoss() ~== 1.3529429766879466 relTol delta)

    val rdd2 = rdd.map {
      case (prediction: Double, label: Double, weight: Double, probability: Array[Double]) =>
        (prediction, label, 1.0, probability)
    }
    val metrics2 = new MulticlassMetrics(rdd2)
    assert(metrics2.logLoss() ~== 0.9682005730687164 relTol delta)
  }

  test("MulticlassMetrics supports hammingLoss") {
    /*
     Using the following Python code to verify the correctness.

     from sklearn.metrics import hamming_loss
     y_true = [2, 2, 3, 4]
     y_pred = [1, 2, 3, 4]
     weights = [1.5, 2.0, 1.0, 0.5]

     >>> hamming_loss(y_true, y_pred)
     0.25
     >>> hamming_loss(y_true, y_pred, sample_weight=weights)
     0.3
    */

    val preds = Seq(1.0, 2.0, 3.0, 4.0)
    val labels = Seq(2.0, 2.0, 3.0, 4.0)
    val weights = Seq(1.5, 2.0, 1.0, 0.5)

    val rdd = sc.parallelize(preds.zip(labels))
    val metrics = new MulticlassMetrics(rdd)
    assert(metrics.hammingLoss ~== 0.25 relTol delta)

    val rdd2 = sc.parallelize(preds.zip(labels).zip(weights))
      .map { case ((pred, label), weight) =>
        (pred, label, weight)
      }
    val metrics2 = new MulticlassMetrics(rdd2)
    assert(metrics2.hammingLoss ~== 0.3 relTol delta)
  }
}
