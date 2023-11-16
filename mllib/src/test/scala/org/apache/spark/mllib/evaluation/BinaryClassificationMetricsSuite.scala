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
import org.apache.spark.util.ArrayImplicits._

class BinaryClassificationMetricsSuite extends SparkFunSuite with MLlibTestSparkContext {

  private def assertSequencesMatch(actual: Seq[Double], expected: Seq[Double]): Unit = {
    actual.zip(expected).foreach { case (a, e) => assert(a ~== e absTol 1.0e-5) }
  }

  private def assertTupleSequencesMatch(actual: Seq[(Double, Double)],
       expected: Seq[(Double, Double)]): Unit = {
    actual.zip(expected).foreach { case ((ax, ay), (ex, ey)) =>
      assert(ax ~== ex absTol 1.0e-5)
      assert(ay ~== ey absTol 1.0e-5)
    }
  }

  private def validateMetrics(metrics: BinaryClassificationMetrics,
      expectedThresholds: Seq[Double],
      expectedROCCurve: Seq[(Double, Double)],
      expectedPRCurve: Seq[(Double, Double)],
      expectedFMeasures1: Seq[Double],
      expectedFmeasures2: Seq[Double],
      expectedPrecisions: Seq[Double],
      expectedRecalls: Seq[Double]): Unit = {

    assertSequencesMatch(metrics.thresholds().collect().toImmutableArraySeq, expectedThresholds)
    assertTupleSequencesMatch(metrics.roc().collect().toImmutableArraySeq, expectedROCCurve)
    assert(metrics.areaUnderROC() ~== AreaUnderCurve.of(expectedROCCurve) absTol 1E-5)
    assertTupleSequencesMatch(metrics.pr().collect().toImmutableArraySeq, expectedPRCurve)
    assert(metrics.areaUnderPR() ~== AreaUnderCurve.of(expectedPRCurve) absTol 1E-5)
    assertTupleSequencesMatch(metrics.fMeasureByThreshold().collect().toImmutableArraySeq,
      expectedThresholds.zip(expectedFMeasures1))
    assertTupleSequencesMatch(metrics.fMeasureByThreshold(2.0).collect().toImmutableArraySeq,
      expectedThresholds.zip(expectedFmeasures2))
    assertTupleSequencesMatch(metrics.precisionByThreshold().collect().toImmutableArraySeq,
      expectedThresholds.zip(expectedPrecisions))
    assertTupleSequencesMatch(metrics.recallByThreshold().collect().toImmutableArraySeq,
      expectedThresholds.zip(expectedRecalls))
  }

  test("binary evaluation metrics") {
    val scoreAndLabels = sc.parallelize(
      Seq((0.1, 0.0), (0.1, 1.0), (0.4, 0.0), (0.6, 0.0), (0.6, 1.0), (0.6, 1.0), (0.8, 1.0)), 2)
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val thresholds = Seq(0.8, 0.6, 0.4, 0.1)
    val numTruePositives = Seq(1, 3, 3, 4)
    val numFalsePositives = Seq(0, 1, 2, 3)
    val numPositives = 4
    val numNegatives = 3
    val precisions = numTruePositives.zip(numFalsePositives).map { case (t, f) =>
      t.toDouble / (t + f)
    }
    val recalls = numTruePositives.map(t => t.toDouble / numPositives)
    val fpr = numFalsePositives.map(f => f.toDouble / numNegatives)
    val rocCurve = Seq((0.0, 0.0)) ++ fpr.zip(recalls) ++ Seq((1.0, 1.0))
    val pr = recalls.zip(precisions)
    val prCurve = Seq((0.0, 1.0)) ++ pr
    val f1 = pr.map { case (r, p) => 2.0 * (p * r) / (p + r)}
    val f2 = pr.map { case (r, p) => 5.0 * (p * r) / (4.0 * p + r)}

    validateMetrics(metrics, thresholds, rocCurve, prCurve, f1, f2, precisions, recalls)
  }

  test("binary evaluation metrics with weights") {
    val w1 = 1.5
    val w2 = 0.7
    val w3 = 0.4
    val scoreAndLabelsWithWeights = sc.parallelize(
      Seq((0.1, 0.0, w1), (0.1, 1.0, w2), (0.4, 0.0, w1), (0.6, 0.0, w3),
        (0.6, 1.0, w2), (0.6, 1.0, w2), (0.8, 1.0, w1)), 2)
    val metrics = new BinaryClassificationMetrics(scoreAndLabelsWithWeights, 0)
    val thresholds = Seq(0.8, 0.6, 0.4, 0.1)
    val numTruePositives =
      Seq(1 * w1, 1 * w1 + 2 * w2, 1 * w1 + 2 * w2, 3 * w2 + 1 * w1)
    val numFalsePositives = Seq(0.0, 1.0 * w3, 1.0 * w1 + 1.0 * w3, 1.0 * w3 + 2.0 * w1)
    val numPositives = 3 * w2 + 1 * w1
    val numNegatives = 2 * w1 + w3
    val precisions = numTruePositives.zip(numFalsePositives).map { case (t, f) =>
      t.toDouble / (t + f)
    }
    val recalls = numTruePositives.map(_ / numPositives)
    val fpr = numFalsePositives.map(_ / numNegatives)
    val rocCurve = Seq((0.0, 0.0)) ++ fpr.zip(recalls) ++ Seq((1.0, 1.0))
    val pr = recalls.zip(precisions)
    val prCurve = Seq((0.0, 1.0)) ++ pr
    val f1 = pr.map { case (r, p) => 2.0 * (p * r) / (p + r)}
    val f2 = pr.map { case (r, p) => 5.0 * (p * r) / (4.0 * p + r)}

    validateMetrics(metrics, thresholds, rocCurve, prCurve, f1, f2, precisions, recalls)
  }

  test("binary evaluation metrics for RDD where all examples have positive label") {
    val scoreAndLabels = sc.parallelize(Seq((0.5, 1.0), (0.5, 1.0)), 2)
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)

    val thresholds = Seq(0.5)
    val precisions = Seq(1.0)
    val recalls = Seq(1.0)
    val fpr = Seq(0.0)
    val rocCurve = Seq((0.0, 0.0)) ++ fpr.zip(recalls) ++ Seq((1.0, 1.0))
    val pr = recalls.zip(precisions)
    val prCurve = Seq((0.0, 1.0)) ++ pr
    val f1 = pr.map { case (r, p) => 2.0 * (p * r) / (p + r)}
    val f2 = pr.map { case (r, p) => 5.0 * (p * r) / (4.0 * p + r)}

    validateMetrics(metrics, thresholds, rocCurve, prCurve, f1, f2, precisions, recalls)
  }

  test("binary evaluation metrics for RDD where all examples have negative label") {
    val scoreAndLabels = sc.parallelize(Seq((0.5, 0.0), (0.5, 0.0)), 2)
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)

    val thresholds = Seq(0.5)
    val precisions = Seq(0.0)
    val recalls = Seq(0.0)
    val fpr = Seq(1.0)
    val rocCurve = Seq((0.0, 0.0)) ++ fpr.zip(recalls) ++ Seq((1.0, 1.0))
    val pr = recalls.zip(precisions)
    val prCurve = Seq((0.0, 0.0)) ++ pr
    val f1 = pr.map {
      case (0, 0) => 0.0
      case (r, p) => 2.0 * (p * r) / (p + r)
    }
    val f2 = pr.map {
      case (0, 0) => 0.0
      case (r, p) => 5.0 * (p * r) / (4.0 * p + r)
    }

    validateMetrics(metrics, thresholds, rocCurve, prCurve, f1, f2, precisions, recalls)
  }

  test("binary evaluation metrics with downsampling") {
    val scoreAndLabels = Seq(
      (0.1, 0.0), (0.2, 0.0), (0.3, 1.0), (0.4, 0.0), (0.5, 0.0),
      (0.6, 1.0), (0.7, 1.0), (0.8, 0.0), (0.9, 1.0))

    val scoreAndLabelsRDD = sc.parallelize(scoreAndLabels, 1)

    val original = new BinaryClassificationMetrics(scoreAndLabelsRDD)
    val originalROC = original.roc().collect().sorted.toList
    // Add 2 for (0,0) and (1,1) appended at either end
    assert(2 + scoreAndLabels.size == originalROC.size)
    assert(
      List(
        (0.0, 0.0), (0.0, 0.25), (0.2, 0.25), (0.2, 0.5), (0.2, 0.75),
        (0.4, 0.75), (0.6, 0.75), (0.6, 1.0), (0.8, 1.0), (1.0, 1.0),
        (1.0, 1.0)
      ) ==
      originalROC)

    val numBins = 4

    val downsampled = new BinaryClassificationMetrics(scoreAndLabelsRDD, numBins)
    val downsampledROC = downsampled.roc().collect().sorted.toList
    assert(
      // May have to add 1 if the sample factor didn't divide evenly
      2 + (numBins + (if (scoreAndLabels.size % numBins == 0) 0 else 1)) ==
      downsampledROC.size)
    assert(
      List(
        (0.0, 0.0), (0.2, 0.25), (0.2, 0.75), (0.6, 0.75), (0.8, 1.0),
        (1.0, 1.0), (1.0, 1.0)
      ) ==
      downsampledROC)

    val downsampledRecall = downsampled.recallByThreshold().collect().sorted.toList
    assert(
      // May have to add 1 if the sample factor didn't divide evenly
      numBins + (if (scoreAndLabels.size % numBins == 0) 0 else 1) ==
      downsampledRecall.size)
    assert(
      List(
        (0.1, 1.0), (0.2, 1.0), (0.4, 0.75), (0.6, 0.75), (0.8, 0.25)
      ) ==
      downsampledRecall)
  }

}
