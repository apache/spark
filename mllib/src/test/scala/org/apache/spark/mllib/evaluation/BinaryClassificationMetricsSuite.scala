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

class BinaryClassificationMetricsSuite extends SparkFunSuite with MLlibTestSparkContext {

  private def areWithinEpsilon(x: (Double, Double)): Boolean = x._1 ~= (x._2) absTol 1E-5

  private def pairsWithinEpsilon(x: ((Double, Double), (Double, Double))): Boolean =
    (x._1._1 ~= x._2._1 absTol 1E-5) && (x._1._2 ~= x._2._2 absTol 1E-5)

  private def assertSequencesMatch(left: Seq[Double], right: Seq[Double]): Unit = {
      assert(left.zip(right).forall(areWithinEpsilon))
  }

  private def assertTupleSequencesMatch(left: Seq[(Double, Double)],
       right: Seq[(Double, Double)]): Unit = {
    assert(left.zip(right).forall(pairsWithinEpsilon))
  }

  private def validateMetrics(metrics: BinaryClassificationMetrics,
      expectedThresholds: Seq[Double],
      expectedROCCurve: Seq[(Double, Double)],
      expectedPRCurve: Seq[(Double, Double)],
      expectedFMeasures1: Seq[Double],
      expectedFmeasures2: Seq[Double],
      expectedPrecisions: Seq[Double],
      expectedRecalls: Seq[Double]) = {

    assertSequencesMatch(metrics.thresholds().collect(), expectedThresholds)
    assertTupleSequencesMatch(metrics.roc().collect(), expectedROCCurve)
    assert(metrics.areaUnderROC() ~== AreaUnderCurve.of(expectedROCCurve) absTol 1E-5)
    assertTupleSequencesMatch(metrics.pr().collect(), expectedPRCurve)
    assert(metrics.areaUnderPR() ~== AreaUnderCurve.of(expectedPRCurve) absTol 1E-5)
    assertTupleSequencesMatch(metrics.fMeasureByThreshold().collect(),
      expectedThresholds.zip(expectedFMeasures1))
    assertTupleSequencesMatch(metrics.fMeasureByThreshold(2.0).collect(),
      expectedThresholds.zip(expectedFmeasures2))
    assertTupleSequencesMatch(metrics.precisionByThreshold().collect(),
      expectedThresholds.zip(expectedPrecisions))
    assertTupleSequencesMatch(metrics.recallByThreshold().collect(),
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
    val prCurve = Seq((0.0, 1.0)) ++ pr
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
  }

}
