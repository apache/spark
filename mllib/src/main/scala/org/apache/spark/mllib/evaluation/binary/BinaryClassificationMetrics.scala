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

package org.apache.spark.mllib.evaluation.binary

import org.apache.spark.rdd.{UnionRDD, RDD}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.evaluation.AreaUnderCurve
import org.apache.spark.Logging

/**
 * Implementation of [[org.apache.spark.mllib.evaluation.binary.BinaryConfusionMatrix]].
 *
 * @param count label counter for labels with scores greater than or equal to the current score
 * @param totalCount label counter for all labels
 */
private case class BinaryConfusionMatrixImpl(
    count: LabelCounter,
    totalCount: LabelCounter) extends BinaryConfusionMatrix with Serializable {

  /** number of true positives */
  override def numTruePositives: Long = count.numPositives

  /** number of false positives */
  override def numFalsePositives: Long = count.numNegatives

  /** number of false negatives */
  override def numFalseNegatives: Long = totalCount.numPositives - count.numPositives

  /** number of true negatives */
  override def numTrueNegatives: Long = totalCount.numNegatives - count.numNegatives

  /** number of positives */
  override def numPositives: Long = totalCount.numPositives

  /** number of negatives */
  override def numNegatives: Long = totalCount.numNegatives
}

/**
 * Evaluator for binary classification.
 *
 * @param scoreAndLabels an RDD of (score, label) pairs.
 */
class BinaryClassificationMetrics(scoreAndLabels: RDD[(Double, Double)])
  extends Serializable with Logging {

  private lazy val (
      cumulativeCounts: RDD[(Double, LabelCounter)],
      confusions: RDD[(Double, BinaryConfusionMatrix)]) = {
    // Create a bin for each distinct score value, count positives and negatives within each bin,
    // and then sort by score values in descending order.
    val counts = scoreAndLabels.combineByKey(
      createCombiner = (label: Double) => new LabelCounter(0L, 0L) += label,
      mergeValue = (c: LabelCounter, label: Double) => c += label,
      mergeCombiners = (c1: LabelCounter, c2: LabelCounter) => c1 += c2
    ).sortByKey(ascending = false)
    val agg = counts.values.mapPartitions({ iter =>
      val agg = new LabelCounter()
      iter.foreach(agg += _)
      Iterator(agg)
    }, preservesPartitioning = true).collect()
    val partitionwiseCumulativeCounts =
      agg.scanLeft(new LabelCounter())((agg: LabelCounter, c: LabelCounter) => agg.clone() += c)
    val totalCount = partitionwiseCumulativeCounts.last
    logInfo(s"Total counts: $totalCount")
    val cumulativeCounts = counts.mapPartitionsWithIndex(
      (index: Int, iter: Iterator[(Double, LabelCounter)]) => {
        val cumCount = partitionwiseCumulativeCounts(index)
        iter.map { case (score, c) =>
          cumCount += c
          (score, cumCount.clone())
        }
      }, preservesPartitioning = true)
    cumulativeCounts.persist()
    val confusions = cumulativeCounts.map { case (score, cumCount) =>
      (score, BinaryConfusionMatrixImpl(cumCount, totalCount).asInstanceOf[BinaryConfusionMatrix])
    }
    (cumulativeCounts, confusions)
  }

  /** Unpersist intermediate RDDs used in the computation. */
  def unpersist() {
    cumulativeCounts.unpersist()
  }

  /** Returns thresholds in descending order. */
  def thresholds(): RDD[Double] = cumulativeCounts.map(_._1)

  /**
   * Returns the receiver operating characteristic (ROC) curve,
   * which is an RDD of (false positive rate, true positive rate)
   * with (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
   * @see http://en.wikipedia.org/wiki/Receiver_operating_characteristic
   */
  def roc(): RDD[(Double, Double)] = {
    val rocCurve = createCurve(FalsePositiveRate, Recall)
    val sc = confusions.context
    val first = sc.makeRDD(Seq((0.0, 0.0)), 1)
    val last = sc.makeRDD(Seq((1.0, 1.0)), 1)
    new UnionRDD[(Double, Double)](sc, Seq(first, rocCurve, last))
  }

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   */
  def areaUnderROC(): Double = AreaUnderCurve.of(roc())

  /**
   * Returns the precision-recall curve, which is an RDD of (recall, precision),
   * NOT (precision, recall), with (0.0, 1.0) prepended to it.
   * @see http://en.wikipedia.org/wiki/Precision_and_recall
   */
  def pr(): RDD[(Double, Double)] = {
    val prCurve = createCurve(Recall, Precision)
    val sc = confusions.context
    val first = sc.makeRDD(Seq((0.0, 1.0)), 1)
    first.union(prCurve)
  }

  /**
   * Computes the area under the precision-recall curve.
   */
  def areaUnderPR(): Double = AreaUnderCurve.of(pr())

  /**
   * Returns the (threshold, F-Measure) curve.
   * @param beta the beta factor in F-Measure computation.
   * @return an RDD of (threshold, F-Measure) pairs.
   * @see http://en.wikipedia.org/wiki/F1_score
   */
  def fMeasureByThreshold(beta: Double): RDD[(Double, Double)] = createCurve(FMeasure(beta))

  /** Returns the (threshold, F-Measure) curve with beta = 1.0. */
  def fMeasureByThreshold(): RDD[(Double, Double)] = fMeasureByThreshold(1.0)

  /** Returns the (threshold, precision) curve. */
  def precisionByThreshold(): RDD[(Double, Double)] = createCurve(Precision)

  /** Returns the (threshold, recall) curve. */
  def recallByThreshold(): RDD[(Double, Double)] = createCurve(Recall)

  /** Creates a curve of (threshold, metric). */
  private def createCurve(y: BinaryClassificationMetricComputer): RDD[(Double, Double)] = {
    confusions.map { case (s, c) =>
      (s, y(c))
    }
  }

  /** Creates a curve of (metricX, metricY). */
  private def createCurve(
      x: BinaryClassificationMetricComputer,
      y: BinaryClassificationMetricComputer): RDD[(Double, Double)] = {
    confusions.map { case (_, c) =>
      (x(c), y(c))
    }
  }
}

/**
 * A counter for positives and negatives.
 *
 * @param numPositives number of positive labels
 * @param numNegatives number of negative labels
 */
private class LabelCounter(
    var numPositives: Long = 0L,
    var numNegatives: Long = 0L) extends Serializable {

  /** Processes a label. */
  def +=(label: Double): LabelCounter = {
    // Though we assume 1.0 for positive and 0.0 for negative, the following check will handle
    // -1.0 for negative as well.
    if (label > 0.5) numPositives += 1L else numNegatives += 1L
    this
  }

  /** Merges another counter. */
  def +=(other: LabelCounter): LabelCounter = {
    numPositives += other.numPositives
    numNegatives += other.numNegatives
    this
  }

  override def clone: LabelCounter = {
    new LabelCounter(numPositives, numNegatives)
  }

  override def toString: String = s"{numPos: $numPositives, numNeg: $numNegatives}"
}
