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

import org.apache.spark.rdd.RDD
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
  override def tp: Long = count.numPositives

  /** number of false positives */
  override def fp: Long = count.numNegatives

  /** number of false negatives */
  override def fn: Long = totalCount.numPositives - count.numPositives

  /** number of true negatives */
  override def tn: Long = totalCount.numNegatives - count.numNegatives

  /** number of positives */
  override def p: Long = totalCount.numPositives

  /** number of negatives */
  override def n: Long = totalCount.numNegatives
}

/**
 * Evaluator for binary classification.
 *
 * @param scoreAndLabels an RDD of (score, label) pairs.
 */
class BinaryClassificationEvaluator(scoreAndLabels: RDD[(Double, Double)])
    extends Serializable with Logging {

  private lazy val (
      cumCounts: RDD[(Double, LabelCounter)],
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
    val partitionwiseCumCounts =
      agg.scanLeft(new LabelCounter())((agg: LabelCounter, c: LabelCounter) => agg.clone() += c)
    val totalCount = partitionwiseCumCounts.last
    logInfo(s"Total counts: $totalCount")
    val cumCounts = counts.mapPartitionsWithIndex(
      (index: Int, iter: Iterator[(Double, LabelCounter)]) => {
        val cumCount = partitionwiseCumCounts(index)
        iter.map { case (score, c) =>
          cumCount += c
          (score, cumCount.clone())
        }
      }, preservesPartitioning = true)
    cumCounts.persist()
    val confusions = cumCounts.map { case (score, cumCount) =>
      (score, BinaryConfusionMatrixImpl(cumCount, totalCount).asInstanceOf[BinaryConfusionMatrix])
    }
    (cumCounts, confusions)
  }

  /** Unpersist intermediate RDDs used in the computation. */
  def unpersist() {
    cumCounts.unpersist()
  }

  /**
   * Returns the receiver operating characteristic (ROC) curve.
   * @see http://en.wikipedia.org/wiki/Receiver_operating_characteristic
   */
  def rocCurve(): RDD[(Double, Double)] = createCurve(FalsePositiveRate, Recall)

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   */
  def rocAUC(): Double = AreaUnderCurve.of(rocCurve())

  /**
   * Returns the precision-recall curve.
   * @see http://en.wikipedia.org/wiki/Precision_and_recall
   */
  def prCurve(): RDD[(Double, Double)] = createCurve(Recall, Precision)

  /**
   * Computes the area under the precision-recall curve.
   */
  def prAUC(): Double = AreaUnderCurve.of(prCurve())

  /**
   * Returns the (threshold, F-Measure) curve.
   * @param beta the beta factor in F-Measure computation.
   * @return an RDD of (threshold, F-Measure) pairs.
   * @see http://en.wikipedia.org/wiki/F1_score
   */
  def fMeasureByThreshold(beta: Double): RDD[(Double, Double)] = createCurve(FMeasure(beta))

  /** Returns the (threshold, F-Measure) curve with beta = 1.0. */
  def fMeasureByThreshold(): RDD[(Double, Double)] = fMeasureByThreshold(1.0)

  /** Creates a curve of (threshold, metric). */
  private def createCurve(y: BinaryClassificationMetric): RDD[(Double, Double)] = {
    confusions.map { case (s, c) =>
      (s, y(c))
    }
  }

  /** Creates a curve of (metricX, metricY). */
  private def createCurve(
      x: BinaryClassificationMetric,
      y: BinaryClassificationMetric): RDD[(Double, Double)] = {
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
