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

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Binary confusion matrix.
 *
 * @param count label counter for labels with scores greater than or equal to the current score
 * @param total label counter for all labels
 */
case class BinaryConfusionMatrix(
    private val count: LabelCounter,
    private val total: LabelCounter) extends Serializable {

  /** number of true positives */
  def tp: Long = count.numPositives

  /** number of false positives */
  def fp: Long = count.numNegatives

  /** number of false negatives */
  def fn: Long = total.numPositives - count.numPositives

  /** number of true negatives */
  def tn: Long = total.numNegatives - count.numNegatives

  /** number of positives */
  def p: Long = total.numPositives

  /** number of negatives */
  def n: Long = total.numNegatives
}

private trait Metric {
  def apply(c: BinaryConfusionMatrix): Double
}

object Precision extends Metric {
  override def apply(c: BinaryConfusionMatrix): Double =
    c.tp.toDouble / (c.tp + c.fp)
}

object FalsePositiveRate extends Metric {
  override def apply(c: BinaryConfusionMatrix): Double =
    c.fp.toDouble / c.n
}

object Recall extends Metric {
  override def apply(c: BinaryConfusionMatrix): Double =
    c.tp.toDouble / c.p
}

case class FMeasure(beta: Double) extends Metric {
  private val beta2 = beta * beta
  override def apply(c: BinaryConfusionMatrix): Double = {
    val precision = Precision(c)
    val recall = Recall(c)
    (1.0 + beta2) * (precision * recall) / (beta2 * precision + recall)
  }
}

/**
 * Evaluator for binary classification.
 *
 * @param scoreAndlabels an RDD of (score, label) pairs.
 */
class BinaryClassificationEvaluator(scoreAndlabels: RDD[(Double, Double)]) extends Serializable {

  private lazy val (cumCounts: RDD[(Double, LabelCounter)], totalCount: LabelCounter, scoreAndConfusion: RDD[(Double, BinaryConfusionMatrix)]) = {
    // Create a bin for each distinct score value, count positives and negatives within each bin,
    // and then sort by score values in descending order.
    val counts = scoreAndlabels.combineByKey(
      createCombiner = (label: Double) => new LabelCounter(0L, 0L) += label,
      mergeValue = (c: LabelCounter, label: Double) => c += label,
      mergeCombiners = (c1: LabelCounter, c2: LabelCounter) => c1 += c2
    ).sortByKey(ascending = false)
    val agg = counts.values.mapPartitions({ iter =>
      val agg = new LabelCounter()
      iter.foreach(agg += _)
      Iterator(agg)
    }, preservesPartitioning = true).collect()
    val cum = agg.scanLeft(new LabelCounter())((agg: LabelCounter, c: LabelCounter) => agg + c)
    val totalCount = cum.last
    val cumCounts = counts.mapPartitionsWithIndex((index: Int, iter: Iterator[(Double, LabelCounter)]) => {
      val cumCount = cum(index)
      iter.map { case (score, c) =>
        cumCount += c
        (score, cumCount.clone())
      }
    }, preservesPartitioning = true)
    cumCounts.persist()
    val scoreAndConfusion = cumCounts.map { case (score, cumCount) =>
      (score, BinaryConfusionMatrix(cumCount, totalCount))
    }
    (cumCounts, totalCount, scoreAndConfusion)
  }

  def unpersist() {
    cumCounts.unpersist()
  }

  def rocCurve(): RDD[(Double, Double)] = createCurve(FalsePositiveRate, Recall)

  def rocAUC(): Double = AreaUnderCurve.of(rocCurve())

  def prCurve(): RDD[(Double, Double)] = createCurve(Recall, Precision)

  def prAUC(): Double = AreaUnderCurve.of(prCurve())

  def fMeasureByThreshold(beta: Double): RDD[(Double, Double)] = createCurve(FMeasure(beta))

  def fMeasureByThreshold() = fMeasureByThreshold(1.0)

  private def createCurve(y: Metric): RDD[(Double, Double)] = {
    scoreAndConfusion.map { case (s, c) =>
      (s, y(c))
    }
  }

  private def createCurve(x: Metric, y: Metric): RDD[(Double, Double)] = {
    scoreAndConfusion.map { case (_, c) =>
      (x(c), y(c))
    }
  }
}

class LocalBinaryClassificationEvaluator {
  def get(data: Iterable[(Double, Double)]) {
    val counts = data.groupBy(_._1).mapValues { s =>
      val c = new LabelCounter()
      s.foreach(c += _._2)
      c
    }.toSeq.sortBy(- _._1)
    println("counts: " + counts.toList)
    val total = new LabelCounter()
    val cum = counts.map { s =>
      total += s._2
      (s._1, total.clone())
    }
    println("cum: " + cum.toList)
    val roc = cum.map { case (s, c) =>
      (1.0 * c.numNegatives / total.numNegatives, 1.0 * c.numPositives / total.numPositives)
    }
    val rocAUC = AreaUnderCurve.of(roc)
    println(rocAUC)
    val pr = cum.map { case (s, c) =>
      (1.0 * c.numPositives / total.numPositives,
        1.0 * c.numPositives / (c.numPositives + c.numNegatives))
    }
    val prAUC = AreaUnderCurve.of(pr)
    println(prAUC)
  }
}

/**
 * A counter for positives and negatives.
 *
 * @param numPositives
 * @param numNegatives
 */
private[evaluation]
class LabelCounter(var numPositives: Long = 0L, var numNegatives: Long = 0L) extends Serializable {

  /** Process a label. */
  def +=(label: Double): LabelCounter = {
    // Though we assume 1.0 for positive and 0.0 for negative, the following check will handle
    // -1.0 for negative as well.
    if (label > 0.5) numPositives += 1L else numNegatives += 1L
    this
  }

  /** Merge another counter. */
  def +=(other: LabelCounter): LabelCounter = {
    numPositives += other.numPositives
    numNegatives += other.numNegatives
    this
  }

  def +(label: Double): LabelCounter = {
    this.clone() += label
  }

  def +(other: LabelCounter): LabelCounter = {
    this.clone() += other
  }

  def sum: Long = numPositives + numNegatives

  override def clone: LabelCounter = {
    new LabelCounter(numPositives, numNegatives)
  }

  override def toString: String = s"[$numPositives,$numNegatives]"
}

