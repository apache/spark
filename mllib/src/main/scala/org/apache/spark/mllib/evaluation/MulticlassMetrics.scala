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

import scala.collection.Map
import scala.collection.mutable

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * Evaluator for multiclass classification.
 *
 * @param predictionAndLabels an RDD of (prediction, label, weight, probability) or
 *                            (prediction, label, weight) or (prediction, label) tuples.
 */
@Since("1.1.0")
class MulticlassMetrics @Since("1.1.0") (predictionAndLabels: RDD[_ <: Product]) {

  /**
   * An auxiliary constructor taking a DataFrame.
   * @param predictionAndLabels a DataFrame with columns: prediction, label, weight (optional)
   *                            and probability (only for logLoss)
   */
  private[mllib] def this(predictionAndLabels: DataFrame) =
    this(predictionAndLabels.rdd.map { r =>
      r.size match {
        case 2 => (r.getDouble(0), r.getDouble(1), 1.0, null)
        case 3 => (r.getDouble(0), r.getDouble(1), r.getDouble(2), null)
        case 4 => (r.getDouble(0), r.getDouble(1), r.getDouble(2), r.getSeq[Double](3).toArray)
        case _ => throw new IllegalArgumentException(s"Expected Row of tuples, got $r")
      }
    })

  private lazy val confusions = predictionAndLabels.map {
    case (prediction: Double, label: Double, weight: Double, _) =>
      ((label, prediction), weight)
    case (prediction: Double, label: Double, weight: Double) =>
      ((label, prediction), weight)
    case (prediction: Double, label: Double) =>
      ((label, prediction), 1.0)
    case other =>
      throw new IllegalArgumentException(s"Expected tuples, got $other")
  }.reduceByKey(_ + _)
    .collectAsMap()

  private lazy val labelCountByClass: Map[Double, Double] = {
    val labelCountByClass = mutable.Map.empty[Double, Double]
    confusions.iterator.foreach {
      case ((label, _), weight) =>
        val w = labelCountByClass.getOrElse(label, 0.0)
        labelCountByClass.update(label, w + weight)
    }
    labelCountByClass.toMap
  }

  private lazy val labelCount: Double = labelCountByClass.values.sum

  private lazy val tpByClass: Map[Double, Double] = {
    val tpByClass = mutable.Map.empty[Double, Double]
    confusions.iterator.foreach {
      case ((label, prediction), weight) =>
        val w = tpByClass.getOrElse(label, 0.0)
        if (label == prediction) {
          tpByClass.update(label, w + weight)
        } else if (w == 0.0) {
          tpByClass.update(label, w)
        }
    }
    tpByClass.toMap
  }

  private lazy val fpByClass: Map[Double, Double] = {
    val fpByClass = mutable.Map.empty[Double, Double]
    confusions.iterator.foreach {
      case ((label, prediction), weight) =>
        val w = fpByClass.getOrElse(prediction, 0.0)
        if (label != prediction) {
          fpByClass.update(prediction, w + weight)
        } else if (w == 0.0) {
          fpByClass.update(prediction, w)
        }
    }
    fpByClass.toMap
  }


  /**
   * Returns confusion matrix:
   * predicted classes are in columns,
   * they are ordered by class label ascending,
   * as in "labels"
   */
  @Since("1.1.0")
  def confusionMatrix: Matrix = {
    val n = labels.length
    val values = Array.ofDim[Double](n * n)
    var i = 0
    while (i < n) {
      var j = 0
      while (j < n) {
        values(i + j * n) = confusions.getOrElse((labels(i), labels(j)), 0.0)
        j += 1
      }
      i += 1
    }
    Matrices.dense(n, n, values)
  }

  /**
   * Returns true positive rate for a given label (category)
   * @param label the label.
   */
  @Since("1.1.0")
  def truePositiveRate(label: Double): Double = recall(label)

  /**
   * Returns false positive rate for a given label (category)
   * @param label the label.
   */
  @Since("1.1.0")
  def falsePositiveRate(label: Double): Double = {
    val fp = fpByClass.getOrElse(label, 0.0)
    fp / (labelCount - labelCountByClass(label))
  }

  /**
   * Returns precision for a given label (category)
   * @param label the label.
   */
  @Since("1.1.0")
  def precision(label: Double): Double = {
    val tp = tpByClass(label)
    val fp = fpByClass.getOrElse(label, 0.0)
    if (tp + fp == 0) 0 else tp / (tp + fp)
  }

  /**
   * Returns recall for a given label (category)
   * @param label the label.
   */
  @Since("1.1.0")
  def recall(label: Double): Double = tpByClass(label) / labelCountByClass(label)

  /**
   * Returns f-measure for a given label (category)
   * @param label the label.
   * @param beta the beta parameter.
   */
  @Since("1.1.0")
  def fMeasure(label: Double, beta: Double): Double = {
    val p = precision(label)
    val r = recall(label)
    val betaSqrd = beta * beta
    if (p + r == 0) 0 else (1 + betaSqrd) * p * r / (betaSqrd * p + r)
  }

  /**
   * Returns f1-measure for a given label (category)
   * @param label the label.
   */
  @Since("1.1.0")
  def fMeasure(label: Double): Double = fMeasure(label, 1.0)

  /**
   * Returns accuracy
   * (equals to the total number of correctly classified instances
   * out of the total number of instances.)
   */
  @Since("2.0.0")
  lazy val accuracy: Double = tpByClass.values.sum / labelCount

  /**
   * Returns weighted true positive rate
   * (equals to precision, recall and f-measure)
   */
  @Since("1.1.0")
  lazy val weightedTruePositiveRate: Double = weightedRecall

  /**
   * Returns weighted false positive rate
   */
  @Since("1.1.0")
  lazy val weightedFalsePositiveRate: Double = labelCountByClass.map { case (category, count) =>
    falsePositiveRate(category) * count / labelCount
  }.sum

  /**
   * Returns weighted averaged recall
   * (equals to precision, recall and f-measure)
   */
  @Since("1.1.0")
  lazy val weightedRecall: Double = labelCountByClass.map { case (category, count) =>
    recall(category) * count / labelCount
  }.sum

  /**
   * Returns weighted averaged precision
   */
  @Since("1.1.0")
  lazy val weightedPrecision: Double = labelCountByClass.map { case (category, count) =>
    precision(category) * count / labelCount
  }.sum

  /**
   * Returns weighted averaged f-measure
   * @param beta the beta parameter.
   */
  @Since("1.1.0")
  def weightedFMeasure(beta: Double): Double = labelCountByClass.map { case (category, count) =>
    fMeasure(category, beta) * count / labelCount
  }.sum

  /**
   * Returns weighted averaged f1-measure
   */
  @Since("1.1.0")
  lazy val weightedFMeasure: Double = weightedFMeasure(1.0)

  /**
   * Returns the sequence of labels in ascending order
   */
  @Since("1.1.0")
  lazy val labels: Array[Double] = tpByClass.keys.toArray.sorted

  /**
   * Returns Hamming-loss
   */
  @Since("3.0.0")
  lazy val hammingLoss: Double = {
    var numerator = 0.0
    var denominator = 0.0
    confusions.iterator.foreach {
      case ((label, prediction), weight) =>
        if (label != prediction) {
          numerator += weight
        }
        denominator += weight
    }
    numerator / denominator
  }

  /**
   * Returns the log-loss, aka logistic loss or cross-entropy loss.
   * @param eps log-loss is undefined for p=0 or p=1, so probabilities are
   *            clipped to max(eps, min(1 - eps, p)).
   */
  @Since("3.0.0")
  def logLoss(eps: Double = 1e-15): Double = {
    require(eps > 0 && eps < 0.5, s"eps must be in range (0, 0.5), but got $eps")
    val loss1 = - math.log(eps)
    val loss2 = - math.log1p(-eps)

    val (lossSum, weightSum) = predictionAndLabels.map {
      case (_, label: Double, weight: Double, probability: Array[Double]) =>
        require(label.toInt == label && label >= 0, s"Invalid label $label")
        require(probability != null, "probability of each class can not be null")
        val p = probability(label.toInt)
        val loss = if (p < eps) {
          loss1
        } else if (p > 1 - eps) {
          loss2
        } else {
          - math.log(p)
        }
        (loss * weight, weight)

      case other =>
        throw new IllegalArgumentException(s"Invalid RDD value for MulticlassMetrics.logLoss. " +
          s"Expected quadruples, got $other")
    }.treeReduce { case ((l1, w1), (l2, w2)) =>
      (l1 + l2, w1 + w2)
    }

    lossSum / weightSum
  }
}
