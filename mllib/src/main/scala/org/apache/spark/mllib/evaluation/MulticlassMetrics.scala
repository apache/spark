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

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Evaluator for multiclass classification.
 *
 * @param predictionAndLabels an RDD of (prediction, label, weight) or
 *                         (prediction, label) tuples.
 */
@Since("1.1.0")
class MulticlassMetrics @Since("1.1.0") (predictionAndLabels: RDD[_ <: Product]) {
  val predLabelsWeight: RDD[(Double, Double, Double)] = predictionAndLabels.map {
    case (prediction: Double, label: Double, weight: Double) =>
      (prediction, label, weight)
    case (prediction: Double, label: Double) =>
      (prediction, label, 1.0)
    case other =>
      throw new IllegalArgumentException(s"Expected tuples, got $other")
  }

  /**
   * An auxiliary constructor taking a DataFrame.
   * @param predictionAndLabels a DataFrame with two double columns: prediction and label
   */
  private[mllib] def this(predictionAndLabels: DataFrame) =
    this(predictionAndLabels.rdd.map {
      case Row(prediction: Double, label: Double, weight: Double) =>
        (prediction, label, weight)
      case Row(prediction: Double, label: Double) =>
        (prediction, label, 1.0)
      case other =>
        throw new IllegalArgumentException(s"Expected Row of tuples, got $other")
    })

  private lazy val labelCountByClass: Map[Double, Double] =
    predLabelsWeight.map {
      case (_: Double, label: Double, weight: Double) =>
        (label, weight)
    }.reduceByKey(_ + _)
    .collectAsMap()
  private lazy val labelCount: Double = labelCountByClass.values.sum
  private lazy val tpByClass: Map[Double, Double] = predLabelsWeight
    .map {
      case (prediction: Double, label: Double, weight: Double) =>
        (label, if (label == prediction) weight else 0.0)
    }.reduceByKey(_ + _)
    .collectAsMap()
  private lazy val fpByClass: Map[Double, Double] = predLabelsWeight
    .map {
      case (prediction: Double, label: Double, weight: Double) =>
        (prediction, if (prediction != label) weight else 0.0)
    }.reduceByKey(_ + _)
    .collectAsMap()
  private lazy val confusions = predLabelsWeight
    .map {
      case (prediction: Double, label: Double, weight: Double) =>
        ((label, prediction), weight)
    }.reduceByKey(_ + _)
    .collectAsMap()

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
    if (tp + fp == 0) 0 else tp.toDouble / (tp + fp)
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
    falsePositiveRate(category) * count.toDouble / labelCount
  }.sum

  /**
   * Returns weighted averaged recall
   * (equals to precision, recall and f-measure)
   */
  @Since("1.1.0")
  lazy val weightedRecall: Double = labelCountByClass.map { case (category, count) =>
    recall(category) * count.toDouble / labelCount
  }.sum

  /**
   * Returns weighted averaged precision
   */
  @Since("1.1.0")
  lazy val weightedPrecision: Double = labelCountByClass.map { case (category, count) =>
    precision(category) * count.toDouble / labelCount
  }.sum

  /**
   * Returns weighted averaged f-measure
   * @param beta the beta parameter.
   */
  @Since("1.1.0")
  def weightedFMeasure(beta: Double): Double = labelCountByClass.map { case (category, count) =>
    fMeasure(category, beta) * count.toDouble / labelCount
  }.sum

  /**
   * Returns weighted averaged f1-measure
   */
  @Since("1.1.0")
  lazy val weightedFMeasure: Double = labelCountByClass.map { case (category, count) =>
    fMeasure(category, 1.0) * count.toDouble / labelCount
  }.sum

  /**
   * Returns the sequence of labels in ascending order
   */
  @Since("1.1.0")
  lazy val labels: Array[Double] = tpByClass.keys.toArray.sorted
}
