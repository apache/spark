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
import org.apache.spark.sql.DataFrame

/**
 * Evaluator for multiclass classification.
 *
 * @param predictionAndLabels an RDD of (prediction, label) pairs.
 */
@Since("1.1.0")
class MulticlassMetrics @Since("1.1.0") (predictionAndLabels: RDD[(Double, Double)]) {

  /**
   * An auxiliary constructor taking a DataFrame.
   * @param predictionAndLabels a DataFrame with two double columns: prediction and label
   */
  private[mllib] def this(predictionAndLabels: DataFrame) =
    this(predictionAndLabels.rdd.map(r => (r.getDouble(0), r.getDouble(1))))

  private lazy val labelCountByClass: Map[Double, Long] = predictionAndLabels.values.countByValue()
  private lazy val labelCount: Long = labelCountByClass.values.sum
  private lazy val tpByClass: Map[Double, Int] = predictionAndLabels
    .map { case (prediction, label) =>
      (label, if (label == prediction) 1 else 0)
    }.reduceByKey(_ + _)
    .collectAsMap()
  private lazy val fpByClass: Map[Double, Int] = predictionAndLabels
    .map { case (prediction, label) =>
      (prediction, if (prediction != label) 1 else 0)
    }.reduceByKey(_ + _)
    .collectAsMap()
  private lazy val confusions = predictionAndLabels
    .map { case (prediction, label) =>
      ((label, prediction), 1)
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
        values(i + j * n) = confusions.getOrElse((labels(i), labels(j)), 0).toDouble
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
    val fp = fpByClass.getOrElse(label, 0)
    fp.toDouble / (labelCount - labelCountByClass(label))
  }

  /**
   * Returns precision for a given label (category)
   * @param label the label.
   */
  @Since("1.1.0")
  def precision(label: Double): Double = {
    val tp = tpByClass(label)
    val fp = fpByClass.getOrElse(label, 0)
    if (tp + fp == 0) 0 else tp.toDouble / (tp + fp)
  }

  /**
   * Returns recall for a given label (category)
   * @param label the label.
   */
  @Since("1.1.0")
  def recall(label: Double): Double = tpByClass(label).toDouble / labelCountByClass(label)

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
   * Returns precision
   */
  @Since("1.1.0")
  @deprecated("Use accuracy.", "2.0.0")
  lazy val precision: Double = accuracy

  /**
   * Returns recall
   * (equals to precision for multiclass classifier
   * because sum of all false positives is equal to sum
   * of all false negatives)
   */
  @Since("1.1.0")
  @deprecated("Use accuracy.", "2.0.0")
  lazy val recall: Double = accuracy

  /**
   * Returns f-measure
   * (equals to precision and recall because precision equals recall)
   */
  @Since("1.1.0")
  @deprecated("Use accuracy.", "2.0.0")
  lazy val fMeasure: Double = accuracy

  /**
   * Returns accuracy
   * (equals to the total number of correctly classified instances
   * out of the total number of instances.)
   */
  @Since("2.0.0")
  lazy val accuracy: Double = tpByClass.values.sum.toDouble / labelCount

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
