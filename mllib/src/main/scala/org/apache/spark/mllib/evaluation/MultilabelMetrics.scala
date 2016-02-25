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

import org.apache.spark.annotation.Since
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame

/**
 * Evaluator for multilabel classification.
 * @param predictionAndLabels an RDD of (predictions, labels) pairs,
 * both are non-null Arrays, each with unique elements.
 */
@Since("1.2.0")
class MultilabelMetrics @Since("1.2.0") (predictionAndLabels: RDD[(Array[Double], Array[Double])]) {

  /**
   * An auxiliary constructor taking a DataFrame.
   * @param predictionAndLabels a DataFrame with two double array columns: prediction and label
   */
  private[mllib] def this(predictionAndLabels: DataFrame) =
    this(predictionAndLabels.map(r => (r.getSeq[Double](0).toArray, r.getSeq[Double](1).toArray)))

  private lazy val numDocs: Long = predictionAndLabels.count()

  private lazy val numLabels: Long = predictionAndLabels.flatMap { case (_, labels) =>
    labels}.distinct().count()

  /**
   * Returns subset accuracy
   * (for equal sets of labels)
   */
  @Since("1.2.0")
  lazy val subsetAccuracy: Double = predictionAndLabels.filter { case (predictions, labels) =>
    predictions.deep == labels.deep
  }.count().toDouble / numDocs

  /**
   * Returns accuracy
   */
  @Since("1.2.0")
  lazy val accuracy: Double = predictionAndLabels.map { case (predictions, labels) =>
    labels.intersect(predictions).size.toDouble /
      (labels.size + predictions.size - labels.intersect(predictions).size)}.sum / numDocs


  /**
   * Returns Hamming-loss
   */
  @Since("1.2.0")
  lazy val hammingLoss: Double = predictionAndLabels.map { case (predictions, labels) =>
    labels.size + predictions.size - 2 * labels.intersect(predictions).size
  }.sum / (numDocs * numLabels)

  /**
   * Returns document-based precision averaged by the number of documents
   */
  @Since("1.2.0")
  lazy val precision: Double = predictionAndLabels.map { case (predictions, labels) =>
    if (predictions.size > 0) {
      predictions.intersect(labels).size.toDouble / predictions.size
    } else {
      0
    }
  }.sum / numDocs

  /**
   * Returns document-based recall averaged by the number of documents
   */
  @Since("1.2.0")
  lazy val recall: Double = predictionAndLabels.map { case (predictions, labels) =>
    labels.intersect(predictions).size.toDouble / labels.size
  }.sum / numDocs

  /**
   * Returns document-based f1-measure averaged by the number of documents
   */
  @Since("1.2.0")
  lazy val f1Measure: Double = predictionAndLabels.map { case (predictions, labels) =>
    2.0 * predictions.intersect(labels).size / (predictions.size + labels.size)
  }.sum / numDocs

  private lazy val tpPerClass = predictionAndLabels.flatMap { case (predictions, labels) =>
    predictions.intersect(labels)
  }.countByValue()

  private lazy val fpPerClass = predictionAndLabels.flatMap { case (predictions, labels) =>
    predictions.diff(labels)
  }.countByValue()

  private lazy val fnPerClass = predictionAndLabels.flatMap { case(predictions, labels) =>
    labels.diff(predictions)
  }.countByValue()

  /**
   * Returns precision for a given label (category)
   * @param label the label.
   */
  @Since("1.2.0")
  def precision(label: Double): Double = {
    val tp = tpPerClass(label)
    val fp = fpPerClass.getOrElse(label, 0L)
    if (tp + fp == 0) 0.0 else tp.toDouble / (tp + fp)
  }

  /**
   * Returns recall for a given label (category)
   * @param label the label.
   */
  @Since("1.2.0")
  def recall(label: Double): Double = {
    val tp = tpPerClass(label)
    val fn = fnPerClass.getOrElse(label, 0L)
    if (tp + fn == 0) 0.0 else tp.toDouble / (tp + fn)
  }

  /**
   * Returns f1-measure for a given label (category)
   * @param label the label.
   */
  @Since("1.2.0")
  def f1Measure(label: Double): Double = {
    val p = precision(label)
    val r = recall(label)
    if((p + r) == 0) 0.0 else 2 * p * r / (p + r)
  }

  private lazy val sumTp = tpPerClass.foldLeft(0L) { case (sum, (_, tp)) => sum + tp }
  private lazy val sumFpClass = fpPerClass.foldLeft(0L) { case (sum, (_, fp)) => sum + fp }
  private lazy val sumFnClass = fnPerClass.foldLeft(0L) { case (sum, (_, fn)) => sum + fn }

  /**
   * Returns micro-averaged label-based precision
   * (equals to micro-averaged document-based precision)
   */
  @Since("1.2.0")
  lazy val microPrecision: Double = {
    val sumFp = fpPerClass.foldLeft(0L){ case(cum, (_, fp)) => cum + fp}
    sumTp.toDouble / (sumTp + sumFp)
  }

  /**
   * Returns micro-averaged label-based recall
   * (equals to micro-averaged document-based recall)
   */
  @Since("1.2.0")
  lazy val microRecall: Double = {
    val sumFn = fnPerClass.foldLeft(0.0){ case(cum, (_, fn)) => cum + fn}
    sumTp.toDouble / (sumTp + sumFn)
  }

  /**
   * Returns micro-averaged label-based f1-measure
   * (equals to micro-averaged document-based f1-measure)
   */
  @Since("1.2.0")
  lazy val microF1Measure: Double = 2.0 * sumTp / (2 * sumTp + sumFnClass + sumFpClass)

  /**
   * Returns the sequence of labels in ascending order
   */
  @Since("1.2.0")
  lazy val labels: Array[Double] = tpPerClass.keys.toArray.sorted
}
