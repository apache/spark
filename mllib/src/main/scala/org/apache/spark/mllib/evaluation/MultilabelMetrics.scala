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
 * Evaluator for multilabel classification.
 * @param predictionAndLabels an RDD of (predictions, labels) pairs,
 * both are non-null Arrays, each with unique elements.
 */
class MultilabelMetrics(predictionAndLabels: RDD[(Array[Double], Array[Double])]) {

  private lazy val numDocs: Long = predictionAndLabels.count()

  private lazy val numLabels: Long = predictionAndLabels.flatMap { case (_, labels) =>
    labels}.distinct().count()

  /**
   * Returns strict Accuracy
   * (for equal sets of labels)
   */
  lazy val strictAccuracy: Double = predictionAndLabels.filter { case (predictions, labels) =>
    predictions.deep == labels.deep }.count().toDouble / numDocs

  /**
   * Returns Accuracy
   */
  lazy val accuracy: Double = predictionAndLabels.map { case (predictions, labels) =>
    labels.intersect(predictions).size.toDouble /
      (labels.size + predictions.size - labels.intersect(predictions).size)}.sum / numDocs

  /**
   * Returns Hamming-loss
   */
  lazy val hammingLoss: Double = predictionAndLabels.map { case (predictions, labels) =>
    labels.diff(predictions).size + predictions.diff(labels).size}.
    sum / (numDocs * numLabels)

  /**
   * Returns Document-based Precision averaged by the number of documents
   */
  lazy val macroPrecisionDoc: Double = predictionAndLabels.map { case (predictions, labels) =>
    if (predictions.size > 0) {
      predictions.intersect(labels).size.toDouble / predictions.size
    } else 0
  }.sum / numDocs

  /**
   * Returns Document-based Recall averaged by the number of documents
   */
  lazy val macroRecallDoc: Double = predictionAndLabels.map { case (predictions, labels) =>
    labels.intersect(predictions).size.toDouble / labels.size}.sum / numDocs

  /**
   * Returns Document-based F1-measure averaged by the number of documents
   */
  lazy val macroF1MeasureDoc: Double = predictionAndLabels.map { case (predictions, labels) =>
    2.0 * predictions.intersect(labels).size / (predictions.size + labels.size)}.sum / numDocs

  /**
   * Returns micro-averaged document-based Precision
   * (equals to label-based microPrecision)
   */
  lazy val microPrecisionDoc: Double = microPrecisionClass

  /**
   * Returns micro-averaged document-based Recall
   * (equals to label-based microRecall)
   */
  lazy val microRecallDoc: Double = microRecallClass

  /**
   * Returns micro-averaged document-based F1-measure
   * (equals to label-based microF1measure)
   */
  lazy val microF1MeasureDoc: Double = microF1MeasureClass

  private lazy val tpPerClass = predictionAndLabels.flatMap { case (predictions, labels) =>
    predictions.intersect(labels).map(category => (category, 1))}.reduceByKey(_ + _).collectAsMap()

  private lazy val fpPerClass = predictionAndLabels.flatMap { case(predictions, labels) =>
    predictions.diff(labels).map(category => (category, 1))}.reduceByKey(_ + _).collectAsMap()

  private lazy val fnPerClass = predictionAndLabels.flatMap{ case(predictions, labels) =>
    labels.diff(predictions).map(category => (category, 1))}.reduceByKey(_ + _).collectAsMap()

  /**
   * Returns Precision for a given label (category)
   * @param label the label.
   */
  def precisionClass(label: Double) = {
    val tp = tpPerClass(label)
    val fp = fpPerClass.getOrElse(label, 0)
    if (tp + fp == 0) 0 else tp.toDouble / (tp + fp)
  }

  /**
   * Returns Recall for a given label (category)
   * @param label the label.
   */
  def recallClass(label: Double) = {
    val tp = tpPerClass(label)
    val fn = fnPerClass.getOrElse(label, 0)
    if (tp + fn == 0) 0 else tp.toDouble / (tp + fn)
  }

  /**
   * Returns F1-measure for a given label (category)
   * @param label the label.
   */
  def f1MeasureClass(label: Double) = {
    val precision = precisionClass(label)
    val recall = recallClass(label)
    if((precision + recall) == 0) 0 else 2 * precision * recall / (precision + recall)
  }

  private lazy val sumTp = tpPerClass.foldLeft(0L){ case (sum, (_, tp)) => sum + tp}
  private lazy val sumFpClass = fpPerClass.foldLeft(0L){ case (sum, (_, fp)) => sum + fp}
  private lazy val sumFnClass = fnPerClass.foldLeft(0L){ case (sum, (_, fn)) => sum + fn}

  /**
   * Returns micro-averaged label-based Precision
   */
  lazy val microPrecisionClass = {
    val sumFp = fpPerClass.foldLeft(0L){ case(cum, (_, fp)) => cum + fp}
    sumTp.toDouble / (sumTp + sumFp)
  }

  /**
   * Returns micro-averaged label-based Recall
   */
  lazy val microRecallClass = {
    val sumFn = fnPerClass.foldLeft(0.0){ case(cum, (_, fn)) => cum + fn}
    sumTp.toDouble / (sumTp + sumFn)
  }

  /**
   * Returns micro-averaged label-based F1-measure
   */
  lazy val microF1MeasureClass = 2.0 * sumTp / (2 * sumTp + sumFnClass + sumFpClass)
}
