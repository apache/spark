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

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Evaluator for multilabel classification.
 * NB: type Double both for prediction and label is retained
 * for compatibility with model.predict that returns Double
 * and MLUtils.loadLibSVMFile that loads class labels as Double
 *
 * @param predictionAndLabels an RDD of pairs (predictions, labels) sets.
 */
class MultilabelMetrics(predictionAndLabels:RDD[(Set[Double], Set[Double])]) extends Logging{

  private lazy val numDocs = predictionAndLabels.count

  private lazy val numLabels = predictionAndLabels.flatMap{case(_, labels) => labels}.distinct.count

  /**
   * Returns strict Accuracy
   * (for equal sets of labels)
   * @return strictAccuracy.
   */
  lazy val strictAccuracy = predictionAndLabels.filter{case(predictions, labels) =>
    predictions == labels}.count.toDouble / numDocs

  /**
   * Returns Accuracy
   * @return Accuracy.
   */
  lazy val accuracy = predictionAndLabels.map{ case(predictions, labels) =>
    labels.intersect(predictions).size.toDouble / labels.union(predictions).size}.
    fold(0.0)(_ + _) / numDocs

  /**
   * Returns Hamming-loss
   * @return hammingLoss.
   */
  lazy val hammingLoss = (predictionAndLabels.map{ case(predictions, labels) =>
    labels.diff(predictions).size + predictions.diff(labels).size}.
    fold(0)(_ + _)).toDouble / (numDocs * numLabels)

  /**
   * Returns Document-based Precision averaged by the number of documents
   * @return macroPrecisionDoc.
   */
  lazy val macroPrecisionDoc = (predictionAndLabels.map{ case(predictions, labels) =>
    if(predictions.size >0)
      predictions.intersect(labels).size.toDouble / predictions.size else 0}.fold(0.0)(_ + _)) /
    numDocs

  /**
   * Returns Document-based Recall averaged by the number of documents
   * @return macroRecallDoc.
   */
  lazy val macroRecallDoc = (predictionAndLabels.map{ case(predictions, labels) =>
    labels.intersect(predictions).size.toDouble / labels.size}.fold(0.0)(_ + _)) / numDocs

  /**
   * Returns Document-based F1-measure averaged by the number of documents
   * @return macroRecallDoc.
   */
  lazy val macroF1MeasureDoc = (predictionAndLabels.map{ case(predictions, labels) =>
    2.0 * predictions.intersect(labels).size /
      (predictions.size + labels.size)}.fold(0.0)(_ + _)) / numDocs

  /**
   * Returns micro-averaged document-based Precision
   * (equals to label-based microPrecision)
   * @return microPrecisionDoc.
   */
  lazy val microPrecisionDoc = microPrecisionClass

  /**
   * Returns micro-averaged document-based Recall
   * (equals to label-based microRecall)
   * @return microRecallDoc.
   */
  lazy val microRecallDoc = microRecallClass

  /**
   * Returns micro-averaged document-based F1-measure
   * (equals to label-based microF1measure)
   * @return microF1MeasureDoc.
   */
  lazy val microF1MeasureDoc = microF1MeasureClass

  private lazy val tpPerClass = predictionAndLabels.flatMap{ case(predictions, labels) =>
    predictions.intersect(labels).map(category => (category, 1))}.reduceByKey(_ + _).collectAsMap()

  private lazy val fpPerClass = predictionAndLabels.flatMap{ case(predictions, labels) =>
    predictions.diff(labels).map(category => (category, 1))}.reduceByKey(_ + _).collectAsMap()

  private lazy val fnPerClass = predictionAndLabels.flatMap{ case(predictions, labels) =>
    labels.diff(predictions).map(category => (category, 1))}.reduceByKey(_ + _).collectAsMap()

  /**
   * Returns Precision for a given label (category)
   * @param label the label.
   * @return Precision.
   */
  def precisionClass(label: Double) = if((tpPerClass(label) + fpPerClass.getOrElse(label, 0)) == 0)
    0 else tpPerClass(label).toDouble / (tpPerClass(label) + fpPerClass.getOrElse(label, 0))

  /**
   * Returns Recall for a given label (category)
   * @param label the label.
   * @return Recall.
   */
  def recallClass(label: Double) = if((tpPerClass(label) + fnPerClass.getOrElse(label, 0)) == 0)
    0 else
    tpPerClass(label).toDouble / (tpPerClass(label) + fnPerClass.getOrElse(label, 0))

  /**
   * Returns F1-measure for a given label (category)
   * @param label the label.
   * @return F1-measure.
   */
  def f1MeasureClass(label: Double) = {
    val precision = precisionClass(label)
    val recall = recallClass(label)
    if((precision + recall) == 0) 0 else 2 * precision * recall / (precision + recall)
  }

  private lazy val sumTp = tpPerClass.foldLeft(0L){ case(sum, (_, tp)) => sum + tp}
  private lazy val sumFpClass = fpPerClass.foldLeft(0L){ case(sum, (_, fp)) => sum + fp}
  private lazy val sumFnClass = fnPerClass.foldLeft(0L){ case(sum, (_, fn)) => sum + fn}

  /**
   * Returns micro-averaged label-based Precision
   * @return microPrecisionClass.
   */
  lazy val microPrecisionClass = {
    val sumFp = fpPerClass.foldLeft(0L){ case(sumFp, (_, fp)) => sumFp + fp}
    sumTp.toDouble / (sumTp + sumFp)
  }

  /**
   * Returns micro-averaged label-based Recall
   * @return microRecallClass.
   */
  lazy val microRecallClass = {
    val sumFn = fnPerClass.foldLeft(0.0){ case(sumFn, (_, fn)) => sumFn + fn}
    sumTp.toDouble / (sumTp + sumFn)
  }

  /**
   * Returns micro-averaged label-based F1-measure
   * @return microRecallClass.
   */
  lazy val microF1MeasureClass = 2.0 * sumTp / (2 * sumTp + sumFnClass + sumFpClass)

}
