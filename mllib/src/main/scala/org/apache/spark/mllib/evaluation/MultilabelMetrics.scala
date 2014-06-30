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

  private lazy val numDocs = predictionAndLabels.count()

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
    predictions.intersect(labels).size.toDouble / labels.size}.fold(0.0)(_ + _)) / numDocs

  /**
   * Returns micro-averaged document-based Precision
   * @return microPrecisionDoc.
   */
  lazy val microPrecisionDoc = {
    val (sumTp, sumPredictions) = predictionAndLabels.map{ case(predictions, labels) =>
      (predictions.intersect(labels).size, predictions.size)}.
      fold((0, 0)){ case((tp1, predictions1), (tp2, predictions2)) =>
      (tp1 + tp2, predictions1 + predictions2)}
    sumTp.toDouble / sumPredictions
  }

  /**
   * Returns micro-averaged document-based Recall
   * @return microRecallDoc.
   */
  lazy val microRecallDoc = {
    val (sumTp, sumLabels) = predictionAndLabels.map{ case(predictions, labels) =>
      (predictions.intersect(labels).size, labels.size)}.
      fold((0, 0)){ case((tp1, labels1), (tp2, labels2)) =>
      (tp1 + tp2, labels1 + labels2)}
    sumTp.toDouble / sumLabels
  }

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

  private lazy val sumTp = tpPerClass.foldLeft(0L){ case(sumTp, (_, tp)) => sumTp + tp}

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
  lazy val microF1MeasureClass = {
    val precision = microPrecisionClass
    val recall = microRecallClass
    if((precision + recall) == 0) 0 else 2 * precision * recall / (precision + recall)
  }

}
