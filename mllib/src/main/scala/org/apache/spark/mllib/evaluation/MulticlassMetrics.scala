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
import org.apache.spark.Logging
import org.apache.spark.SparkContext._

/**
 * Evaluator for multiclass classification.
 * NB: type Double both for prediction and label is retained
 * for compatibility with model.predict that returns Double
 * and MLUtils.loadLibSVMFile that loads class labels as Double
 *
 * @param predictionsAndLabels an RDD of (prediction, label) pairs.
 */
class MulticlassMetrics(predictionsAndLabels: RDD[(Double, Double)]) extends Logging {

  /* class = category; label = instance of class; prediction = instance of class */

  private lazy val labelCountByClass = predictionsAndLabels.values.countByValue()
  private lazy val labelCount = labelCountByClass.foldLeft(0L){case(sum, (_, count)) => sum + count}
  private lazy val tpByClass = predictionsAndLabels.map{ case (prediction, label) =>
    (label, if(label == prediction) 1 else 0) }.reduceByKey{_ + _}.collectAsMap
  private lazy val fpByClass = predictionsAndLabels.map{ case (prediction, label) =>
    (prediction, if(prediction != label) 1 else 0) }.reduceByKey{_ + _}.collectAsMap

  /**
   * Returns Precision for a given label (category)
   * @param label the label.
   * @return Precision.
   */
  def precision(label: Double): Double = if(tpByClass(label) + fpByClass.getOrElse(label, 0) == 0) 0
    else tpByClass(label).toDouble / (tpByClass(label) + fpByClass.getOrElse(label, 0)).toDouble

  /**
   * Returns Recall for a given label (category)
   * @param label the label.
   * @return Recall.
   */
  def recall(label: Double): Double = tpByClass(label).toDouble / labelCountByClass(label).toDouble

  /**
   * Returns F1-measure for a given label (category)
   * @param label the label.
   * @return F1-measure.
   */
  def f1Measure(label: Double): Double =
    2 * precision(label) * recall(label) / (precision(label) + recall(label))

  /**
   * Returns micro-averaged Recall
   * (equals to microPrecision and microF1measure for multiclass classifier)
   * @return microRecall.
   */
  def microRecall: Double =
    tpByClass.foldLeft(0L){case (sum,(_, tp)) => sum + tp}.toDouble / labelCount.toDouble

  /**
   * Returns micro-averaged Precision
   * (equals to microPrecision and microF1measure for multiclass classifier)
   * @return microPrecision.
   */
  def microPrecision: Double = microRecall

  /**
   * Returns micro-averaged F1-measure
   * (equals to microPrecision and microRecall for multiclass classifier)
   * @return microF1measure.
   */
  def microF1Measure: Double = microRecall

  /**
   * Returns weighted averaged Recall
   * @return weightedRecall.
   */
  def weightedRecall: Double = labelCountByClass.foldLeft(0.0){case(wRecall, (category, count)) =>
    wRecall + recall(category) * count.toDouble / labelCount.toDouble}

  /**
   * Returns weighted averaged Precision
   * @return weightedPrecision.
   */
  def weightedPrecision: Double =
    labelCountByClass.foldLeft(0.0){case(wPrecision, (category, count)) =>
    wPrecision + precision(category) * count.toDouble / labelCount.toDouble}

  /**
   * Returns weighted averaged F1-measure
   * @return weightedF1Measure.
   */
  def weightedF1Measure: Double =
    labelCountByClass.foldLeft(0.0){case(wF1measure, (category, count)) =>
    wF1measure + f1Measure(category) * count.toDouble / labelCount.toDouble}

  /**
   * Returns map with Precisions for individual classes
   * @return precisionPerClass.
   */
  def precisionPerClass =
    labelCountByClass.map{case (category, _) => (category, precision(category))}.toMap

  /**
   * Returns map with Recalls for individual classes
   * @return recallPerClass.
   */
  def recallPerClass =
    labelCountByClass.map{case (category, _) => (category, recall(category))}.toMap

  /**
   * Returns map with F1-measures for individual classes
   * @return f1MeasurePerClass.
   */
  def f1MeasurePerClass =
    labelCountByClass.map{case (category, _) => (category, f1Measure(category))}.toMap
}
