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

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.SparkContext._

/**
 * Evaluator for multiclass classification.
 *
 * @param predictionsAndLabels an RDD of (prediction, label) pairs.
 */
@Experimental
class MulticlassMetrics(predictionsAndLabels: RDD[(Double, Double)]) extends Logging {

  private lazy val labelCountByClass = predictionsAndLabels.values.countByValue()
  private lazy val labelCount = labelCountByClass.values.sum
  private lazy val tpByClass = predictionsAndLabels
    .map{ case (prediction, label) =>
    (label, if (label == prediction) 1 else 0)
    }.reduceByKey(_ + _)
    .collectAsMap()
  private lazy val fpByClass = predictionsAndLabels
    .map{ case (prediction, label) =>
    (prediction, if (prediction != label) 1 else 0)
  }.reduceByKey(_ + _)
    .collectAsMap()

  /**
   * Returns precision for a given label (category)
   * @param label the label.
   */
  def precision(label: Double): Double = {
    val tp = tpByClass(label)
    val fp = fpByClass.getOrElse(label, 0)
    if (tp + fp == 0) 0 else tp.toDouble / (tp + fp)
  }

  /**
   * Returns recall for a given label (category)
   * @param label the label.
   */
  def recall(label: Double): Double = tpByClass(label).toDouble / labelCountByClass(label)

  /**
   * Returns f-measure for a given label (category)
   * @param label the label.
   */
  def fMeasure(label: Double, beta:Double = 1.0): Double = {
    val p = precision(label)
    val r = recall(label)
    val betaSqrd = beta * beta
    if (p + r == 0) 0 else (1 + betaSqrd) * p * r / (betaSqrd * p + r)
  }

  /**
   * Returns micro-averaged recall
   * (equals to microPrecision and microF1measure for multiclass classifier)
   */
  lazy val recall: Double =
    tpByClass.values.sum.toDouble / labelCount

  /**
   * Returns micro-averaged precision
   * (equals to microPrecision and microF1measure for multiclass classifier)
   */
  lazy val precision: Double = recall

  /**
   * Returns micro-averaged f-measure
   * (equals to microPrecision and microRecall for multiclass classifier)
   */
  lazy val fMeasure: Double = recall

  /**
   * Returns weighted averaged recall
   * (equals to micro-averaged precision, recall and f-measure)
   */
  lazy val weightedRecall: Double = labelCountByClass.map { case (category, count) =>
    recall(category) * count.toDouble / labelCount
  }.sum

  /**
   * Returns weighted averaged precision
   */
  lazy val weightedPrecision: Double = labelCountByClass.map { case (category, count) =>
    precision(category) * count.toDouble / labelCount
  }.sum

  /**
   * Returns weighted averaged f1-measure
   */
  lazy val weightedF1Measure: Double = labelCountByClass.map { case (category, count) =>
    fMeasure(category) * count.toDouble / labelCount
  }.sum

  /**
   * Returns the sequence of labels in ascending order
   */
  lazy val labels = tpByClass.unzip._1.toSeq.sorted

}
