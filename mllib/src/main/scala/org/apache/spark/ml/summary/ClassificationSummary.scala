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
package org.apache.spark.ml.summary

import org.apache.spark.ml.linalg._
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType


private[spark] trait ClassificationSummary extends SupervisedPredictionSummary {

  @transient private val multiclassMetrics = {
    new MulticlassMetrics(
      predictions.select(
        col(predictionCol),
        col(labelCol).cast(DoubleType))
        .rdd.map { case Row(prediction: Double, label: Double) => (prediction, label) })
  }

  /**
   * Returns the sequence of labels in ascending order. This order matches the order used
   * in metrics which are specified as arrays over labels, e.g., truePositiveRateByLabel.
   *
   * Note: In most cases, it will be values {0.0, 1.0, ..., numClasses-1}, However, if the
   * training set is missing a label, then all of the arrays over labels
   * (e.g., from truePositiveRateByLabel) will be of length numClasses-1 instead of the
   * expected numClasses.
   */
  def labels: Array[Double] = multiclassMetrics.labels

  /** Returns true positive rate for each label (category). */
  def truePositiveRateByLabel: Array[Double] = recallByLabel

  /** Returns false positive rate for each label (category). */
  def falsePositiveRateByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.falsePositiveRate(label))
  }

  /** Returns precision for each label (category). */
  def precisionByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.precision(label))
  }

  /** Returns recall for each label (category). */
  def recallByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.recall(label))
  }

  /** Returns f-measure for each label (category). */
  def fMeasureByLabel(beta: Double): Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.fMeasure(label, beta))
  }

  /** Returns f1-measure for each label (category). */
  def fMeasureByLabel: Array[Double] = fMeasureByLabel(1.0)

  /**
   * Returns accuracy.
   * (equals to the total number of correctly classified instances
   * out of the total number of instances.)
   */
  def accuracy: Double = multiclassMetrics.accuracy

  /**
   * Returns weighted true positive rate.
   * (equals to precision, recall and f-measure)
   */
  def weightedTruePositiveRate: Double = weightedRecall

  /** Returns weighted false positive rate. */
  def weightedFalsePositiveRate: Double = multiclassMetrics.weightedFalsePositiveRate

  /**
   * Returns weighted averaged recall.
   * (equals to precision, recall and f-measure)
   */
  def weightedRecall: Double = multiclassMetrics.weightedRecall

  /** Returns weighted averaged precision. */
  def weightedPrecision: Double = multiclassMetrics.weightedPrecision

  /** Returns weighted averaged f-measure. */
  def weightedFMeasure(beta: Double): Double = multiclassMetrics.weightedFMeasure(beta)

  /** Returns weighted averaged f1-measure. */
  def weightedFMeasure: Double = multiclassMetrics.weightedFMeasure(1.0)

}

private[spark] trait ProbabilisticClassificationSummary extends ClassificationSummary {

  /** Field in "predictions" which gives the probability of each class as a vector. */
  def probabilityCol: String

}

private[spark] trait BinaryClassificationSummary extends ClassificationSummary

private[spark] trait BinaryProbabilisticClassificationSummary extends BinaryClassificationSummary
  with ProbabilisticClassificationSummary {

  private val sparkSession = predictions.sparkSession
  import sparkSession.implicits._

  // TODO: Allow the user to vary the number of bins using a setBins method in
  // BinaryClassificationMetrics. For now the default is set to 100.
  @transient private val binaryMetrics = new BinaryClassificationMetrics(
    predictions.select(col(probabilityCol), col(labelCol).cast(DoubleType)).rdd.map {
      case Row(score: Vector, label: Double) => (score(1), label)
    }, 100
  )

  /**
   * Returns the receiver operating characteristic (ROC) curve,
   * which is a Dataframe having two fields (FPR, TPR)
   * with (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
   * See http://en.wikipedia.org/wiki/Receiver_operating_characteristic
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @transient lazy val roc: DataFrame = binaryMetrics.roc().toDF("FPR", "TPR")

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  lazy val areaUnderROC: Double = binaryMetrics.areaUnderROC()

  /**
   * Returns the precision-recall curve, which is a Dataframe containing
   * two fields recall, precision with (0.0, 1.0) prepended to it.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @transient lazy val pr: DataFrame = binaryMetrics.pr().toDF("recall", "precision")

  /**
   * Returns a dataframe with two fields (threshold, F-Measure) curve with beta = 1.0.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @transient lazy val fMeasureByThreshold: DataFrame = {
    binaryMetrics.fMeasureByThreshold().toDF("threshold", "F-Measure")
  }

  /**
   * Returns a dataframe with two fields (threshold, precision) curve.
   * Every possible probability obtained in transforming the dataset are used
   * as thresholds used in calculating the precision.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @transient lazy val precisionByThreshold: DataFrame = {
    binaryMetrics.precisionByThreshold().toDF("threshold", "precision")
  }

  /**
   * Returns a dataframe with two fields (threshold, recall) curve.
   * Every possible probability obtained in transforming the dataset are used
   * as thresholds used in calculating the recall.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @transient lazy val recallByThreshold: DataFrame = {
    binaryMetrics.recallByThreshold().toDF("threshold", "recall")
  }

}

