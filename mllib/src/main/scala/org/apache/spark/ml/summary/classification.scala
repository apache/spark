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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

/**
 * :: Experimental ::
 * Abstraction for logistic regression results for a given model.
 *
 * Currently, the summary ignores the instance weights.
 */
@Experimental
trait LogisticRegressionSummary extends Summary {

  /** Field in "predictions" which gives the probability of each class as a vector. */
  @Since("1.5.0")
  def probabilityCol: String

  /** Field in "predictions" which gives the true label of each instance (if available). */
  @Since("1.5.0")
  def labelCol: String

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
  @Since("2.3.0")
  def labels: Array[Double] = multiclassMetrics.labels

  /** Returns true positive rate for each label (category). */
  @Since("2.3.0")
  def truePositiveRateByLabel: Array[Double] = recallByLabel

  /** Returns false positive rate for each label (category). */
  @Since("2.3.0")
  def falsePositiveRateByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.falsePositiveRate(label))
  }

  /** Returns precision for each label (category). */
  @Since("2.3.0")
  def precisionByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.precision(label))
  }

  /** Returns recall for each label (category). */
  @Since("2.3.0")
  def recallByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.recall(label))
  }

  /** Returns f-measure for each label (category). */
  @Since("2.3.0")
  def fMeasureByLabel(beta: Double): Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.fMeasure(label, beta))
  }

  /** Returns f1-measure for each label (category). */
  @Since("2.3.0")
  def fMeasureByLabel: Array[Double] = fMeasureByLabel(1.0)

  /**
   * Returns accuracy.
   * (equals to the total number of correctly classified instances
   * out of the total number of instances.)
   */
  @Since("2.3.0")
  def accuracy: Double = multiclassMetrics.accuracy

  /**
   * Returns weighted true positive rate.
   * (equals to precision, recall and f-measure)
   */
  @Since("2.3.0")
  def weightedTruePositiveRate: Double = weightedRecall

  /** Returns weighted false positive rate. */
  @Since("2.3.0")
  def weightedFalsePositiveRate: Double = multiclassMetrics.weightedFalsePositiveRate

  /**
   * Returns weighted averaged recall.
   * (equals to precision, recall and f-measure)
   */
  @Since("2.3.0")
  def weightedRecall: Double = multiclassMetrics.weightedRecall

  /** Returns weighted averaged precision. */
  @Since("2.3.0")
  def weightedPrecision: Double = multiclassMetrics.weightedPrecision

  /** Returns weighted averaged f-measure. */
  @Since("2.3.0")
  def weightedFMeasure(beta: Double): Double = multiclassMetrics.weightedFMeasure(beta)

  /** Returns weighted averaged f1-measure. */
  @Since("2.3.0")
  def weightedFMeasure: Double = multiclassMetrics.weightedFMeasure(1.0)

  /**
   * Convenient method for casting to binary logistic regression summary.
   * This method will throws an Exception if the summary is not a binary summary.
   */
  @Since("2.3.0")
  def asBinary: BinaryLogisticRegressionSummary = this match {
    case b: BinaryLogisticRegressionSummary => b
    case _ =>
      throw new RuntimeException("Cannot cast to a binary summary.")
  }
}

/**
 * :: Experimental ::
 * Abstraction for multiclass logistic regression training results.
 * Currently, the training summary ignores the training weights except
 * for the objective trace.
 */
@Experimental
sealed trait LogisticRegressionTrainingSummary extends LogisticRegressionSummary {

  /** objective function (scaled loss + regularization) at each iteration. */
  @Since("1.5.0")
  def objectiveHistory: Array[Double]

  /** Number of training iterations. */
  @Since("1.5.0")
  def totalIterations: Int = objectiveHistory.length

}

/**
 * :: Experimental ::
 * Abstraction for binary logistic regression results for a given model.
 *
 * Currently, the summary ignores the instance weights.
 */
@Experimental
sealed trait BinaryLogisticRegressionSummary extends LogisticRegressionSummary {

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
  @Since("1.5.0")
  @transient lazy val roc: DataFrame = binaryMetrics.roc().toDF("FPR", "TPR")

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  lazy val areaUnderROC: Double = binaryMetrics.areaUnderROC()

  /**
   * Returns the precision-recall curve, which is a Dataframe containing
   * two fields recall, precision with (0.0, 1.0) prepended to it.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  @transient lazy val pr: DataFrame = binaryMetrics.pr().toDF("recall", "precision")

  /**
   * Returns a dataframe with two fields (threshold, F-Measure) curve with beta = 1.0.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
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
  @Since("1.5.0")
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
  @Since("1.5.0")
  @transient lazy val recallByThreshold: DataFrame = {
    binaryMetrics.recallByThreshold().toDF("threshold", "recall")
  }
}

/**
 * :: Experimental ::
 * Abstraction for binary logistic regression training results.
 * Currently, the training summary ignores the training weights except
 * for the objective trace.
 */
@Experimental
sealed trait BinaryLogisticRegressionTrainingSummary extends BinaryLogisticRegressionSummary
  with LogisticRegressionTrainingSummary

/**
 * Multiclass logistic regression training results.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the probability of
 *                       each class as a vector.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param featuresCol field in "predictions" which gives the features of each instance as a vector.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
private[ml] class LogisticRegressionTrainingSummaryImpl(
    predictions: DataFrame,
    probabilityCol: String,
    predictionCol: String,
    labelCol: String,
    featuresCol: String,
    override val objectiveHistory: Array[Double])
  extends LogisticRegressionSummaryImpl(
    predictions, probabilityCol, predictionCol, labelCol, featuresCol)
    with LogisticRegressionTrainingSummary

/**
 * Multiclass logistic regression results for a given model.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the probability of
 *                       each class as a vector.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param featuresCol field in "predictions" which gives the features of each instance as a vector.
 */
private[ml] class LogisticRegressionSummaryImpl(
    @transient override val predictions: DataFrame,
    override val probabilityCol: String,
    override val predictionCol: String,
    override val labelCol: String,
    override val featuresCol: String)
  extends LogisticRegressionSummary

/**
 * Binary logistic regression training results.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the probability of
 *                       each class as a vector.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param featuresCol field in "predictions" which gives the features of each instance as a vector.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
private[ml] class BinaryLogisticRegressionTrainingSummaryImpl(
    predictions: DataFrame,
    probabilityCol: String,
    predictionCol: String,
    labelCol: String,
    featuresCol: String,
    override val objectiveHistory: Array[Double])
  extends BinaryLogisticRegressionSummaryImpl(
    predictions, probabilityCol, predictionCol, labelCol, featuresCol)
    with BinaryLogisticRegressionTrainingSummary

/**
 * Binary logistic regression results for a given model.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the probability of
 *                       each class as a vector.
 * @param predictionCol field in "predictions" which gives the prediction of
 *                      each class as a double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param featuresCol field in "predictions" which gives the features of each instance as a vector.
 */
private[ml] class BinaryLogisticRegressionSummaryImpl(
    predictions: DataFrame,
    probabilityCol: String,
    predictionCol: String,
    labelCol: String,
    featuresCol: String)
  extends LogisticRegressionSummaryImpl(
    predictions, probabilityCol, predictionCol, labelCol, featuresCol)
    with BinaryLogisticRegressionSummary

