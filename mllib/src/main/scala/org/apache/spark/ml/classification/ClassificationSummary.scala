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

package org.apache.spark.ml.classification

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.Summary
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType


/**
 * Abstraction for multiclass classification results for a given model.
 */
private[spark] trait ClassificationSummary extends Summary with Serializable {

  /**
   * Dataframe output by the model's `transform` method.
   */
  @Since("3.1.0")
  def predictions: DataFrame

  /** Field in "predictions" which gives the prediction of each class. */
  @Since("3.1.0")
  def predictionCol: String

  /** Field in "predictions" which gives the true label of each instance (if available). */
  @Since("3.1.0")
  def labelCol: String

  /** Field in "predictions" which gives the weight of each instance. */
  @Since("3.1.0")
  def weightCol: String

  @transient private val multiclassMetrics = {
    val weightColumn = if (predictions.schema.fieldNames.contains(weightCol)) {
      col(weightCol).cast(DoubleType)
    } else {
      lit(1.0)
    }
    new MulticlassMetrics(
      predictions.select(col(predictionCol), col(labelCol).cast(DoubleType), weightColumn)
        .rdd.map {
          case Row(prediction: Double, label: Double, weight: Double) => (prediction, label, weight)
      })
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
  @Since("3.1.0")
  def labels: Array[Double] = multiclassMetrics.labels

  /** Returns true positive rate for each label (category). */
  @Since("3.1.0")
  def truePositiveRateByLabel: Array[Double] = recallByLabel

  /** Returns false positive rate for each label (category). */
  @Since("3.1.0")
  def falsePositiveRateByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.falsePositiveRate(label))
  }

  /** Returns precision for each label (category). */
  @Since("3.1.0")
  def precisionByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.precision(label))
  }

  /** Returns recall for each label (category). */
  @Since("3.1.0")
  def recallByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.recall(label))
  }

  /** Returns f-measure for each label (category). */
  @Since("3.1.0")
  def fMeasureByLabel(beta: Double): Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.fMeasure(label, beta))
  }

  /** Returns f1-measure for each label (category). */
  @Since("3.1.0")
  def fMeasureByLabel: Array[Double] = fMeasureByLabel(1.0)

  /**
   * Returns accuracy.
   * (equals to the total number of correctly classified instances
   * out of the total number of instances.)
   */
  @Since("3.1.0")
  def accuracy: Double = multiclassMetrics.accuracy

  /**
   * Returns weighted true positive rate.
   * (equals to precision, recall and f-measure)
   */
  @Since("3.1.0")
  def weightedTruePositiveRate: Double = weightedRecall

  /** Returns weighted false positive rate. */
  @Since("3.1.0")
  def weightedFalsePositiveRate: Double = multiclassMetrics.weightedFalsePositiveRate

  /**
   * Returns weighted averaged recall.
   * (equals to precision, recall and f-measure)
   */
  @Since("3.1.0")
  def weightedRecall: Double = multiclassMetrics.weightedRecall

  /** Returns weighted averaged precision. */
  @Since("3.1.0")
  def weightedPrecision: Double = multiclassMetrics.weightedPrecision

  /** Returns weighted averaged f-measure. */
  @Since("3.1.0")
  def weightedFMeasure(beta: Double): Double = multiclassMetrics.weightedFMeasure(beta)

  /** Returns weighted averaged f1-measure. */
  @Since("3.1.0")
  def weightedFMeasure: Double = multiclassMetrics.weightedFMeasure(1.0)
}

/**
 * Abstraction for training results.
 */
private[spark] trait TrainingSummary {

  /**
   *  objective function (scaled loss + regularization) at each iteration.
   *  It contains one more element, the initial state, than number of iterations.
   */
  @Since("3.1.0")
  def objectiveHistory: Array[Double]

  /** Number of training iterations. */
  @Since("3.1.0")
  def totalIterations: Int = {
    assert(objectiveHistory.length > 0, "objectiveHistory length should be greater than 0.")
    objectiveHistory.length - 1
  }
}

/**
 * Abstraction for binary classification results for a given model.
 */
private[spark] trait BinaryClassificationSummary extends ClassificationSummary {

  private val sparkSession = predictions.sparkSession
  import sparkSession.implicits._

  /**
   *  Field in "predictions" which gives the probability or rawPrediction of each class as a
   *  vector.
   */
  def scoreCol: String = null

  @transient private val binaryMetrics = {
    val weightColumn = if (predictions.schema.fieldNames.contains(weightCol)) {
      col(weightCol).cast(DoubleType)
    } else {
      lit(1.0)
    }

    // TODO: Allow the user to vary the number of bins using a setBins method in
    // BinaryClassificationMetrics. For now the default is set to 1000.
    new BinaryClassificationMetrics(
      predictions.select(col(scoreCol), col(labelCol).cast(DoubleType), weightColumn).rdd.map {
        case Row(score: Vector, label: Double, weight: Double) => (score(1), label, weight)
      }, 1000
    )
  }

  /**
   * Returns the receiver operating characteristic (ROC) curve,
   * which is a Dataframe having two fields (FPR, TPR)
   * with (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
   * See http://en.wikipedia.org/wiki/Receiver_operating_characteristic
   */
  @Since("3.1.0")
  @transient lazy val roc: DataFrame = binaryMetrics.roc().toDF("FPR", "TPR")

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   */
  @Since("3.1.0")
  lazy val areaUnderROC: Double = binaryMetrics.areaUnderROC()

  /**
   * Returns the precision-recall curve, which is a Dataframe containing
   * two fields recall, precision with (0.0, 1.0) prepended to it.
   */
  @Since("3.1.0")
  @transient lazy val pr: DataFrame = binaryMetrics.pr().toDF("recall", "precision")

  /**
   * Returns a dataframe with two fields (threshold, F-Measure) curve with beta = 1.0.
   */
  @Since("3.1.0")
  @transient lazy val fMeasureByThreshold: DataFrame = {
    binaryMetrics.fMeasureByThreshold().toDF("threshold", "F-Measure")
  }

  /**
   * Returns a dataframe with two fields (threshold, precision) curve.
   * Every possible probability obtained in transforming the dataset are used
   * as thresholds used in calculating the precision.
   */
  @Since("3.1.0")
  @transient lazy val precisionByThreshold: DataFrame = {
    binaryMetrics.precisionByThreshold().toDF("threshold", "precision")
  }

  /**
   * Returns a dataframe with two fields (threshold, recall) curve.
   * Every possible probability obtained in transforming the dataset are used
   * as thresholds used in calculating the recall.
   */
  @Since("3.1.0")
  @transient lazy val recallByThreshold: DataFrame = {
    binaryMetrics.recallByThreshold().toDF("threshold", "recall")
  }
}
