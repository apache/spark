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
import org.apache.spark.ml.ModelSummary
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType


/**
 * Abstraction for multiclass classification results for a given model.
 */
private[spark] trait ClassificationSummary extends ModelSummary with Serializable {

  /**
   * Dataframe output by the model's `transform` method.
   */
  @Since("3.1.0")
  def predictions: DataFrame = getModelSummaryAttrDataFrame("predictions")

  /** Field in "predictions" which gives the prediction of each class. */
  @Since("3.1.0")
  def predictionCol: String = getModelSummaryAttr("predictionCol").asInstanceOf[String]

  /** Field in "predictions" which gives the true label of each instance (if available). */
  @Since("3.1.0")
  def labelCol: String = getModelSummaryAttr("labelCol").asInstanceOf[String]

  /** Field in "predictions" which gives the weight of each instance. */
  @Since("3.1.0")
  def weightCol: String = getModelSummaryAttr("weightCol").asInstanceOf[String]

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
  def labels: Array[Double] = getModelSummaryAttr("labels").asInstanceOf[Array[Double]]

  /** Returns true positive rate for each label (category). */
  @Since("3.1.0")
  def truePositiveRateByLabel: Array[Double] = {
    getModelSummaryAttr("truePositiveRateByLabel").asInstanceOf[Array[Double]]
  }

  /** Returns false positive rate for each label (category). */
  @Since("3.1.0")
  def falsePositiveRateByLabel: Array[Double] = {
    getModelSummaryAttr("falsePositiveRateByLabel").asInstanceOf[Array[Double]]
  }

  /** Returns precision for each label (category). */
  @Since("3.1.0")
  def precisionByLabel: Array[Double] = {
    getModelSummaryAttr("precisionByLabel").asInstanceOf[Array[Double]]
  }

  /** Returns recall for each label (category). */
  @Since("3.1.0")
  def recallByLabel: Array[Double] = {
    getModelSummaryAttr("recallByLabel").asInstanceOf[Array[Double]]
  }

  /** Returns f-measure for each label (category). */
  @Since("3.1.0")
  def fMeasureByLabel(beta: Double): Array[Double] = {
    // TODO
    throw new UnsupportedOperationException()
  }

  /** Returns f1-measure for each label (category). */
  @Since("3.1.0")
  def fMeasureByLabel: Array[Double] = {
    getModelSummaryAttr("fMeasureByLabel").asInstanceOf[Array[Double]]
  }

  /**
   * Returns accuracy.
   * (equals to the total number of correctly classified instances
   * out of the total number of instances.)
   */
  @Since("3.1.0")
  def accuracy: Double = {
    getModelSummaryAttr("accuracy").asInstanceOf[Double]
  }

  /**
   * Returns weighted true positive rate.
   * (equals to precision, recall and f-measure)
   */
  @Since("3.1.0")
  def weightedTruePositiveRate: Double = {
    getModelSummaryAttr("weightedTruePositiveRate").asInstanceOf[Double]
  }

  /** Returns weighted false positive rate. */
  @Since("3.1.0")
  def weightedFalsePositiveRate: Double = {
    getModelSummaryAttr("weightedFalsePositiveRate").asInstanceOf[Double]
  }

  /**
   * Returns weighted averaged recall.
   * (equals to precision, recall and f-measure)
   */
  @Since("3.1.0")
  def weightedRecall: Double = {
    getModelSummaryAttr("weightedRecall").asInstanceOf[Double]
  }

  /** Returns weighted averaged precision. */
  @Since("3.1.0")
  def weightedPrecision: Double = {
    getModelSummaryAttr("weightedPrecision").asInstanceOf[Double]
  }

  /** Returns weighted averaged f-measure. */
  @Since("3.1.0")
  def weightedFMeasure(beta: Double): Double = {
    // TODO
    throw new UnsupportedOperationException()
  }

  /** Returns weighted averaged f1-measure. */
  @Since("3.1.0")
  def weightedFMeasure: Double = {
    getModelSummaryAttr("weightedFMeasure").asInstanceOf[Double]
  }
}

/**
 * Abstraction for training results.
 */
private[spark] trait TrainingSummary extends ModelSummary {

  /**
   *  objective function (scaled loss + regularization) at each iteration.
   *  It contains one more element, the initial state, than number of iterations.
   */
  @Since("3.1.0")
  def objectiveHistory: Array[Double] = {
    getModelSummaryAttr("objectiveHistory").asInstanceOf[Array[Double]]
  }

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

  /**
   *  Field in "predictions" which gives the probability or rawPrediction of each class as a
   *  vector.
   */
  def scoreCol: String = getModelSummaryAttr("scoreCol").asInstanceOf[String]

  /**
   * Returns the receiver operating characteristic (ROC) curve,
   * which is a Dataframe having two fields (FPR, TPR)
   * with (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
   * See http://en.wikipedia.org/wiki/Receiver_operating_characteristic
   */
  @Since("3.1.0")
  @transient lazy val roc: DataFrame = {
    getModelSummaryAttrDataFrame("roc")
  }

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   */
  @Since("3.1.0")
  lazy val areaUnderROC: Double = {
    getModelSummaryAttr("areaUnderROC").asInstanceOf[Double]
  }

  /**
   * Returns the precision-recall curve, which is a Dataframe containing
   * two fields recall, precision with (0.0, 1.0) prepended to it.
   */
  @Since("3.1.0")
  @transient lazy val pr: DataFrame = {
    getModelSummaryAttrDataFrame("pr")
  }

  /**
   * Returns a dataframe with two fields (threshold, precision) curve.
   * Every possible probability obtained in transforming the dataset are used
   * as thresholds used in calculating the precision.
   */
  @Since("3.1.0")
  @transient lazy val precisionByThreshold: DataFrame = {
    getModelSummaryAttrDataFrame("precisionByThreshold")
  }

  /**
   * Returns a dataframe with two fields (threshold, recall) curve.
   * Every possible probability obtained in transforming the dataset are used
   * as thresholds used in calculating the recall.
   */
  @Since("3.1.0")
  @transient lazy val recallByThreshold: DataFrame = {
    getModelSummaryAttrDataFrame("recallByThreshold")
  }
}
