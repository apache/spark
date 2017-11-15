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
import org.apache.spark.sql.DataFrame

/**
 * :: Experimental ::
 * Abstraction for logistic regression results for a given model.
 *
 * Currently, the summary ignores the instance weights.
 */
@Experimental
sealed trait LogisticRegressionSummary extends ProbabilisticClassificationSummary {

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
sealed trait BinaryLogisticRegressionSummary extends LogisticRegressionSummary
  with BinaryProbabilisticClassificationSummary


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
    override val objectiveHistory: Array[Double]) extends LogisticRegressionSummaryImpl(
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
    override val objectiveHistory: Array[Double]) extends BinaryLogisticRegressionSummaryImpl(
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
