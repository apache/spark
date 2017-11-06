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

import breeze.stats.distributions.StudentsT

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * :: Experimental ::
 * Linear regression training results. Currently, the training summary ignores the
 * training weights except for the objective trace.
 *
 * @param predictions predictions output by the model's `transform` method.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
@Since("1.5.0")
@Experimental
class LinearRegressionTrainingSummary private[ml] (
    predictions: DataFrame,
    predictionCol: String,
    labelCol: String,
    featuresCol: String,
    model: LinearRegressionModel,
    diagInvAtWA: Array[Double],
    val objectiveHistory: Array[Double])
  extends LinearRegressionSummary(
    predictions,
    predictionCol,
    labelCol,
    featuresCol,
    model,
    diagInvAtWA) {

  /**
   * Number of training iterations until termination
   *
   * This value is only available when using the "l-bfgs" solver.
   *
   * @see `LinearRegression.solver`
   */
  @Since("1.5.0")
  val totalIterations: Int = objectiveHistory.length

}

/**
 * :: Experimental ::
 * Linear regression results evaluated on a dataset.
 *
 * @param predictions predictions output by the model's `transform` method.
 * @param predictionCol Field in "predictions" which gives the predicted value of the label at
 *                      each instance.
 * @param labelCol Field in "predictions" which gives the true label of each instance.
 * @param featuresCol Field in "predictions" which gives the features of each instance as a vector.
 */
@Since("1.5.0")
@Experimental
class LinearRegressionSummary private[ml] (
    @transient override val predictions: DataFrame,
    override val predictionCol: String,
    override val labelCol: String,
    override val featuresCol: String,
    private val privateModel: LinearRegressionModel,
    private val diagInvAtWA: Array[Double]) extends RegressionSummary {

  override protected val throughOrigin: Boolean = privateModel.getFitIntercept

  /** Degrees of freedom */
  @Since("2.2.0")
  val degreesOfFreedom: Long = if (privateModel.getFitIntercept) {
    numInstances - privateModel.coefficients.size - 1
  } else {
    numInstances - privateModel.coefficients.size
  }

  /**
   * The weighted residuals, the usual residuals rescaled by
   * the square root of the instance weights.
   */
  lazy val devianceResiduals: Array[Double] = {
    val weighted =
      if (!privateModel.isDefined(privateModel.weightCol) || privateModel.getWeightCol.isEmpty) {
        lit(1.0)
      } else {
        sqrt(col(privateModel.getWeightCol))
      }
    val dr = predictions
      .select(col(privateModel.getLabelCol).minus(col(privateModel.getPredictionCol))
        .multiply(weighted).as("weightedResiduals"))
      .select(min(col("weightedResiduals")).as("min"), max(col("weightedResiduals")).as("max"))
      .first()
    Array(dr.getDouble(0), dr.getDouble(1))
  }

  /**
   * Standard error of estimated coefficients and intercept.
   * This value is only available when using the "normal" solver.
   *
   * If `LinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   *
   * @see `LinearRegression.solver`
   */
  lazy val coefficientStandardErrors: Array[Double] = {
    if (diagInvAtWA.length == 1 && diagInvAtWA(0) == 0) {
      throw new UnsupportedOperationException(
        "No Std. Error of coefficients available for this LinearRegressionModel")
    } else {
      val rss =
        if (!privateModel.isDefined(privateModel.weightCol) || privateModel.getWeightCol.isEmpty) {
          meanSquaredError * numInstances
        } else {
          val t = udf { (pred: Double, label: Double, weight: Double) =>
            math.pow(label - pred, 2.0) * weight }
          predictions.select(t(col(privateModel.getPredictionCol), col(privateModel.getLabelCol),
            col(privateModel.getWeightCol)).as("wse")).agg(sum(col("wse"))).first().getDouble(0)
        }
      val sigma2 = rss / degreesOfFreedom
      diagInvAtWA.map(_ * sigma2).map(math.sqrt)
    }
  }

  /**
   * T-statistic of estimated coefficients and intercept.
   * This value is only available when using the "normal" solver.
   *
   * If `LinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   *
   * @see `LinearRegression.solver`
   */
  lazy val tValues: Array[Double] = {
    if (diagInvAtWA.length == 1 && diagInvAtWA(0) == 0) {
      throw new UnsupportedOperationException(
        "No t-statistic available for this LinearRegressionModel")
    } else {
      val estimate = if (privateModel.getFitIntercept) {
        Array.concat(privateModel.coefficients.toArray, Array(privateModel.intercept))
      } else {
        privateModel.coefficients.toArray
      }
      estimate.zip(coefficientStandardErrors).map { x => x._1 / x._2 }
    }
  }

  /**
   * Two-sided p-value of estimated coefficients and intercept.
   * This value is only available when using the "normal" solver.
   *
   * If `LinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   *
   * @see `LinearRegression.solver`
   */
  lazy val pValues: Array[Double] = {
    if (diagInvAtWA.length == 1 && diagInvAtWA(0) == 0) {
      throw new UnsupportedOperationException(
        "No p-value available for this LinearRegressionModel")
    } else {
      tValues.map { x => 2.0 * (1.0 - StudentsT(degreesOfFreedom.toDouble).cdf(math.abs(x))) }
    }
  }

}

