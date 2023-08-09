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

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Evaluator for regression.
 *
 * @param predictionAndObservations an RDD of either (prediction, observation, weight)
 *                                                    or (prediction, observation) pairs
 * @param throughOrigin True if the regression is through the origin. For example, in linear
 *                      regression, it will be true without fitting intercept.
 */
@Since("1.2.0")
class RegressionMetrics @Since("2.0.0") (
    predictionAndObservations: RDD[_ <: Product], throughOrigin: Boolean)
    extends Logging {

  @Since("1.2.0")
  def this(predictionAndObservations: RDD[_ <: Product]) =
    this(predictionAndObservations, false)

  /**
   * An auxiliary constructor taking a DataFrame.
   * @param predictionAndObservations a DataFrame with two double columns:
   *                                  prediction and observation
   */
  private[mllib] def this(predictionAndObservations: DataFrame) =
    this(predictionAndObservations.rdd.map {
      case Row(prediction: Double, label: Double, weight: Double) =>
        (prediction, label, weight)
      case Row(prediction: Double, label: Double) =>
        (prediction, label, 1.0)
      case other =>
        throw new IllegalArgumentException(s"Expected Row of tuples, got $other")
    })

  /**
   * Use SummarizerBuffer to calculate summary statistics of observations and errors.
   */
  private lazy val summary = {
    val weightedVectors = predictionAndObservations.map {
      case (prediction: Double, observation: Double, weight: Double) =>
        (Vectors.dense(observation, observation - prediction, prediction), weight)
      case (prediction: Double, observation: Double) =>
        (Vectors.dense(observation, observation - prediction, prediction), 1.0)
    }
    Statistics.colStats(weightedVectors,
      Seq("mean", "normL1", "normL2", "variance"))
  }

  private lazy val SSy = math.pow(summary.normL2(0), 2)
  private lazy val SSerr = math.pow(summary.normL2(1), 2)
  private lazy val SStot = summary.variance(0) * (summary.weightSum - 1)
  private lazy val SSreg = math.pow(summary.normL2(2), 2) +
    math.pow(summary.mean(0), 2) * summary.weightSum -
    2 * summary.mean(0) * summary.mean(2) * summary.weightSum

  /**
   * Returns the variance explained by regression.
   * explainedVariance = $\sum_i (\hat{y_i} - \bar{y})^2^ / n$
   * @see <a href="https://en.wikipedia.org/wiki/Fraction_of_variance_unexplained">
   * Fraction of variance unexplained (Wikipedia)</a>
   */
  @Since("1.2.0")
  def explainedVariance: Double = {
    SSreg / summary.weightSum
  }

  /**
   * Returns the mean absolute error, which is a risk function corresponding to the
   * expected value of the absolute error loss or l1-norm loss.
   */
  @Since("1.2.0")
  def meanAbsoluteError: Double = {
    summary.normL1(1) / summary.weightSum
  }

  /**
   * Returns the mean squared error, which is a risk function corresponding to the
   * expected value of the squared error loss or quadratic loss.
   */
  @Since("1.2.0")
  def meanSquaredError: Double = {
    SSerr / summary.weightSum
  }

  /**
   * Returns the root mean squared error, which is defined as the square root of
   * the mean squared error.
   */
  @Since("1.2.0")
  def rootMeanSquaredError: Double = {
    math.sqrt(this.meanSquaredError)
  }

  /**
   * Returns R^2^, the unadjusted coefficient of determination.
   * @see <a href="http://en.wikipedia.org/wiki/Coefficient_of_determination">
   * Coefficient of determination (Wikipedia)</a>
   * In case of regression through the origin, the definition of R^2^ is to be modified.
   * @see <a href="https://online.stat.psu.edu/~ajw13/stat501/SpecialTopics/Reg_thru_origin.pdf">
   * J. G. Eisenhauer, Regression through the Origin. Teaching Statistics 25, 76-80 (2003)</a>
   */
  @Since("1.2.0")
  def r2: Double = {
    if (throughOrigin) {
      1 - SSerr / SSy
    } else {
      1 - SSerr / SStot
    }
  }

  private[spark] def count: Long = summary.count
}
