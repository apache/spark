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
import org.apache.spark.mllib.stat.{MultivariateOnlineSummarizer, MultivariateStatisticalSummary}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * Evaluator for regression.
 *
 * @param predictionAndObservations an RDD of (prediction, observation) pairs
 * @param throughOrigin True if the regression is through the origin. For example, in linear
 *                      regression, it will be true without fitting intercept.
 */
@Since("1.2.0")
class RegressionMetrics @Since("2.0.0") (
    predictionAndObservations: RDD[(Double, Double)], throughOrigin: Boolean)
    extends Logging {

  @Since("1.2.0")
  def this(predictionAndObservations: RDD[(Double, Double)]) =
    this(predictionAndObservations, false)

  /**
   * An auxiliary constructor taking a DataFrame.
   * @param predictionAndObservations a DataFrame with two double columns:
   *                                  prediction and observation
   */
  private[mllib] def this(predictionAndObservations: DataFrame) =
    this(predictionAndObservations.rdd.map(r => (r.getDouble(0), r.getDouble(1))))

  /**
   * Use MultivariateOnlineSummarizer to calculate summary statistics of observations and errors.
   */
  private lazy val summary: MultivariateStatisticalSummary = {
    val summary: MultivariateStatisticalSummary = predictionAndObservations.map {
      case (prediction, observation) => Vectors.dense(observation, observation - prediction)
    }.aggregate(new MultivariateOnlineSummarizer())(
        (summary, v) => summary.add(v),
        (sum1, sum2) => sum1.merge(sum2)
      )
    summary
  }

  private lazy val SSy = math.pow(summary.normL2(0), 2)
  private lazy val SSerr = math.pow(summary.normL2(1), 2)
  private lazy val SStot = summary.variance(0) * (summary.count - 1)
  private lazy val SSreg = {
    val yMean = summary.mean(0)
    predictionAndObservations.map {
      case (prediction, _) => math.pow(prediction - yMean, 2)
    }.sum()
  }

  /**
   * Returns the variance explained by regression.
   * explainedVariance = \sum_i (\hat{y_i} - \bar{y})^2 / n
   * @see [[https://en.wikipedia.org/wiki/Fraction_of_variance_unexplained]]
   */
  @Since("1.2.0")
  def explainedVariance: Double = {
    SSreg / summary.count
  }

  /**
   * Returns the mean absolute error, which is a risk function corresponding to the
   * expected value of the absolute error loss or l1-norm loss.
   */
  @Since("1.2.0")
  def meanAbsoluteError: Double = {
    summary.normL1(1) / summary.count
  }

  /**
   * Returns the mean squared error, which is a risk function corresponding to the
   * expected value of the squared error loss or quadratic loss.
   */
  @Since("1.2.0")
  def meanSquaredError: Double = {
    SSerr / summary.count
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
   * @see [[http://en.wikipedia.org/wiki/Coefficient_of_determination]]
   * In case of regression through the origin, the definition of R^2^ is to be modified.
   * @see J. G. Eisenhauer, Regression through the Origin. Teaching Statistics 25, 76-80 (2003)
   * [[https://online.stat.psu.edu/~ajw13/stat501/SpecialTopics/Reg_thru_origin.pdf]]
   */
  @Since("1.2.0")
  def r2: Double = {
    if (throughOrigin) {
      1 - SSerr / SSy
    } else {
      1 - SSerr / SStot
    }
  }
}
