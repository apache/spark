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
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, MultivariateOnlineSummarizer}
import org.apache.spark.sql.DataFrame

/**
 * Evaluator for regression.
 *
 * @param predictionAndObservationsWithWeight an RDD of (prediction, observation, weight) tuples.
 */
@Since("1.2.0")
class RegressionMetrics @Since("1.2.0") (
    predictionAndObservationsWithWeight: => RDD[(Double, Double, Double)]) extends Logging {

  /**
    * An auxiliary constructor taking RDD without weights of sample datasets.
    * In this case, the default weight of each sample should be 1.0
    * as MultivariateOnlineSummarizer do so.
    * @param predictionAndObservation an RDD of (prediction, observation) pairs
    */
  def this(predictionAndObservation: RDD[(Double, Double)]) {
    this(predictionAndObservation.map({
      case (prediction, observation) => (prediction, observation, 1.0)
    }))
  }

  /**
   * An auxiliary constructor taking a DataFrame.
   * @param predictionAndObservations a DataFrame with two double columns:
   *                                  prediction and observation
   */
  private[mllib] def this(predictionAndObservations: DataFrame) =
    this(predictionAndObservations.map(r => (r.getDouble(0), r.getDouble(1), 1.0)))

  /**
   * Use MultivariateOnlineSummarizer to calculate summary statistics of observations and errors.
   */
  private lazy val summary: MultivariateStatisticalSummary = {
    val summary: MultivariateStatisticalSummary = predictionAndObservationsWithWeight.map {
      case (prediction, observation, weight) =>
        (Vectors.dense(observation, observation - prediction), weight)
    }.aggregate(new MultivariateOnlineSummarizer())(
        (summary, sample) => summary.add(sample._1, sample._2),
        (sum1, sum2) => sum1.merge(sum2)
      )
    summary
  }
  private lazy val SSerr = math.pow(summary.normL2(1), 2)
  private lazy val SStot = summary.variance(0) * (summary.count - 1)
  private lazy val SSreg = {
    val yMean = summary.mean(0)
    predictionAndObservationsWithWeight.map {
      case (prediction, _, _) => math.pow(prediction - yMean, 2)
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
   */
  @Since("1.2.0")
  def r2: Double = {
    1 - SSerr / SStot
  }
}
