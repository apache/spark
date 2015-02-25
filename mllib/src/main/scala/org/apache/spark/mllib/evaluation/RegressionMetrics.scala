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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, MultivariateOnlineSummarizer}

/**
 * :: Experimental ::
 * Evaluator for regression.
 *
 * @param predictionAndObservations an RDD of (prediction, observation) pairs.
 */
@Experimental
class RegressionMetrics(predictionAndObservations: RDD[(Double, Double)]) extends Logging {

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

  /**
   * Returns the explained variance regression score.
   * explainedVariance = 1 - variance(y - \hat{y}) / variance(y)
   * Reference: [[http://en.wikipedia.org/wiki/Explained_variation]]
   */
  def explainedVariance: Double = {
    1 - summary.variance(1) / summary.variance(0)
  }

  /**
   * Returns the mean absolute error, which is a risk function corresponding to the
   * expected value of the absolute error loss or l1-norm loss.
   */
  def meanAbsoluteError: Double = {
    summary.normL1(1) / summary.count
  }

  /**
   * Returns the mean squared error, which is a risk function corresponding to the
   * expected value of the squared error loss or quadratic loss.
   */
  def meanSquaredError: Double = {
    val rmse = summary.normL2(1) / math.sqrt(summary.count)
    rmse * rmse
  }

  /**
   * Returns the root mean squared error, which is defined as the square root of
   * the mean squared error.
   */
  def rootMeanSquaredError: Double = {
    summary.normL2(1) / math.sqrt(summary.count)
  }

  /**
   * Returns R^2^, the coefficient of determination.
   * Reference: [[http://en.wikipedia.org/wiki/Coefficient_of_determination]]
   */
  def r2: Double = {
    1 - math.pow(summary.normL2(1), 2) / (summary.variance(0) * (summary.count - 1))
  }
}
