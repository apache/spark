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
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer

/**
 * :: Experimental ::
 * Evaluator for regression.
 *
 * @param predictionAndObservations an RDD of (prediction,observation) pairs.
 */
@Experimental
class RegressionMetrics(predictionAndObservations: RDD[(Double, Double)]) extends Logging {

  /**
   * Use MultivariateOnlineSummarizer to calculate mean and variance of different combination.
   * MultivariateOnlineSummarizer is a numerically stable algorithm to compute mean and variance 
   * in a online fashion.
   */
  private lazy val summarizer: MultivariateOnlineSummarizer = {
    val summarizer: MultivariateOnlineSummarizer = predictionAndObservations.map{
      case (prediction,observation) => Vectors.dense(
        Array(observation, observation - prediction)
      )
    }.aggregate(new MultivariateOnlineSummarizer())(
        (summary, v) => summary.add(v),
        (sum1,sum2) => sum1.merge(sum2)
      )
    summarizer
  }

  /**
   * Returns the explained variance regression score.
   * explainedVarianceScore = 1 - variance(y - \hat{y}) / variance(y)
   * Reference: [[http://en.wikipedia.org/wiki/Explained_variation]]
   */
  def explainedVarianceScore: Double = {
    1 - summarizer.variance(1) / summarizer.variance(0)
  }

  /**
   * Returns the mean absolute error, which is a risk function corresponding to the
   * expected value of the absolute error loss or l1-norm loss.
   */
  def meanAbsoluteError: Double = {
    summarizer.normL1(1) / summarizer.count
  }

  /**
   * Returns the mean squared error, which is a risk function corresponding to the
   * expected value of the squared error loss or quadratic loss.
   */
  def meanSquaredError: Double = {
    math.pow(summarizer.normL2(1),2) / summarizer.count
  }

  /**
   * Returns the root mean squared error, which is defined as the square root of
   * the mean squared error.
   */
  def rootMeanSquaredError: Double = {
    summarizer.normL2(1) / math.sqrt(summarizer.count)
  }

  /**
   * Returns R^2^, the coefficient of determination.
   * Reference: [[http://en.wikipedia.org/wiki/Coefficient_of_determination]]
   */
  def r2Score: Double = {
    1 - math.pow(summarizer.normL2(1),2) / (summarizer.variance(0) * (summarizer.count - 1))
  }
}
