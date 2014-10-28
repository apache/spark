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
import org.apache.spark.mllib.rdd.RDDFunctions._

/**
 * :: Experimental ::
 * Evaluator for regression.
 *
 * @param valuesAndPreds an RDD of (value, pred) pairs.
 */
@Experimental
class RegressionMetrics(valuesAndPreds: RDD[(Double, Double)]) extends Logging {

  /**
   * Use MultivariateOnlineSummarizer to calculate mean and variance of different combination.
   * MultivariateOnlineSummarizer is a numerically stable algorithm to compute mean and variance 
   * in a online fashion.
   */
  private lazy val summarizer: MultivariateOnlineSummarizer = {
    val summarizer: MultivariateOnlineSummarizer = valuesAndPreds.map{
      case (value,pred) => Vectors.dense(
        Array(value, value - pred, math.abs(value - pred), math.pow(value - pred, 2.0))
      )
    }.treeAggregate(new MultivariateOnlineSummarizer())(
        (summary, v) => summary.add(v),
        (sum1,sum2) => sum1.merge(sum2)
      )
    summarizer
  }

  /**
   * Computes the explained variance regression score
   */
  def explainedVarianceScore(): Double = {
    1 - summarizer.variance(1) / summarizer.variance(0)
  }

  /**
   * Computes the mean absolute error, which is a risk function corresponding to the
   * expected value of the absolute error loss or l1-norm loss.
   */
  def mae(): Double = {
    summarizer.mean(2)
  }

  /**
   * Computes the mean square error, which is a risk function corresponding to the
   * expected value of the squared error loss or quadratic loss.
   */
  def mse(): Double = {
    summarizer.mean(3)
  }

  /**
   * Computes R^2^, the coefficient of determination.
   * @return
   */
  def r2_score(): Double = {
    1 - summarizer.mean(3) * summarizer.count / (summarizer.variance(0) * (summarizer.count - 1))
  }
}
