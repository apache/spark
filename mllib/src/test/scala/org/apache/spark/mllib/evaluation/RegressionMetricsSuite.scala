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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class RegressionMetricsSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("regression metrics for unbiased (includes intercept term) predictor") {
    /* Verify results in R:
       preds = c(2.25, -0.25, 1.75, 7.75)
       obs = c(3.0, -0.5, 2.0, 7.0)

       SStot = sum((obs - mean(obs))^2)
       SSreg = sum((preds - mean(obs))^2)
       SSerr = sum((obs - preds)^2)

       explainedVariance = SSreg / length(obs)
       explainedVariance
       > [1] 8.796875
       meanAbsoluteError = mean(abs(preds - obs))
       meanAbsoluteError
       > [1] 0.5
       meanSquaredError = mean((preds - obs)^2)
       meanSquaredError
       > [1] 0.3125
       rmse = sqrt(meanSquaredError)
       rmse
       > [1] 0.559017
       r2 = 1 - SSerr / SStot
       r2
       > [1] 0.9571734
     */
    val predictionAndObservations = sc.parallelize(
      Seq((2.25, 3.0), (-0.25, -0.5), (1.75, 2.0), (7.75, 7.0)), 2)
    val metrics = new RegressionMetrics(predictionAndObservations)
    assert(metrics.explainedVariance ~== 8.79687 absTol 1E-5,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 0.5 absTol 1E-5, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 0.3125 absTol 1E-5, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.55901 absTol 1E-5,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 0.95717 absTol 1E-5, "r2 score mismatch")
  }

  test("regression metrics for biased (no intercept term) predictor") {
    /* Verify results in R:
       preds = c(2.5, 0.0, 2.0, 8.0)
       obs = c(3.0, -0.5, 2.0, 7.0)

       SStot = sum((obs - mean(obs))^2)
       SSreg = sum((preds - mean(obs))^2)
       SSerr = sum((obs - preds)^2)

       explainedVariance = SSreg / length(obs)
       explainedVariance
       > [1] 8.859375
       meanAbsoluteError = mean(abs(preds - obs))
       meanAbsoluteError
       > [1] 0.5
       meanSquaredError = mean((preds - obs)^2)
       meanSquaredError
       > [1] 0.375
       rmse = sqrt(meanSquaredError)
       rmse
       > [1] 0.6123724
       r2 = 1 - SSerr / SStot
       r2
       > [1] 0.9486081
     */
    val predictionAndObservations = sc.parallelize(
      Seq((2.5, 3.0), (0.0, -0.5), (2.0, 2.0), (8.0, 7.0)), 2)
    val metrics = new RegressionMetrics(predictionAndObservations)
    assert(metrics.explainedVariance ~== 8.85937 absTol 1E-5,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 0.5 absTol 1E-5, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 0.375 absTol 1E-5, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.61237 absTol 1E-5,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 0.94860 absTol 1E-5, "r2 score mismatch")
  }

  test("regression metrics with complete fitting") {
    val predictionAndObservations = sc.parallelize(
      Seq((3.0, 3.0), (0.0, 0.0), (2.0, 2.0), (8.0, 8.0)), 2)
    val metrics = new RegressionMetrics(predictionAndObservations)
    assert(metrics.explainedVariance ~== 8.6875 absTol 1E-5,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 0.0 absTol 1E-5, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 0.0 absTol 1E-5, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.0 absTol 1E-5,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 1.0 absTol 1E-5, "r2 score mismatch")
  }

  test("regression metrics with same(1.0) weight samples") {
    val predictionAndObservationWithWeight = sc.parallelize(
      Seq((2.25, 3.0, 1.0), (-0.25, -0.5, 1.0), (1.75, 2.0, 1.0), (7.75, 7.0, 1.0)), 2)
    val metrics = new RegressionMetrics(predictionAndObservationWithWeight)
    assert(metrics.explainedVariance ~== 8.79687 absTol 1E-5,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 0.5 absTol 1E-5, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 0.3125 absTol 1E-5, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.55901 absTol 1E-5,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 0.95717 absTol 1E-5, "r2 score mismatch")
  }


  /**
    * The following values are hand calculated using the formula:
    * [[https://en.wikipedia.org/wiki/Weighted_arithmetic_mean#Reliability_weights]]
    * preds = c(2.25, -0.25, 1.75, 7.75)
    * obs = c(3.0, -0.5, 2.0, 7.0)
    * weights = c(0.1, 0.2, 0.15, 0.05)
    * count = 4
    *
    * Weighted metrics can be calculated with MultivariateStatisticalSummary.
    *             (observations, observations - predictions)
    * mean        (1.7, 0.05)
    * variance    (7.3, 0.3)
    * numNonZeros (0.5, 0.5)
    * max         (7.0, 0.75)
    * min         (-0.5, -0.75)
    * normL2      (2.0, 0.32596)
    * normL1      (1.05, 0.2)
    *
    * explainedVariance: sum((preds - 1.7)^2) / count = 10.1775
    * meanAbsoluteError: normL1(1) / count = 0.05
    * meanSquaredError: normL2(1)^2 / count = 0.02656
    * rootMeanSquaredError: sqrt(meanSquaredError) = 0.16298
    * r2: 1 - normL2(1)^2 / (variance(0) * (count - 1)) = 0.9951484
    */
  test("regression metrics with weighted samples") {
    val predictionAndObservationWithWeight = sc.parallelize(
      Seq((2.25, 3.0, 0.1), (-0.25, -0.5, 0.2), (1.75, 2.0, 0.15), (7.75, 7.0, 0.05)), 2)
    val metrics = new RegressionMetrics(predictionAndObservationWithWeight)
    assert(metrics.explainedVariance ~== 10.1775 absTol 1E-5,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 0.05 absTol 1E-5, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 0.02656248 absTol 1E-5, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.16298 absTol 1E-5,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 0.9951484 absTol 1E-5, "r2 score mismatch")
  }
}
