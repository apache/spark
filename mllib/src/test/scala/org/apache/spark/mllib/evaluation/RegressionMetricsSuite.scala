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
  val obs = List[Double](77, 85, 62, 55, 63, 88, 57, 81, 51)
  val eps = 1E-5

  test("regression metrics for unbiased (includes intercept term) predictor") {
    /* Verify results in R:
        y = c(77, 85, 62, 55, 63, 88, 57, 81, 51)
        x = c(16, 22, 14, 10, 13, 19, 12, 18, 11)
        df <- as.data.frame(cbind(x, y))
        model <- lm(y ~  x, data=df)
        preds = signif(predict(model), digits = 4)
        preds
            1     2     3     4     5     6     7     8     9
        72.08 91.88 65.48 52.28 62.18 81.98 58.88 78.68 55.58
        options(digits=8)
        explainedVariance = mean((preds - mean(y))^2)
        [1] 157.3
        meanAbsoluteError = mean(abs(preds - y))
        meanAbsoluteError
        [1] 3.7355556
        meanSquaredError = mean((preds - y)^2)
        meanSquaredError
        [1] 17.539511
        rmse = sqrt(meanSquaredError)
        rmse
        [1] 4.18802
        r2 = summary(model)$r.squared
        r2
        [1] 0.89968225
     */
    val preds = List(72.08, 91.88, 65.48, 52.28, 62.18, 81.98, 58.88, 78.68, 55.58)
    val predictionAndObservations = sc.parallelize(preds.zip(obs), 2)
    val metrics = new RegressionMetrics(predictionAndObservations)
    assert(metrics.explainedVariance ~== 157.3 absTol eps,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 3.7355556 absTol eps, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 17.539511 absTol eps, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 4.18802 absTol eps,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 0.89968225 absTol eps, "r2 score mismatch")
  }

  test("regression metrics for biased (no intercept term) predictor") {
    /* Verify results in R:
        y = c(77, 85, 62, 55, 63, 88, 57, 81, 51)
        x = c(16, 22, 14, 10, 13, 19, 12, 18, 11)
        df <- as.data.frame(cbind(x, y))
        model <- lm(y ~ 0 + x, data=df)
        preds = signif(predict(model), digits = 4)
        preds
            1     2     3     4     5     6     7     8     9
        72.12 99.17 63.11 45.08 58.60 85.65 54.09 81.14 49.58
        options(digits=8)
        explainedVariance = mean((preds - mean(y))^2)
        explainedVariance
        [1] 294.88167
        meanAbsoluteError = mean(abs(preds - y))
        meanAbsoluteError
        [1] 4.5888889
        meanSquaredError = mean((preds - y)^2)
        meanSquaredError
        [1] 39.958711
        rmse = sqrt(meanSquaredError)
        rmse
        [1] 6.3212903
        r2 = summary(model)$r.squared
        r2
        [1] 0.99185395
     */
    val preds = List(72.12, 99.17, 63.11, 45.08, 58.6, 85.65, 54.09, 81.14, 49.58)
    val predictionAndObservations = sc.parallelize(preds.zip(obs), 2)
    val metrics = new RegressionMetrics(predictionAndObservations, true)
    assert(metrics.explainedVariance ~== 294.88167 absTol eps,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 4.5888889 absTol eps, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 39.958711 absTol eps, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 6.3212903 absTol eps,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 0.99185395 absTol eps, "r2 score mismatch")
  }

  test("regression metrics with complete fitting") {
    /* Verify results in R:
        y = c(77, 85, 62, 55, 63, 88, 57, 81, 51)
        preds = y
        explainedVariance = mean((preds - mean(y))^2)
        explainedVariance
        [1] 174.8395
        meanAbsoluteError = mean(abs(preds - y))
        meanAbsoluteError
        [1] 0
        meanSquaredError = mean((preds - y)^2)
        meanSquaredError
        [1] 0
        rmse = sqrt(meanSquaredError)
        rmse
        [1] 0
        r2 = 1 - sum((preds - y)^2)/sum((y - mean(y))^2)
        r2
        [1] 1
     */
    val preds = obs
    val predictionAndObservations = sc.parallelize(preds.zip(obs), 2)
    val metrics = new RegressionMetrics(predictionAndObservations)
    assert(metrics.explainedVariance ~== 174.83951 absTol eps,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 0.0 absTol eps, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 0.0 absTol eps, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.0 absTol eps,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 1.0 absTol eps, "r2 score mismatch")
  }

  test("regression metrics with same (1.0) weight samples") {
    val predictionAndObservationWithWeight = sc.parallelize(
      Seq((2.25, 3.0, 1.0), (-0.25, -0.5, 1.0), (1.75, 2.0, 1.0), (7.75, 7.0, 1.0)), 2)
    val metrics = new RegressionMetrics(predictionAndObservationWithWeight, false)
    assert(metrics.explainedVariance ~== 8.79687 absTol eps,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 0.5 absTol eps, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 0.3125 absTol eps, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.55901 absTol eps,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 0.95717 absTol eps, "r2 score mismatch")
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
   * explainedVariance: sum(pow((preds - 1.7),2)*weight) / weightedCount = 5.2425
   * meanAbsoluteError: normL1(1) / weightedCount = 0.4
   * meanSquaredError: pow(normL2(1),2) / weightedCount = 0.2125
   * rootMeanSquaredError: sqrt(meanSquaredError) = 0.46098
   * r2: 1 - pow(normL2(1),2) / (variance(0) * (weightedCount - 1)) = 1.02910
   */
  test("regression metrics with weighted samples") {
    val predictionAndObservationWithWeight = sc.parallelize(
      Seq((2.25, 3.0, 0.1), (-0.25, -0.5, 0.2), (1.75, 2.0, 0.15), (7.75, 7.0, 0.05)), 2)
    val metrics = new RegressionMetrics(predictionAndObservationWithWeight, false)
    assert(metrics.explainedVariance ~== 5.2425 absTol eps,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 0.4 absTol eps, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 0.2125 absTol eps, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.46098 absTol eps,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 1.02910 absTol eps, "r2 score mismatch")
  }
}
