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
        preds <- signif(predict(model), digits = 4)

        cat("predictions: ", preds, "\n")
        cat("explainedVariance =", mean((preds - mean(y))^2), "\n")
        cat("meanAbsoluteError =", mean(abs(preds - y)), "\n")
        cat("meanSquaredError  =", mean((preds - y)^2), "\n")
        cat("rmse =", sqrt(mean((preds - y)^2)), "\n")
        cat("r2 =", summary(model)$r.squared, "\n")

      Output of R code:
        predictions:  72.08 91.88 65.48 52.28 62.18 81.98 58.88 78.68 55.58
        explainedVariance = 157.3
        meanAbsoluteError = 3.735556
        meanSquaredError  = 17.53951
        rmse = 4.18802
        r2 = 0.8996822
     */
    val preds = List(72.08, 91.88, 65.48, 52.28, 62.18, 81.98, 58.88, 78.68, 55.58)
    val pairs: Seq[(Double, Double)] = preds.zip(obs)
    val predictionAndObservations = sc.parallelize(pairs, 2)
    val metrics = new RegressionMetrics(predictionAndObservations)
    assert(metrics.explainedVariance ~== 157.3 absTol eps,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 3.735556 absTol eps, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 17.53951 absTol eps, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 4.18802 absTol eps,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 0.8996822 absTol eps, "r2 score mismatch")
  }

  test("regression metrics for biased (no intercept term) predictor") {
    /* Verify results in R:
        y = c(77, 85, 62, 55, 63, 88, 57, 81, 51)
        x = c(16, 22, 14, 10, 13, 19, 12, 18, 11)
        df <- as.data.frame(cbind(x, y))
        model <- lm(y ~ 0 + x, data=df)
        preds <- signif(predict(model), digits = 4)

        cat("predictions: ", preds, "\n")
        cat("explainedVariance =", mean((preds - mean(y))^2), "\n")
        cat("meanAbsoluteError =", mean(abs(preds - y)), "\n")
        cat("meanSquaredError  =", mean((preds - y)^2), "\n")
        cat("rmse =", sqrt(mean((preds - y)^2)), "\n")
        cat("r2 =", summary(model)$r.squared, "\n")

      Output of R code:
        predictions:  72.12 99.17 63.11 45.08 58.6 85.65 54.09 81.14 49.58
        explainedVariance = 294.88167
        meanAbsoluteError = 4.588889
        meanSquaredError  = 39.95871
        rmse = 6.32129
        r2 = 0.991854
     */
    val preds = List(72.12, 99.17, 63.11, 45.08, 58.6, 85.65, 54.09, 81.14, 49.58)
    val pairs: Seq[(Double, Double)] = preds.zip(obs)
    val predictionAndObservations = sc.parallelize(pairs, 2)
    val metrics = new RegressionMetrics(predictionAndObservations, false)
    assert(metrics.explainedVariance ~== 294.88167 absTol eps,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 4.588889 absTol eps, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 39.95871 absTol eps, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 6.32129 absTol eps,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 0.991854 absTol eps, "r2 score mismatch")
  }

  test("regression metrics with complete fitting") {
    /* Verify results in R:
        y = c(77, 85, 62, 55, 63, 88, 57, 81, 51)
        preds <- y

        cat("predictions: ", preds, "\n")
        cat("explainedVariance =", mean((preds - mean(y))^2), "\n")
        cat("meanAbsoluteError =", mean(abs(preds - y)), "\n")
        cat("meanSquaredError  =", mean((preds - y)^2), "\n")
        cat("rmse =", sqrt(mean((preds - y)^2)), "\n")
        cat("r2 =", 1 - sum((preds - y)^2)/sum((y - mean(y))^2), "\n")

      Output of R code:
        predictions:  77 85 62 55 63 88 57 81 51
        explainedVariance = 174.83951
        meanAbsoluteError = 0
        meanSquaredError  = 0
        rmse = 0
        r2 = 1
     */
    val preds = obs
    val pairs: Seq[(Double, Double)] = preds.zip(obs)
    val predictionAndObservations = sc.parallelize(pairs, 2)
    val metrics = new RegressionMetrics(predictionAndObservations)
    assert(metrics.explainedVariance ~== 174.83951 absTol eps,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 0.0 absTol eps, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 0.0 absTol eps, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.0 absTol eps,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 1.0 absTol eps, "r2 score mismatch")
  }
}
