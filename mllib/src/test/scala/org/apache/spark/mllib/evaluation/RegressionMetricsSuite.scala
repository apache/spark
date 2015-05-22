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

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class RegressionMetricsSuite extends FunSuite with MLlibTestSparkContext {

  test("regression metrics") {
    val predictionAndObservations = sc.parallelize(
      Seq((2.5,3.0),(0.0,-0.5),(2.0,2.0),(8.0,7.0)), 2)
    val metrics = new RegressionMetrics(predictionAndObservations)
    assert(metrics.explainedVariance ~== 0.95717 absTol 1E-5,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 0.5 absTol 1E-5, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 0.375 absTol 1E-5, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.61237 absTol 1E-5,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 0.94861 absTol 1E-5, "r2 score mismatch")
  }

  test("regression metrics with complete fitting") {
    val predictionAndObservations = sc.parallelize(
      Seq((3.0,3.0),(0.0,0.0),(2.0,2.0),(8.0,8.0)), 2)
    val metrics = new RegressionMetrics(predictionAndObservations)
    assert(metrics.explainedVariance ~== 1.0 absTol 1E-5,
      "explained variance regression score mismatch")
    assert(metrics.meanAbsoluteError ~== 0.0 absTol 1E-5, "mean absolute error mismatch")
    assert(metrics.meanSquaredError ~== 0.0 absTol 1E-5, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.0 absTol 1E-5,
      "root mean squared error mismatch")
    assert(metrics.r2 ~== 1.0 absTol 1E-5, "r2 score mismatch")
  }
}
