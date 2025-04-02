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

class RankingMetricsSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("Ranking metrics: MAP, NDCG, Recall") {
    val predictionAndLabels = sc.parallelize(
      Seq(
        (Array(1, 6, 2, 7, 8, 3, 9, 10, 4, 5), Array(1, 2, 3, 4, 5)),
        (Array(4, 1, 5, 6, 2, 7, 3, 8, 9, 10), Array(1, 2, 3)),
        (Array(1, 2, 3, 4, 5), Array.empty[Int])),
      2)
    val eps = 1.0e-5

    val metrics = new RankingMetrics(predictionAndLabels)
    val map = metrics.meanAveragePrecision

    assert(metrics.precisionAt(1) ~== 1.0 / 3 absTol eps)
    assert(metrics.precisionAt(2) ~== 1.0 / 3 absTol eps)
    assert(metrics.precisionAt(3) ~== 1.0 / 3 absTol eps)
    assert(metrics.precisionAt(4) ~== 0.75 / 3 absTol eps)
    assert(metrics.precisionAt(5) ~== 0.8 / 3 absTol eps)
    assert(metrics.precisionAt(10) ~== 0.8 / 3 absTol eps)
    assert(metrics.precisionAt(15) ~== 8.0 / 45 absTol eps)

    assert(map ~== 0.355026 absTol eps)

    assert(metrics.meanAveragePrecisionAt(1) ~== 0.333334 absTol eps)
    assert(metrics.meanAveragePrecisionAt(2) ~== 0.25 absTol eps)
    assert(metrics.meanAveragePrecisionAt(3) ~== 0.24074 absTol eps)

    assert(metrics.ndcgAt(3) ~== 1.0 / 3 absTol eps)
    assert(metrics.ndcgAt(5) ~== 0.328788 absTol eps)
    assert(metrics.ndcgAt(10) ~== 0.487913 absTol eps)
    assert(metrics.ndcgAt(15) ~== metrics.ndcgAt(10) absTol eps)

    assert(metrics.recallAt(1) ~== 1.0 / 15 absTol eps)
    assert(metrics.recallAt(2) ~== 8.0 / 45 absTol eps)
    assert(metrics.recallAt(3) ~== 11.0 / 45 absTol eps)
    assert(metrics.recallAt(4) ~== 11.0 / 45 absTol eps)
    assert(metrics.recallAt(5) ~== 16.0 / 45 absTol eps)
    assert(metrics.recallAt(10) ~== 2.0 / 3 absTol eps)
    assert(metrics.recallAt(15) ~== 2.0 / 3 absTol eps)
  }

  test("Ranking metrics: NDCG with relevance") {
    val predictionAndLabels = sc.parallelize(
      Seq(
        (
          Array(1, 6, 2, 7, 8, 3, 9, 10, 4, 5),
          Array(1, 2, 3, 4, 5),
          Array(3.0, 2.0, 1.0, 1.0, 1.0)),
        (Array(4, 1, 5, 6, 2, 7, 3, 8, 9, 10), Array(1, 2, 3), Array(2.0, 0.0, 0.0)),
        (Array(1, 2, 3, 4, 5), Array.empty[Int], Array.empty[Double])),
      2)
    val eps = 1.0e-5

    val metrics = new RankingMetrics(predictionAndLabels)
    val map = metrics.meanAveragePrecision

    assert(metrics.precisionAt(1) ~== 1.0 / 3 absTol eps)
    assert(metrics.precisionAt(2) ~== 1.0 / 3 absTol eps)
    assert(metrics.precisionAt(3) ~== 1.0 / 3 absTol eps)
    assert(metrics.precisionAt(4) ~== 0.75 / 3 absTol eps)
    assert(metrics.precisionAt(5) ~== 0.8 / 3 absTol eps)
    assert(metrics.precisionAt(10) ~== 0.8 / 3 absTol eps)
    assert(metrics.precisionAt(15) ~== 8.0 / 45 absTol eps)

    assert(map ~== 0.355026 absTol eps)

    assert(metrics.meanAveragePrecisionAt(1) ~== 0.333334 absTol eps)
    assert(metrics.meanAveragePrecisionAt(2) ~== 0.25 absTol eps)
    assert(metrics.meanAveragePrecisionAt(3) ~== 0.24074 absTol eps)

    assert(metrics.ndcgAt(3) ~== 0.511959 absTol eps)
    assert(metrics.ndcgAt(5) ~== 0.487806 absTol eps)
    assert(metrics.ndcgAt(10) ~== 0.518700 absTol eps)
    assert(metrics.ndcgAt(15) ~== metrics.ndcgAt(10) absTol eps)

    assert(metrics.recallAt(1) ~== 1.0 / 15 absTol eps)
    assert(metrics.recallAt(2) ~== 8.0 / 45 absTol eps)
    assert(metrics.recallAt(3) ~== 11.0 / 45 absTol eps)
    assert(metrics.recallAt(4) ~== 11.0 / 45 absTol eps)
    assert(metrics.recallAt(5) ~== 16.0 / 45 absTol eps)
    assert(metrics.recallAt(10) ~== 2.0 / 3 absTol eps)
    assert(metrics.recallAt(15) ~== 2.0 / 3 absTol eps)
  }

  test("MAP, NDCG, Recall with few predictions (SPARK-14886)") {
    val predictionAndLabels = sc.parallelize(
      Seq((Array(1, 6, 2), Array(1, 2, 3, 4, 5)), (Array.empty[Int], Array(1, 2, 3))),
      2)
    val eps = 1.0e-5

    val metrics = new RankingMetrics(predictionAndLabels)
    assert(metrics.precisionAt(1) ~== 0.5 absTol eps)
    assert(metrics.precisionAt(2) ~== 0.25 absTol eps)
    assert(metrics.ndcgAt(1) ~== 0.5 absTol eps)
    assert(metrics.ndcgAt(2) ~== 0.30657 absTol eps)
    assert(metrics.recallAt(1) ~== 0.1 absTol eps)
    assert(metrics.recallAt(2) ~== 0.1 absTol eps)
  }

}
