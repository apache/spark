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
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class RankingMetricsSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("Ranking metrics: map, ndcg") {
    val predictionAndLabels = sc.parallelize(
      Seq(
        (Array[Int](1, 6, 2, 7, 8, 3, 9, 10, 4, 5), Array[Int](1, 2, 3, 4, 5)),
        (Array[Int](4, 1, 5, 6, 2, 7, 3, 8, 9, 10), Array[Int](1, 2, 3)),
        (Array[Int](1, 2, 3, 4, 5), Array[Int]())
      ), 2)
    val eps: Double = 1E-5

    val metrics = new RankingMetrics(predictionAndLabels)
    val map = metrics.meanAveragePrecision

    assert(metrics.precisionAt(1) ~== 1.0/3 absTol eps)
    assert(metrics.precisionAt(2) ~== 1.0/3 absTol eps)
    assert(metrics.precisionAt(3) ~== 1.0/3 absTol eps)
    assert(metrics.precisionAt(4) ~== 0.75/3 absTol eps)
    assert(metrics.precisionAt(5) ~== 0.8/3 absTol eps)
    assert(metrics.precisionAt(10) ~== 0.8/3 absTol eps)
    assert(metrics.precisionAt(15) ~== 8.0/45 absTol eps)

    assert(map ~== 0.355026 absTol eps)

    assert(metrics.ndcgAt(3) ~== 1.0/3 absTol eps)
    assert(metrics.ndcgAt(5) ~== 0.328788 absTol eps)
    assert(metrics.ndcgAt(10) ~== 0.487913 absTol eps)
    assert(metrics.ndcgAt(15) ~== metrics.ndcgAt(10) absTol eps)

  }
}
