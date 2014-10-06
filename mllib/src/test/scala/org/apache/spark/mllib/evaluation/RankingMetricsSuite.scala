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
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.util.LocalSparkContext

class RankingMetricsSuite extends FunSuite with LocalSparkContext {
  test("Ranking metrics: map, ndcg") {
    val predictionAndLabels = sc.parallelize(
      Seq(
        (Array[Double](1, 6, 2, 7, 8, 3, 9, 10, 4, 5), Array[Double](1, 2, 3, 4, 5)),
        (Array[Double](4, 1, 5, 6, 2, 7, 3, 8, 9, 10), Array[Double](1, 2, 3))
      ), 2)
    val eps: Double = 1E-5

    val metrics = new RankingMetrics(predictionAndLabels)
    val precAtK = metrics.precAtK.collect()
    val avePrec = metrics.avePrec.collect()
    val map = metrics.meanAvePrec
    val ndcg = metrics.ndcg.collect()
    val aveNdcg = metrics.meanNdcg

    assert(precAtK(0)(4) ~== 0.4 absTol eps)
    assert(precAtK(1)(6) ~== 3.0/7 absTol eps)
    assert(avePrec(0) ~== 0.622222 absTol eps)
    assert(avePrec(1) ~== 0.442857 absTol eps)
    assert(map ~== 0.532539 absTol eps)
    assert(ndcg(0) ~== 0.508740 absTol eps)
    assert(ndcg(1) ~== 0.296082 absTol eps)
    assert(aveNdcg ~== 0.402411 absTol eps)
  }
}
