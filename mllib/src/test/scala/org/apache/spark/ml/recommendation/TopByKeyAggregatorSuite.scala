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

package org.apache.spark.ml.recommendation

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Dataset


class TopByKeyAggregatorSuite extends SparkFunSuite with MLlibTestSparkContext {

  private def getTopK(k: Int): Dataset[(Int, Array[(Int, Float)])] = {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val topKAggregator = new TopByKeyAggregator[Int, Int, Float](k, Ordering.by(_._2))
    Seq(
      (0, 3, 54f),
      (0, 4, 44f),
      (0, 5, 42f),
      (0, 6, 28f),
      (1, 3, 39f),
      (2, 3, 51f),
      (2, 5, 45f),
      (2, 6, 18f)
    ).toDS().groupByKey(_._1).agg(topKAggregator.toColumn)
  }

  test("topByKey with k < #items") {
    val topK = getTopK(2)
    assert(topK.count() === 3)

    val expected = Map(
      0 -> Array((3, 54f), (4, 44f)),
      1 -> Array((3, 39f)),
      2 -> Array((3, 51f), (5, 45f))
    )
    checkTopK(topK, expected)
  }

  test("topByKey with k > #items") {
    val topK = getTopK(5)
    assert(topK.count() === 3)

    val expected = Map(
      0 -> Array((3, 54f), (4, 44f), (5, 42f), (6, 28f)),
      1 -> Array((3, 39f)),
      2 -> Array((3, 51f), (5, 45f), (6, 18f))
    )
    checkTopK(topK, expected)
  }

  private def checkTopK(
      topK: Dataset[(Int, Array[(Int, Float)])],
      expected: Map[Int, Array[(Int, Float)]]): Unit = {
    topK.collect().foreach { case (id, recs) => assert(recs === expected(id)) }
  }
}
