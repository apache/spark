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

package org.apache.spark.sql.graphframes.expressions

import scala.util.Random

import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

class KMinSamplingSuite extends SparkFunSuite with GraphFrameTestSparkContext {
  test("test kmin sampling") {
    val data = Seq(
      (1L, 2L, 1L),
      (1L, 3L, 2L),
      (1L, 4L, 3L),
      (1L, 5L, 2L),
      (2L, 1L, 1L),
      (2L, 4L, 2L),
      (3L, 1L, 1L),
      (4L, 2L, 2L))

    val toAgg = spark.createDataFrame(data).toDF("src", "dst", "weight")
    val encoder = KMinSampling.getEncoder(spark, LongType, Seq("dst", "weight"))
    val aggUDF = KMinSampling.fromSparkType(LongType, 3, encoder)

    val result =
      toAgg.groupBy("src").agg(aggUDF(col("dst"), col("weight")).alias("r"))

    val collectedResult = result.collect()
    val collectedMap = collectedResult.map(f => (f.getLong(0), f.getAs[Seq[Long]](1))).toMap

    assert(collectedMap.get(1L).get === Seq(2L, 3L, 5L))
    assert(collectedMap.get(2L).get === Seq(1L, 4L))
    assert(collectedMap.get(3L).get === Seq(1L))
    assert(collectedMap.get(4L).get === Seq(2L))
  }

  test("test kmin sampling many values") {
    val random = new Random(42L)
    val candidates = Array(1L, 2L, 3L, 4L, 5L, 6L)
    val data = (1L to 10L)
      .flatMap(id => (1 to 100).map(_ => (id, candidates(random.nextInt(5)), random.nextLong())))
      .toSeq

    val toAgg = spark.createDataFrame(data).toDF("src", "dst", "weight")
    val encoder = KMinSampling.getEncoder(spark, LongType, Seq("dst", "weight"))
    val aggUDF = KMinSampling.fromSparkType(LongType, 5, encoder)

    val result =
      toAgg.groupBy("src").agg(aggUDF(col("dst"), col("weight")).alias("r")).collect()

    val collectedMap = result.map(f => (f.getLong(0), f.getAs[Seq[Long]](1))).toMap
    // at least one should be full
    assert(collectedMap.map(f => f._2.size).max == 5)

    // all should be within the limit
    for (id <- (1L to 10L)) {
      assert(collectedMap.get(id).get.size <= 5)
    }
  }
}
