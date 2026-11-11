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

package org.apache.spark.sql

import java.time.LocalTime

import org.apache.spark.sql.functions.{count_min_sketch, lit}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.sketch.CountMinSketch

/**
 * End-to-end test suite for count_min_sketch.
 */
class CountMinSketchAggQuerySuite extends SharedSparkSession {

  test("count-min sketch") {
    import testImplicits._

    val eps = 0.1
    val confidence = 0.95
    val seed = 11

    val items = Seq(1, 1, 2, 2, 2, 2, 3, 4, 5)
    val sketch = CountMinSketch.readFrom(items.toDF("id")
      .selectExpr(s"count_min_sketch(id, ${eps}d, ${confidence}d, $seed)")
      .head().get(0).asInstanceOf[Array[Byte]])

    val reference = CountMinSketch.create(eps, confidence, seed)
    items.foreach(reference.add)

    assert(sketch == reference)
  }

  test("function count_min_sketch") {
    import testImplicits._

    val eps = 0.1
    val confidence = 0.95
    val seed = 11

    val items = Seq(1, 1, 2, 2, 2, 2, 3, 4, 5)
    val sketch = CountMinSketch.readFrom(items.toDF("id")
      .select(count_min_sketch($"id", lit(eps), lit(confidence), lit(seed)))
      .head().get(0).asInstanceOf[Array[Byte]])

    val reference = CountMinSketch.create(eps, confidence, seed)
    items.foreach(reference.add)

    assert(sketch == reference)
  }

  test("count-min sketch over the TIME type") {
    val eps = 0.1
    val confidence = 0.95
    val seed = 11

    val sketch = CountMinSketch.readFrom(
      spark.sql(
        s"SELECT count_min_sketch(t, ${eps}d, ${confidence}d, $seed) FROM VALUES " +
          "(TIME'12:00:00'), (TIME'12:00:00'), (TIME'09:00:00'), (TIME'17:00:00') AS tab(t)")
        .head().get(0).asInstanceOf[Array[Byte]])

    // A TIME column sketches by its nanos-of-day, identical to a BIGINT of the same values.
    val reference = CountMinSketch.create(eps, confidence, seed)
    Seq(LocalTime.of(12, 0, 0), LocalTime.of(12, 0, 0), LocalTime.of(9, 0, 0),
      LocalTime.of(17, 0, 0)).foreach(t => reference.add(t.toNanoOfDay))

    assert(sketch == reference)
  }

  test("count_min_sketch TIME frequency is looked up by nanoseconds-of-day") {
    val eps = 0.1
    val confidence = 0.95
    val seed = 11

    val sketch = CountMinSketch.readFrom(
      spark.sql(
        s"SELECT count_min_sketch(t, ${eps}d, ${confidence}d, $seed) FROM VALUES " +
          "(TIME'12:00:00'), (TIME'12:00:00'), (TIME'12:00:00'), (TIME'09:00:00') AS tab(t)")
        .head().get(0).asInstanceOf[Array[Byte]])

    // A TIME value is looked up by its nanoseconds-of-day (LocalTime.toNanoOfDay), not by a
    // LocalTime -- the underlying sketch key is the same long the aggregate stored.
    assert(sketch.estimateCount(LocalTime.of(12, 0, 0).toNanoOfDay) == 3L)
    assert(sketch.estimateCount(LocalTime.of(9, 0, 0).toNanoOfDay) == 1L)
    // Passing the LocalTime itself is not a valid lookup key.
    intercept[IllegalArgumentException](sketch.estimateCount(LocalTime.of(12, 0, 0)))
  }
}
