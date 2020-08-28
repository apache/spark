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

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.sketch.CountMinSketch

/**
 * End-to-end test suite for count_min_sketch.
 */
class CountMinSketchAggQuerySuite extends QueryTest with SharedSparkSession {

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
}
