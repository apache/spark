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

package org.apache.spark.util

import org.apache.spark.SparkFunSuite

class MetricUtilsSuite extends SparkFunSuite {

  test("stringValue with unknown metric type falls back gracefully") {
    val values = Array(100L, 200L, 300L)
    val maxMetrics = Array(300L, 2L, 0L, 5L) // value, stageId, attemptId, taskId
    val result = MetricUtils.stringValue("customType", values, maxMetrics)
    assert(result.contains("[unknown]"))
    assert(result.contains("(stage 2.0: task 5)"))
  }

  test("stringValue with unknown metric type and single value") {
    val values = Array(42L)
    val result = MetricUtils.stringValue("customType", values, Array.empty[Long])
    assert(result == "[unknown] 42")
  }

  test("stringValue with known metric types still works") {
    // sum
    val sumResult = MetricUtils.stringValue("sum", Array(100L, 200L), Array.empty[Long])
    assert(sumResult == "300")

    // size
    val sizeResult = MetricUtils.stringValue("size", Array(1024L), Array.empty[Long])
    assert(sizeResult == "1024.0 B")

    // timing
    val timingResult = MetricUtils.stringValue("timing", Array(500L), Array.empty[Long])
    assert(timingResult == "500 ms")
  }
}
