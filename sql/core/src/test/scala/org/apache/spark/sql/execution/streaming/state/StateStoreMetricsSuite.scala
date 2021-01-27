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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.SparkFunSuite

class StateStoreMetricsSuite extends SparkFunSuite {

  test("StateStoreMetrics.combine should not override individual metrics") {
    val customMetric = StateStoreCustomSumMetric("metric1", "custom metric 1")
    val leftCustomMetrics: Map[StateStoreCustomMetric, Long] =
      Map(customMetric -> 10L)
    val leftMetrics = StateStoreMetrics(1, 10, leftCustomMetrics)

    val rightCustomMetrics: Map[StateStoreCustomMetric, Long] =
      Map(customMetric -> 20L)
    val rightMetrics = StateStoreMetrics(3, 20, rightCustomMetrics)

    val combinedMetrics = StateStoreMetrics.combine(Seq(leftMetrics, rightMetrics))
    assert(combinedMetrics.numKeys == 4)
    assert(combinedMetrics.memoryUsedBytes == 30)
    assert(combinedMetrics.customMetrics.size == 1)
    assert(combinedMetrics.customMetrics(customMetric) == 30L)
  }
}
