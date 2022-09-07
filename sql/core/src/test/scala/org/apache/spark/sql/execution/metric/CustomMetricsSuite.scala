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

package org.apache.spark.sql.execution.metric

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.metric.{CustomAvgMetric, CustomSumMetric, CustomTaskMetric}

class CustomMetricsSuite extends SparkFunSuite {

  test("Build/parse custom metric metric type") {
    Seq(new TestCustomSumMetric, new TestCustomAvgMetric).foreach { customMetric =>
      val metricType = CustomMetrics.buildV2CustomMetricTypeName(customMetric)

      assert(metricType == CustomMetrics.V2_CUSTOM + "_" + customMetric.getClass.getCanonicalName)
      assert(CustomMetrics.parseV2CustomMetricType(metricType).isDefined)
      assert(CustomMetrics.parseV2CustomMetricType(metricType).get ==
        customMetric.getClass.getCanonicalName)
    }
  }

  test("Built-in CustomSumMetric") {
    val metric = new TestCustomSumMetric

    val metricValues1 = Array(0L, 1L, 5L, 5L, 7L, 10L)
    assert(metric.aggregateTaskMetrics(metricValues1) == metricValues1.sum.toString)

    val metricValues2 = Array.empty[Long]
    assert(metric.aggregateTaskMetrics(metricValues2) == "0")
  }

  test("Built-in CustomAvgMetric") {
    val metric = new TestCustomAvgMetric

    val metricValues1 = Array(0L, 1L, 5L, 5L, 7L, 10L)
    assert(metric.aggregateTaskMetrics(metricValues1) == "4.667")

    val metricValues2 = Array.empty[Long]
    assert(metric.aggregateTaskMetrics(metricValues2) == "0")
  }

  test("Report unsupported metrics should be non-op") {
    val taskMetric = new CustomTaskMetric {
      override def name(): String = "custom_metric"
      override def value(): Long = 1L
    }
    CustomMetrics.updateMetrics(Seq(taskMetric), Map.empty)
  }
}

private[spark] class TestCustomSumMetric extends CustomSumMetric {
  override def name(): String = "CustomSumMetric"
  override def description(): String = "Sum up CustomMetric"
}

private[spark] class TestCustomAvgMetric extends CustomAvgMetric {
  override def name(): String = "CustomAvgMetric"
  override def description(): String = "Average CustomMetric"
}
