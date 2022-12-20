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

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}

object CustomMetrics {
  private[spark] val V2_CUSTOM = "v2Custom"

  private[spark] val NUM_ROWS_PER_UPDATE = 100

  /**
   * Given a class name, builds and returns a metric type for a V2 custom metric class
   * `CustomMetric`.
   */
  def buildV2CustomMetricTypeName(customMetric: CustomMetric): String = {
    s"${V2_CUSTOM}_${customMetric.getClass.getName}"
  }

  /**
   * Given a V2 custom metric type name, this method parses it and returns the corresponding
   * `CustomMetric` class name.
   */
  def parseV2CustomMetricType(metricType: String): Option[String] = {
    if (metricType.startsWith(s"${V2_CUSTOM}_")) {
      Some(metricType.drop(V2_CUSTOM.length + 1))
    } else {
      None
    }
  }

  /**
   * Updates given custom metrics. If `currentMetricsValues` has metric which does not exist
   * in `customMetrics` map, it is non-op.
   */
  def updateMetrics(
      currentMetricsValues: Seq[CustomTaskMetric],
      customMetrics: Map[String, SQLMetric]): Unit = {
    currentMetricsValues.foreach { metric =>
      customMetrics.get(metric.name()).map(_.set(metric.value()))
    }
  }
}
