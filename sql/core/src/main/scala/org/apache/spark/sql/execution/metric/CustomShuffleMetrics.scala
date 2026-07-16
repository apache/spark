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

import org.apache.spark.SparkContext
import org.apache.spark.shuffle.api.metric.{CustomShuffleMetric, CustomShuffleTaskMetric}
import org.apache.spark.util.MetricUtils

object CustomShuffleMetrics {

  /**
   * Creates [[SQLMetric]]s for the given declared custom shuffle metrics, keyed by metric name.
   */
  def createMetrics(
      sc: SparkContext,
      metrics: Array[CustomShuffleMetric]): Map[String, SQLMetric] = {
    metrics.map { metric =>
      val label = metric.description()
      // AVERAGE_METRIC is intentionally unsupported: its per-task values must be stored pre-scaled
      // via SQLMetric.set(Double), whereas custom task metrics report a plain Long.
      val acc = metric.metricType() match {
        case MetricUtils.SUM_METRIC => SQLMetrics.createMetric(sc, label)
        case MetricUtils.SIZE_METRIC => SQLMetrics.createSizeMetric(sc, label)
        case MetricUtils.TIMING_METRIC => SQLMetrics.createTimingMetric(sc, label)
        case MetricUtils.NS_TIMING_METRIC => SQLMetrics.createNanoTimingMetric(sc, label)
        case other => throw new IllegalArgumentException(
          s"Unsupported custom shuffle metric type '$other' for metric '${metric.name()}'. " +
          s"Supported types: ${MetricUtils.SUM_METRIC}, ${MetricUtils.SIZE_METRIC}, " +
          s"${MetricUtils.TIMING_METRIC}, ${MetricUtils.NS_TIMING_METRIC}.")
      }
      metric.name() -> acc
    }.toMap
  }

  /**
   * Updates the custom-metric [[SQLMetric]]s with the per-task reported values, matching by name.
   * Reported values with no matching declaration are ignored.
   */
  def updateMetrics(
      taskMetricsValues: Array[CustomShuffleTaskMetric],
      customMetrics: Map[String, SQLMetric]): Unit = {
    taskMetricsValues.foreach { metric =>
      customMetrics.get(metric.name()).foreach(_.set(metric.value()))
    }
  }
}
