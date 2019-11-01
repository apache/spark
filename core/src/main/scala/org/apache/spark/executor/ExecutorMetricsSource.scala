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

package org.apache.spark.executor

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.metrics.source.Source

private[spark] class ExecutorMetricsSource extends Source {

  override val metricRegistry = new MetricRegistry()
  override val sourceName = "ExecutorMetrics"
  @volatile var metricsSnapshot: Array[Long] = Array.fill(ExecutorMetricType.numMetrics)(0L)

  // called by ExecutorMetricsPoller
  def updateMetricsSnapshot(metricsUpdates: Array[Long]): Unit = {
    metricsSnapshot = metricsUpdates
  }

  class ExecutorMetricGauge(idx: Int) extends Gauge[Long] {
    def getValue: Long = metricsSnapshot(idx)
  }

  def register: Unit = {
    // This looks like a bunch of independent gauges as far the metric system
    // is concerned, but actually they're all using one shared snapshot.
    val gauges: IndexedSeq[ExecutorMetricGauge] = (0 until ExecutorMetricType.numMetrics).map {
      idx => new ExecutorMetricGauge(idx)
    }.toIndexedSeq

    ExecutorMetricType.metricToOffset.foreach {
      case (name, idx) =>
        metricRegistry.register(MetricRegistry.name(name), gauges(idx))
    }
  }
}
