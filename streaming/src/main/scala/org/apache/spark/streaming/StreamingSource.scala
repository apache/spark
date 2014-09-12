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

package org.apache.spark.streaming

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source
import org.apache.spark.streaming.ui.StreamingJobProgressListener

private[streaming] class StreamingSource(ssc: StreamingContext) extends Source {
  override val metricRegistry = new MetricRegistry
  override val sourceName = "%s.StreamingMetrics".format(ssc.sparkContext.appName)

  private val streamingListener = ssc.uiTab.listener

  private def registerGauge[T](name: String, f: StreamingJobProgressListener => T,
      defaultValue: T) {
    metricRegistry.register(MetricRegistry.name("streaming", name), new Gauge[T] {
      override def getValue: T = Option(f(streamingListener)).getOrElse(defaultValue)
    })
  }

  // Gauge for number of network receivers
  registerGauge("receivers", _.numReceivers, 0)

  // Gauge for number of total completed batches
  registerGauge("totalCompletedBatches", _.numTotalCompletedBatches, 0L)

  // Gauge for number of unprocessed batches
  registerGauge("unprocessedBatches", _.numUnprocessedBatches, 0L)

  // Gauge for number of waiting batches
  registerGauge("waitingBatches", _.waitingBatches.size, 0L)

  // Gauge for number of running batches
  registerGauge("runningBatches", _.runningBatches.size, 0L)

  // Gauge for number of retained completed batches
  registerGauge("retainedCompletedBatches", _.retainedCompletedBatches.size, 0L)

  // Gauge for last completed batch, useful for monitoring the streaming job's running status,
  // displayed data -1 for any abnormal condition.
  registerGauge("lastCompletedBatch_submissionTime",
    _.lastCompletedBatch.map(_.submissionTime).getOrElse(-1L), -1L)
  registerGauge("lastCompletedBatch_processStartTime",
    _.lastCompletedBatch.flatMap(_.processingStartTime).getOrElse(-1L), -1L)
  registerGauge("lastCompletedBatch_processEndTime",
    _.lastCompletedBatch.flatMap(_.processingEndTime).getOrElse(-1L), -1L)

  // Gauge for last received batch, useful for monitoring the streaming job's running status,
  // displayed data -1 for any abnormal condition.
  registerGauge("lastReceivedBatch_submissionTime",
    _.lastCompletedBatch.map(_.submissionTime).getOrElse(-1L), -1L)
  registerGauge("lastReceivedBatch_processStartTime",
    _.lastCompletedBatch.flatMap(_.processingStartTime).getOrElse(-1L), -1L)
  registerGauge("lastReceivedBatch_processEndTime",
    _.lastCompletedBatch.flatMap(_.processingEndTime).getOrElse(-1L), -1L)
}
