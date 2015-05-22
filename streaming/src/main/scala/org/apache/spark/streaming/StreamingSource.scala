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

  private val streamingListener = ssc.progressListener

  private def registerGauge[T](name: String, f: StreamingJobProgressListener => T,
      defaultValue: T): Unit = {
    registerGaugeWithOption[T](name,
      (l: StreamingJobProgressListener) => Option(f(streamingListener)), defaultValue)
  }

  private def registerGaugeWithOption[T](
      name: String,
      f: StreamingJobProgressListener => Option[T],
      defaultValue: T): Unit = {
    metricRegistry.register(MetricRegistry.name("streaming", name), new Gauge[T] {
      override def getValue: T = f(streamingListener).getOrElse(defaultValue)
    })
  }

  // Gauge for number of network receivers
  registerGauge("receivers", _.numReceivers, 0)

  // Gauge for number of total completed batches
  registerGauge("totalCompletedBatches", _.numTotalCompletedBatches, 0L)

  // Gauge for number of total received records
  registerGauge("totalReceivedRecords", _.numTotalReceivedRecords, 0L)

  // Gauge for number of total processed records
  registerGauge("totalProcessedRecords", _.numTotalProcessedRecords, 0L)

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
  registerGaugeWithOption("lastCompletedBatch_submissionTime",
    _.lastCompletedBatch.map(_.submissionTime), -1L)
  registerGaugeWithOption("lastCompletedBatch_processingStartTime",
    _.lastCompletedBatch.flatMap(_.processingStartTime), -1L)
  registerGaugeWithOption("lastCompletedBatch_processingEndTime",
    _.lastCompletedBatch.flatMap(_.processingEndTime), -1L)

  // Gauge for last completed batch's delay information.
  registerGaugeWithOption("lastCompletedBatch_processingDelay",
    _.lastCompletedBatch.flatMap(_.processingDelay), -1L)
  registerGaugeWithOption("lastCompletedBatch_schedulingDelay",
    _.lastCompletedBatch.flatMap(_.schedulingDelay), -1L)
  registerGaugeWithOption("lastCompletedBatch_totalDelay",
    _.lastCompletedBatch.flatMap(_.totalDelay), -1L)

  // Gauge for last received batch, useful for monitoring the streaming job's running status,
  // displayed data -1 for any abnormal condition.
  registerGaugeWithOption("lastReceivedBatch_submissionTime",
    _.lastCompletedBatch.map(_.submissionTime), -1L)
  registerGaugeWithOption("lastReceivedBatch_processingStartTime",
    _.lastCompletedBatch.flatMap(_.processingStartTime), -1L)
  registerGaugeWithOption("lastReceivedBatch_processingEndTime",
    _.lastCompletedBatch.flatMap(_.processingEndTime), -1L)

  // Gauge for last received batch records.
  registerGauge("lastReceivedBatch_records", _.lastReceivedBatchRecords.values.sum, 0L)
}
