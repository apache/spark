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

package org.apache.spark.sql.execution.streaming

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.{Source => CodahaleSource}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.streaming.StreamingQueryProgress

/**
 * Serves metrics from a [[org.apache.spark.sql.streaming.StreamingQuery]] to
 * Codahale/DropWizard metrics
 */
class MetricsReporter(
    stream: StreamExecution,
    override val sourceName: String) extends CodahaleSource with Logging {

  override val metricRegistry: MetricRegistry = new MetricRegistry

  // Metric names should not have . in them, so that all the metrics of a query are identified
  // together in Ganglia as a single metric group
  registerGauge("inputRate-total", _.inputRowsPerSecond, 0.0)
  registerGauge("processingRate-total", _.processedRowsPerSecond, 0.0)
  registerGauge("latency", _.durationMs.getOrDefault("triggerExecution", 0L).longValue(), 0L)

  private val timestampFormat =
    DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
      .withZone(DateTimeUtils.getZoneId("UTC"))

  registerGauge("eventTime-watermark",
    progress => convertStringDateToMillis(progress.eventTime.get("watermark")), 0L)

  registerGauge("states-rowsTotal", _.stateOperators.map(_.numRowsTotal).sum, 0L)
  registerGauge("states-usedBytes", _.stateOperators.map(_.memoryUsedBytes).sum, 0L)

  private def convertStringDateToMillis(isoUtcDateStr: String) = {
    if (isoUtcDateStr != null) {
      val zonedDateTime = ZonedDateTime.parse(isoUtcDateStr, timestampFormat)
      zonedDateTime.toInstant.toEpochMilli
    } else {
      0L
    }
  }

  private def registerGauge[T](
      name: String,
      f: StreamingQueryProgress => T,
      default: T): Unit = {
    synchronized {
      metricRegistry.register(name, new Gauge[T] {
        override def getValue: T = Option(stream.lastProgress).map(f).getOrElse(default)
      })
    }
  }
}
