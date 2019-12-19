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

package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.api.python.PythonMetrics

private[spark] class PythonMetricsSource extends Source {

  override val metricRegistry = new MetricRegistry()
  override val sourceName = "PythonMetrics"

  // This instruments the time spent to write/send serialized data to Python workers.
  // Includes operations for MapPartition, PythonUDF and PandasUDF.
  // Time is measured in nanoseconds.
  metricRegistry.register(MetricRegistry.name("WriteTimeToWorkers"), new Gauge[Long] {
    override def getValue: Long = PythonMetrics.getToWorkerWriteTime
  })

  // This instruments the number of data batches sent to Python workers.
  // Includes operations for MapPartition, PythonUDF and PandasUDF.
  metricRegistry.register(MetricRegistry.name("NumBatchesToWorkers"), new Gauge[Long] {
    override def getValue: Long = PythonMetrics.getToWorkerBatchCount
  })

  // This instruments the number of bytes sent to Python workers.
  // Includes operations for MapPartition, PythonUDF and PandasUDF.
  metricRegistry.register(MetricRegistry.name("BytesSentToWorkers"), new Gauge[Long] {
    override def getValue: Long = PythonMetrics.getToWorkerBytesWritten
  })

  // This instruments the number of bytes received from to Python workers.
  // Includes operations for MapPartition, PythonUDF and PandasUDF.
  metricRegistry.register(MetricRegistry.name("BytesReceivedFromWorkers"), new Gauge[Long] {
    override def getValue: Long = PythonMetrics.getFromWorkerBytesRead
  })

  // This instruments the time spent reading/receiving data back from Python workers.
  // It includes read operations for MapPartition, PythonUDF and PandasUDF.
  // Time is measured in nanoseconds.
  metricRegistry.register(MetricRegistry.name("FetchResultsTimeFromWorkers"), new Gauge[Long] {
    override def getValue: Long = PythonMetrics.getFromWorkerReadTime
  })

  // This instruments the number of data batches received back from Python workers.
  // Includes  operations for MapPartition, PythonUDF and PandasUDF.
  metricRegistry.register(MetricRegistry.name("NumBatchesFromWorkers"), new Gauge[Long] {
    override def getValue: Long = PythonMetrics.getFromWorkerBatchCount
  })

  // This instruments the number of rows received back from Python workers,
  // for Pandas UDF operations.
  metricRegistry.register(MetricRegistry.name("PandasUDFReceivedNumRows"), new Gauge[Long] {
    override def getValue: Long = PythonMetrics.getPandasUDFReadRowCount
  })

  // This instruments the number of rows sent to Python workers,
  // for Pandas UDF operations.
  metricRegistry.register(MetricRegistry.name("PandasUDFSentNumRows"), new Gauge[Long] {
    override def getValue: Long = PythonMetrics.getPandasUDFWriteRowCount
  })

}
