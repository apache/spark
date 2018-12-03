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
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter

/**
 * A shuffle read metrics reporter for SQL exchange operators.
 * @param tempMetrics [[TempShuffleReadMetrics]] created in TaskContext.
 * @param metrics All metrics in current SparkPlan. This param should not empty and
 *   contains all shuffle metrics defined in createShuffleReadMetrics.
 */
private[spark] class SQLShuffleReadMetricsReporter(
    tempMetrics: TempShuffleReadMetrics,
    metrics: Map[String, SQLMetric]) extends TempShuffleReadMetrics {
  private[this] val _remoteBlocksFetched =
    metrics(SQLShuffleReadMetricsReporter.REMOTE_BLOCKS_FETCHED)
  private[this] val _localBlocksFetched =
    metrics(SQLShuffleReadMetricsReporter.LOCAL_BLOCKS_FETCHED)
  private[this] val _remoteBytesRead =
    metrics(SQLShuffleReadMetricsReporter.REMOTE_BYTES_READ)
  private[this] val _remoteBytesReadToDisk =
    metrics(SQLShuffleReadMetricsReporter.REMOTE_BYTES_READ_TO_DISK)
  private[this] val _localBytesRead =
    metrics(SQLShuffleReadMetricsReporter.LOCAL_BYTES_READ)
  private[this] val _fetchWaitTime =
    metrics(SQLShuffleReadMetricsReporter.FETCH_WAIT_TIME)
  private[this] val _recordsRead =
    metrics(SQLShuffleReadMetricsReporter.RECORDS_READ)

  override def incRemoteBlocksFetched(v: Long): Unit = {
    _remoteBlocksFetched.add(v)
    tempMetrics.incRemoteBlocksFetched(v)
  }
  override def incLocalBlocksFetched(v: Long): Unit = {
    _localBlocksFetched.add(v)
    tempMetrics.incLocalBlocksFetched(v)
  }
  override def incRemoteBytesRead(v: Long): Unit = {
    _remoteBytesRead.add(v)
    tempMetrics.incRemoteBytesRead(v)
  }
  override def incRemoteBytesReadToDisk(v: Long): Unit = {
    _remoteBytesReadToDisk.add(v)
    tempMetrics.incRemoteBytesReadToDisk(v)
  }
  override def incLocalBytesRead(v: Long): Unit = {
    _localBytesRead.add(v)
    tempMetrics.incLocalBytesRead(v)
  }
  override def incFetchWaitTime(v: Long): Unit = {
    _fetchWaitTime.add(v)
    tempMetrics.incFetchWaitTime(v)
  }
  override def incRecordsRead(v: Long): Unit = {
    _recordsRead.add(v)
    tempMetrics.incRecordsRead(v)
  }
}

private[spark] object SQLShuffleReadMetricsReporter {
  val REMOTE_BLOCKS_FETCHED = "remoteBlocksFetched"
  val LOCAL_BLOCKS_FETCHED = "localBlocksFetched"
  val REMOTE_BYTES_READ = "remoteBytesRead"
  val REMOTE_BYTES_READ_TO_DISK = "remoteBytesReadToDisk"
  val LOCAL_BYTES_READ = "localBytesRead"
  val FETCH_WAIT_TIME = "fetchWaitTime"
  val RECORDS_READ = "recordsRead"

  /**
   * Create all shuffle read relative metrics and return the Map.
   */
  def createShuffleReadMetrics(sc: SparkContext): Map[String, SQLMetric] = Map(
    REMOTE_BLOCKS_FETCHED -> SQLMetrics.createMetric(sc, "remote blocks fetched"),
    LOCAL_BLOCKS_FETCHED -> SQLMetrics.createMetric(sc, "local blocks fetched"),
    REMOTE_BYTES_READ -> SQLMetrics.createSizeMetric(sc, "remote bytes read"),
    REMOTE_BYTES_READ_TO_DISK -> SQLMetrics.createSizeMetric(sc, "remote bytes read to disk"),
    LOCAL_BYTES_READ -> SQLMetrics.createSizeMetric(sc, "local bytes read"),
    FETCH_WAIT_TIME -> SQLMetrics.createTimingMetric(sc, "fetch wait time"),
    RECORDS_READ -> SQLMetrics.createMetric(sc, "records read"))
}

/**
 * A shuffle write metrics reporter for SQL exchange operators. Different with
 * [[SQLShuffleReadMetricsReporter]], write metrics reporter will be set and serialized
 * in ShuffleDependency, so the local SQLMetric should transient and create on executor.
 * @param metrics Shuffle write metrics in current SparkPlan.
 */
private[spark] class SQLShuffleWriteMetricsReporter(
  metrics: Map[String, SQLMetric]) extends ShuffleWriteMetricsReporter with Serializable {
  @transient private[this] lazy val _bytesWritten =
    metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_BYTES_WRITTEN)
  @transient private[this] lazy val _recordsWritten =
    metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN)
  @transient private[this] lazy val _writeTime =
    metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_WRITE_TIME)

  override private[spark] def incBytesWritten(v: Long): Unit = {
    _bytesWritten.add(v)
  }
  override private[spark] def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.set(_recordsWritten.value - v)
  }
  override private[spark] def incRecordsWritten(v: Long): Unit = {
    _recordsWritten.add(v)
  }
  override private[spark] def incWriteTime(v: Long): Unit = {
    _writeTime.add(v)
  }
  override private[spark] def decBytesWritten(v: Long): Unit = {
    _bytesWritten.set(_bytesWritten.value - v)
  }
}

private[spark] object SQLShuffleWriteMetricsReporter {
  val SHUFFLE_BYTES_WRITTEN = "shuffleBytesWritten"
  val SHUFFLE_RECORDS_WRITTEN = "shuffleRecordsWritten"
  val SHUFFLE_WRITE_TIME = "shuffleWriteTime"

  def apply(metrics: Map[String, SQLMetric]): SQLShuffleWriteMetricsReporter = {
    new SQLShuffleWriteMetricsReporter(metrics)
  }

  /**
   * Create all shuffle write relative metrics and return the Map.
   */
  def createShuffleWriteMetrics(sc: SparkContext): Map[String, SQLMetric] = Map(
    SHUFFLE_BYTES_WRITTEN ->
      SQLMetrics.createSizeMetric(sc, "shuffle bytes written"),
    SHUFFLE_RECORDS_WRITTEN ->
      SQLMetrics.createMetric(sc, "shuffle records written"),
    SHUFFLE_WRITE_TIME ->
      SQLMetrics.createNanoTimingMetric(sc, "shuffle write time"))
}
