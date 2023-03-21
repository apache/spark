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
 * A shuffle metrics reporter for SQL exchange operators.
 * @param tempMetrics [[TempShuffleReadMetrics]] created in TaskContext.
 * @param metrics All metrics in current SparkPlan. This param should not empty and
 *   contains all shuffle metrics defined in createShuffleReadMetrics.
 */
class SQLShuffleReadMetricsReporter(
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
  private[this] val _corruptMergedBlockChunks =
    metrics(SQLShuffleReadMetricsReporter.CORRUPT_MERGED_BLOCK_CHUNKS)
  private[this] val _mergedFetchFallbackCount =
    metrics(SQLShuffleReadMetricsReporter.MERGED_FETCH_FALLBACK_COUNT)
  private[this] val _remoteMergedBlocksFetched =
    metrics(SQLShuffleReadMetricsReporter.REMOTE_MERGED_BLOCKS_FETCHED)
  private[this] val _localMergedBlocksFetched =
    metrics(SQLShuffleReadMetricsReporter.LOCAL_MERGED_BLOCKS_FETCHED)
  private[this] val _remoteMergedChunksFetched =
    metrics(SQLShuffleReadMetricsReporter.REMOTE_MERGED_CHUNKS_FETCHED)
  private[this] val _localMergedChunksFetched =
    metrics(SQLShuffleReadMetricsReporter.LOCAL_MERGED_CHUNKS_FETCHED)
  private[this] val _remoteMergedBytesRead =
    metrics(SQLShuffleReadMetricsReporter.REMOTE_MERGED_BYTES_READ)
  private[this] val _localMergedBytesRead =
    metrics(SQLShuffleReadMetricsReporter.LOCAL_MERGED_BYTES_READ)
  private[this] val _remoteReqsDuration =
    metrics(SQLShuffleReadMetricsReporter.REMOTE_REQS_DURATION)
  private[this] val _remoteMergedReqsDuration =
    metrics(SQLShuffleReadMetricsReporter.REMOTE_MERGED_REQS_DURATION)

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
  override def incCorruptMergedBlockChunks(v: Long): Unit = {
    _corruptMergedBlockChunks.add(v)
    tempMetrics.incCorruptMergedBlockChunks(v)
  }
  override def incMergedFetchFallbackCount(v: Long): Unit = {
    _mergedFetchFallbackCount.add(v)
    tempMetrics.incMergedFetchFallbackCount(v)
  }
  override def incRemoteMergedBlocksFetched(v: Long): Unit = {
    _remoteMergedBlocksFetched.add(v)
    tempMetrics.incRemoteMergedBlocksFetched(v)
  }
  override def incLocalMergedBlocksFetched(v: Long): Unit = {
    _localMergedBlocksFetched.add(v)
    tempMetrics.incLocalMergedBlocksFetched(v)
  }
  override def incRemoteMergedChunksFetched(v: Long): Unit = {
    _remoteMergedChunksFetched.add(v)
    tempMetrics.incRemoteMergedChunksFetched(v)
  }
  override def incLocalMergedChunksFetched(v: Long): Unit = {
    _localMergedChunksFetched.add(v)
    tempMetrics.incLocalMergedChunksFetched(v)
  }
  override def incRemoteMergedBytesRead(v: Long): Unit = {
    _remoteMergedBytesRead.add(v)
    tempMetrics.incRemoteMergedBytesRead(v)
  }
  override def incLocalMergedBytesRead(v: Long): Unit = {
    _localMergedBytesRead.add(v)
    tempMetrics.incLocalMergedBytesRead(v)
  }
  override def incRemoteReqsDuration(v: Long): Unit = {
    _remoteReqsDuration.add(v)
    tempMetrics.incRemoteReqsDuration(v)
  }
  override def incRemoteMergedReqsDuration(v: Long): Unit = {
    _remoteMergedReqsDuration.add(v)
    tempMetrics.incRemoteMergedReqsDuration(v)
  }
}

object SQLShuffleReadMetricsReporter {
  val REMOTE_BLOCKS_FETCHED = "remoteBlocksFetched"
  val LOCAL_BLOCKS_FETCHED = "localBlocksFetched"
  val REMOTE_BYTES_READ = "remoteBytesRead"
  val REMOTE_BYTES_READ_TO_DISK = "remoteBytesReadToDisk"
  val LOCAL_BYTES_READ = "localBytesRead"
  val FETCH_WAIT_TIME = "fetchWaitTime"
  val RECORDS_READ = "recordsRead"
  val CORRUPT_MERGED_BLOCK_CHUNKS = "corruptMergedBlockChunks"
  val MERGED_FETCH_FALLBACK_COUNT = "mergedFetchFallbackCount"
  val REMOTE_MERGED_BLOCKS_FETCHED = "remoteMergedBlocksFetched"
  val LOCAL_MERGED_BLOCKS_FETCHED = "localMergedBlocksFetched"
  val REMOTE_MERGED_CHUNKS_FETCHED = "remoteMergedChunksFetched"
  val LOCAL_MERGED_CHUNKS_FETCHED = "localMergedChunksFetched"
  val REMOTE_MERGED_BYTES_READ = "remoteMergedBytesRead"
  val LOCAL_MERGED_BYTES_READ = "localMergedBytesRead"
  val REMOTE_REQS_DURATION = "remoteReqsDuration"
  val REMOTE_MERGED_REQS_DURATION = "remoteMergedReqsDuration"

  /**
   * Create all shuffle read relative metrics and return the Map.
   */
  def createShuffleReadMetrics(sc: SparkContext): Map[String, SQLMetric] = Map(
    REMOTE_BLOCKS_FETCHED -> SQLMetrics.createMetric(sc, "remote blocks read"),
    LOCAL_BLOCKS_FETCHED -> SQLMetrics.createMetric(sc, "local blocks read"),
    REMOTE_BYTES_READ -> SQLMetrics.createSizeMetric(sc, "remote bytes read"),
    REMOTE_BYTES_READ_TO_DISK -> SQLMetrics.createSizeMetric(sc, "remote bytes read to disk"),
    LOCAL_BYTES_READ -> SQLMetrics.createSizeMetric(sc, "local bytes read"),
    FETCH_WAIT_TIME -> SQLMetrics.createTimingMetric(sc, "fetch wait time"),
    RECORDS_READ -> SQLMetrics.createMetric(sc, "records read"),
    CORRUPT_MERGED_BLOCK_CHUNKS -> SQLMetrics.createMetric(sc, "corrupt merged block chunks"),
    MERGED_FETCH_FALLBACK_COUNT -> SQLMetrics.createMetric(sc, "merged fetch fallback count"),
    REMOTE_MERGED_BLOCKS_FETCHED -> SQLMetrics.createMetric(sc, "remote merged blocks fetched"),
    LOCAL_MERGED_BLOCKS_FETCHED -> SQLMetrics.createMetric(sc, "local merged blocks fetched"),
    REMOTE_MERGED_CHUNKS_FETCHED -> SQLMetrics.createMetric(sc, "remote merged chunks fetched"),
    LOCAL_MERGED_CHUNKS_FETCHED -> SQLMetrics.createMetric(sc, "local merged chunks fetched"),
    REMOTE_MERGED_BYTES_READ -> SQLMetrics.createSizeMetric(sc,
      "remote merged bytes read"),
    LOCAL_MERGED_BYTES_READ -> SQLMetrics.createSizeMetric(sc,
      "local merged bytes read"),
    REMOTE_REQS_DURATION -> SQLMetrics.createTimingMetric(sc, "remote reqs duration"),
    REMOTE_MERGED_REQS_DURATION -> SQLMetrics.createTimingMetric(sc, "remote merged reqs duration"))
}

/**
 * A shuffle write metrics reporter for SQL exchange operators.
 * @param metricsReporter Other reporter need to be updated in this SQLShuffleWriteMetricsReporter.
 * @param metrics Shuffle write metrics in current SparkPlan.
 */
class SQLShuffleWriteMetricsReporter(
    metricsReporter: ShuffleWriteMetricsReporter,
    metrics: Map[String, SQLMetric]) extends ShuffleWriteMetricsReporter {
  private[this] val _bytesWritten =
    metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_BYTES_WRITTEN)
  private[this] val _recordsWritten =
    metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN)
  private[this] val _writeTime =
    metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_WRITE_TIME)

  override def incBytesWritten(v: Long): Unit = {
    metricsReporter.incBytesWritten(v)
    _bytesWritten.add(v)
  }
  override def decRecordsWritten(v: Long): Unit = {
    metricsReporter.decRecordsWritten(v)
    _recordsWritten.set(_recordsWritten.value - v)
  }
  override def incRecordsWritten(v: Long): Unit = {
    metricsReporter.incRecordsWritten(v)
    _recordsWritten.add(v)
  }
  override def incWriteTime(v: Long): Unit = {
    metricsReporter.incWriteTime(v)
    _writeTime.add(v)
  }
  override def decBytesWritten(v: Long): Unit = {
    metricsReporter.decBytesWritten(v)
    _bytesWritten.set(_bytesWritten.value - v)
  }
}

object SQLShuffleWriteMetricsReporter {
  val SHUFFLE_BYTES_WRITTEN = "shuffleBytesWritten"
  val SHUFFLE_RECORDS_WRITTEN = "shuffleRecordsWritten"
  val SHUFFLE_WRITE_TIME = "shuffleWriteTime"

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
