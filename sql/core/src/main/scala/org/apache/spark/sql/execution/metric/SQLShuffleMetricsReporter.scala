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

import org.apache.spark.executor.TempShuffleReadMetrics

/**
 * A shuffle metrics reporter for SQL exchange operators.
 * @param tempMetrics [[TempShuffleReadMetrics]] created in TaskContext.
 * @param metrics All metrics in current SparkPlan. This param should not empty and
 *   contains all shuffle metrics defined in [[SQLMetrics.getShuffleReadMetrics]].
 */
private[spark] class SQLShuffleMetricsReporter(
  tempMetrics: TempShuffleReadMetrics,
  metrics: Map[String, SQLMetric]) extends TempShuffleReadMetrics {
  private[this] val _remoteBlocksFetched = metrics(SQLMetrics.REMOTE_BLOCKS_FETCHED)
  private[this] val _localBlocksFetched = metrics(SQLMetrics.LOCAL_BLOCKS_FETCHED)
  private[this] val _remoteBytesRead = metrics(SQLMetrics.REMOTE_BYTES_READ)
  private[this] val _remoteBytesReadToDisk = metrics(SQLMetrics.REMOTE_BYTES_READ_TO_DISK)
  private[this] val _localBytesRead = metrics(SQLMetrics.LOCAL_BYTES_READ)
  private[this] val _fetchWaitTime = metrics(SQLMetrics.FETCH_WAIT_TIME)
  private[this] val _recordsRead = metrics(SQLMetrics.RECORDS_READ)

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
