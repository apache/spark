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

package org.apache.spark.shuffle

import org.apache.spark.executor.ShuffleReadMetrics

/**
 * An interface for reporting shuffle information, for each shuffle. This interface assumes
 * all the methods are called on a single-threaded, i.e. concrete implementations would not need
 * to synchronize anything.
 */
private[spark] trait ShuffleMetricsReporter {
  def incRemoteBlocksFetched(v: Long): Unit
  def incLocalBlocksFetched(v: Long): Unit
  def incRemoteBytesRead(v: Long): Unit
  def incRemoteBytesReadToDisk(v: Long): Unit
  def incLocalBytesRead(v: Long): Unit
  def incFetchWaitTime(v: Long): Unit
  def incRecordsRead(v: Long): Unit
}

/**
 * A temporary shuffle read metrics holder that is used to collect shuffle read metrics for each
 * shuffle dependency, and all temporary metrics will be merged into the [[ShuffleReadMetrics]] at
 * last.
 */
private[spark] class TempShuffleReadMetrics extends ShuffleMetricsReporter {
  private[this] var _remoteBlocksFetched = 0L
  private[this] var _localBlocksFetched = 0L
  private[this] var _remoteBytesRead = 0L
  private[this] var _remoteBytesReadToDisk = 0L
  private[this] var _localBytesRead = 0L
  private[this] var _fetchWaitTime = 0L
  private[this] var _recordsRead = 0L

  override def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched += v
  override def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched += v
  override def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead += v
  override def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk += v
  override def incLocalBytesRead(v: Long): Unit = _localBytesRead += v
  override def incFetchWaitTime(v: Long): Unit = _fetchWaitTime += v
  override def incRecordsRead(v: Long): Unit = _recordsRead += v

  def remoteBlocksFetched: Long = _remoteBlocksFetched
  def localBlocksFetched: Long = _localBlocksFetched
  def remoteBytesRead: Long = _remoteBytesRead
  def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk
  def localBytesRead: Long = _localBytesRead
  def fetchWaitTime: Long = _fetchWaitTime
  def recordsRead: Long = _recordsRead
}
