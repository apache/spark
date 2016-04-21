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

import org.apache.spark.InternalAccumulator
import org.apache.spark.annotation.DeveloperApi


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about reading shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleReadMetrics private[spark] () extends Serializable {
  import InternalAccumulator._

  private[executor] val _remoteBlocksFetched =
    TaskMetrics.createIntAccum(shuffleRead.REMOTE_BLOCKS_FETCHED)
  private[executor] val _localBlocksFetched =
    TaskMetrics.createIntAccum(shuffleRead.LOCAL_BLOCKS_FETCHED)
  private[executor] val _remoteBytesRead =
    TaskMetrics.createLongAccum(shuffleRead.REMOTE_BYTES_READ)
  private[executor] val _localBytesRead =
    TaskMetrics.createLongAccum(shuffleRead.LOCAL_BYTES_READ)
  private[executor] val _fetchWaitTime =
    TaskMetrics.createLongAccum(shuffleRead.FETCH_WAIT_TIME)
  private[executor] val _recordsRead =
    TaskMetrics.createLongAccum(shuffleRead.RECORDS_READ)

  /**
   * Number of remote blocks fetched in this shuffle by this task.
   */
  def remoteBlocksFetched: Int = _remoteBlocksFetched.localValue

  /**
   * Number of local blocks fetched in this shuffle by this task.
   */
  def localBlocksFetched: Int = _localBlocksFetched.localValue

  /**
   * Total number of remote bytes read from the shuffle by this task.
   */
  def remoteBytesRead: Long = _remoteBytesRead.localValue

  /**
   * Shuffle data that was read from the local disk (as opposed to from a remote executor).
   */
  def localBytesRead: Long = _localBytesRead.localValue

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  def fetchWaitTime: Long = _fetchWaitTime.localValue

  /**
   * Total number of records read from the shuffle by this task.
   */
  def recordsRead: Long = _recordsRead.localValue

  /**
   * Total bytes fetched in the shuffle by this task (both remote and local).
   */
  def totalBytesRead: Long = remoteBytesRead + localBytesRead

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local).
   */
  def totalBlocksFetched: Int = remoteBlocksFetched + localBlocksFetched

  private[spark] def incRemoteBlocksFetched(v: Int): Unit = _remoteBlocksFetched.add(v)
  private[spark] def incLocalBlocksFetched(v: Int): Unit = _localBlocksFetched.add(v)
  private[spark] def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead.add(v)
  private[spark] def incLocalBytesRead(v: Long): Unit = _localBytesRead.add(v)
  private[spark] def incFetchWaitTime(v: Long): Unit = _fetchWaitTime.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)

  private[spark] def setRemoteBlocksFetched(v: Int): Unit = _remoteBlocksFetched.setValue(v)
  private[spark] def setLocalBlocksFetched(v: Int): Unit = _localBlocksFetched.setValue(v)
  private[spark] def setRemoteBytesRead(v: Long): Unit = _remoteBytesRead.setValue(v)
  private[spark] def setLocalBytesRead(v: Long): Unit = _localBytesRead.setValue(v)
  private[spark] def setFetchWaitTime(v: Long): Unit = _fetchWaitTime.setValue(v)
  private[spark] def setRecordsRead(v: Long): Unit = _recordsRead.setValue(v)

  /**
   * Resets the value of the current metrics (`this`) and and merges all the independent
   * [[TempShuffleReadMetrics]] into `this`.
   */
  private[spark] def setMergeValues(metrics: Seq[TempShuffleReadMetrics]): Unit = {
    _remoteBlocksFetched.setValue(_remoteBlocksFetched.zero)
    _localBlocksFetched.setValue(_localBlocksFetched.zero)
    _remoteBytesRead.setValue(_remoteBytesRead.zero)
    _localBytesRead.setValue(_localBytesRead.zero)
    _fetchWaitTime.setValue(_fetchWaitTime.zero)
    _recordsRead.setValue(_recordsRead.zero)
    metrics.foreach { metric =>
      _remoteBlocksFetched.add(metric.remoteBlocksFetched)
      _localBlocksFetched.add(metric.localBlocksFetched)
      _remoteBytesRead.add(metric.remoteBytesRead)
      _localBytesRead.add(metric.localBytesRead)
      _fetchWaitTime.add(metric.fetchWaitTime)
      _recordsRead.add(metric.recordsRead)
    }
  }
}

/**
 * A temporary shuffle read metrics holder that is used to collect shuffle read metrics for each
 * shuffle dependency, and all temporary metrics will be merged into the [[ShuffleReadMetrics]] at
 * last.
 */
private[spark] class TempShuffleReadMetrics {
  private[this] var _remoteBlocksFetched = 0
  private[this] var _localBlocksFetched = 0
  private[this] var _remoteBytesRead = 0L
  private[this] var _localBytesRead = 0L
  private[this] var _fetchWaitTime = 0L
  private[this] var _recordsRead = 0L

  def incRemoteBlocksFetched(v: Int): Unit = _remoteBlocksFetched += v
  def incLocalBlocksFetched(v: Int): Unit = _localBlocksFetched += v
  def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead += v
  def incLocalBytesRead(v: Long): Unit = _localBytesRead += v
  def incFetchWaitTime(v: Long): Unit = _fetchWaitTime += v
  def incRecordsRead(v: Long): Unit = _recordsRead += v

  def remoteBlocksFetched: Int = _remoteBlocksFetched
  def localBlocksFetched: Int = _localBlocksFetched
  def remoteBytesRead: Long = _remoteBytesRead
  def localBytesRead: Long = _localBytesRead
  def fetchWaitTime: Long = _fetchWaitTime
  def recordsRead: Long = _recordsRead
}
