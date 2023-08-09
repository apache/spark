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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.util.LongAccumulator


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about reading shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleReadMetrics private[spark] () extends Serializable {
  private[executor] val _remoteBlocksFetched = new LongAccumulator
  private[executor] val _localBlocksFetched = new LongAccumulator
  private[executor] val _remoteBytesRead = new LongAccumulator
  private[executor] val _remoteBytesReadToDisk = new LongAccumulator
  private[executor] val _localBytesRead = new LongAccumulator
  private[executor] val _fetchWaitTime = new LongAccumulator
  private[executor] val _recordsRead = new LongAccumulator
  private[executor] val _corruptMergedBlockChunks = new LongAccumulator
  private[executor] val _mergedFetchFallbackCount = new LongAccumulator
  private[executor] val _remoteMergedBlocksFetched = new LongAccumulator
  private[executor] val _localMergedBlocksFetched = new LongAccumulator
  private[executor] val _remoteMergedChunksFetched = new LongAccumulator
  private[executor] val _localMergedChunksFetched = new LongAccumulator
  private[executor] val _remoteMergedBytesRead = new LongAccumulator
  private[executor] val _localMergedBytesRead = new LongAccumulator
  private[executor] val _remoteReqsDuration = new LongAccumulator
  private[executor] val _remoteMergedReqsDuration = new LongAccumulator

  /**
   * Number of remote blocks fetched in this shuffle by this task.
   */
  def remoteBlocksFetched: Long = _remoteBlocksFetched.sum

  /**
   * Number of local blocks fetched in this shuffle by this task.
   */
  def localBlocksFetched: Long = _localBlocksFetched.sum

  /**
   * Total number of remote bytes read from the shuffle by this task.
   */
  def remoteBytesRead: Long = _remoteBytesRead.sum

  /**
   * Total number of remotes bytes read to disk from the shuffle by this task.
   */
  def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk.sum

  /**
   * Shuffle data that was read from the local disk (as opposed to from a remote executor).
   */
  def localBytesRead: Long = _localBytesRead.sum

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  def fetchWaitTime: Long = _fetchWaitTime.sum

  /**
   * Total number of records read from the shuffle by this task.
   */
  def recordsRead: Long = _recordsRead.sum

  /**
   * Total bytes fetched in the shuffle by this task (both remote and local).
   */
  def totalBytesRead: Long = remoteBytesRead + localBytesRead

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local).
   */
  def totalBlocksFetched: Long = remoteBlocksFetched + localBlocksFetched

  /**
   * Number of corrupt merged shuffle block chunks encountered by this task (remote or local).
   */
  def corruptMergedBlockChunks: Long = _corruptMergedBlockChunks.sum

  /**
   * Number of times the task had to fallback to fetch original shuffle blocks for a merged
   * shuffle block chunk (remote or local).
   */
  def mergedFetchFallbackCount: Long = _mergedFetchFallbackCount.sum

  /**
   * Number of remote merged blocks fetched.
   */
  def remoteMergedBlocksFetched: Long = _remoteMergedBlocksFetched.sum

  /**
   * Number of local merged blocks fetched.
   */
  def localMergedBlocksFetched: Long = _localMergedBlocksFetched.sum

  /**
   * Number of remote merged chunks fetched.
   */
  def remoteMergedChunksFetched: Long = _remoteMergedChunksFetched.sum

  /**
   * Number of local merged chunks fetched.
   */
  def localMergedChunksFetched: Long = _localMergedChunksFetched.sum

  /**
   * Total number of remote merged bytes read.
   */
  def remoteMergedBytesRead: Long = _remoteMergedBytesRead.sum

  /**
   * Total number of local merged bytes read.
   */
  def localMergedBytesRead: Long = _localMergedBytesRead.sum

  /**
   * Total time taken for remote requests to complete by this task. This doesn't include
   * duration of remote merged requests.
   */
  def remoteReqsDuration: Long = _remoteReqsDuration.sum

  /**
   * Total time taken for remote merged requests.
   */
  def remoteMergedReqsDuration: Long = _remoteMergedReqsDuration.sum

  private[spark] def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched.add(v)
  private[spark] def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched.add(v)
  private[spark] def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead.add(v)
  private[spark] def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk.add(v)
  private[spark] def incLocalBytesRead(v: Long): Unit = _localBytesRead.add(v)
  private[spark] def incFetchWaitTime(v: Long): Unit = _fetchWaitTime.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
  private[spark] def incCorruptMergedBlockChunks(v: Long): Unit = _corruptMergedBlockChunks.add(v)
  private[spark] def incMergedFetchFallbackCount(v: Long): Unit = _mergedFetchFallbackCount.add(v)
  private[spark] def incRemoteMergedBlocksFetched(v: Long): Unit = _remoteMergedBlocksFetched.add(v)
  private[spark] def incLocalMergedBlocksFetched(v: Long): Unit = _localMergedBlocksFetched.add(v)
  private[spark] def incRemoteMergedChunksFetched(v: Long): Unit = _remoteMergedChunksFetched.add(v)
  private[spark] def incLocalMergedChunksFetched(v: Long): Unit = _localMergedChunksFetched.add(v)
  private[spark] def incRemoteMergedBytesRead(v: Long): Unit =
    _remoteMergedBytesRead.add(v)
  private[spark] def incLocalMergedBytesRead(v: Long): Unit =
    _localMergedBytesRead.add(v)
  private[spark] def incRemoteReqsDuration(v: Long): Unit = _remoteReqsDuration.add(v)
  private[spark] def incRemoteMergedReqsDuration(v: Long): Unit = _remoteMergedReqsDuration.add(v)

  private[spark] def setRemoteBlocksFetched(v: Int): Unit = _remoteBlocksFetched.setValue(v)
  private[spark] def setLocalBlocksFetched(v: Int): Unit = _localBlocksFetched.setValue(v)
  private[spark] def setRemoteBytesRead(v: Long): Unit = _remoteBytesRead.setValue(v)
  private[spark] def setRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk.setValue(v)
  private[spark] def setLocalBytesRead(v: Long): Unit = _localBytesRead.setValue(v)
  private[spark] def setFetchWaitTime(v: Long): Unit = _fetchWaitTime.setValue(v)
  private[spark] def setRecordsRead(v: Long): Unit = _recordsRead.setValue(v)
  private[spark] def setCorruptMergedBlockChunks(v: Long): Unit =
    _corruptMergedBlockChunks.setValue(v)
  private[spark] def setMergedFetchFallbackCount(v: Long): Unit =
    _mergedFetchFallbackCount.setValue(v)
  private[spark] def setRemoteMergedBlocksFetched(v: Long): Unit =
    _remoteMergedBlocksFetched.setValue(v)
  private[spark] def setLocalMergedBlocksFetched(v: Long): Unit =
    _localMergedBlocksFetched.setValue(v)
  private[spark] def setRemoteMergedChunksFetched(v: Long): Unit =
    _remoteMergedChunksFetched.setValue(v)
  private[spark] def setLocalMergedChunksFetched(v: Long): Unit =
    _localMergedChunksFetched.setValue(v)
  private[spark] def setRemoteMergedBytesRead(v: Long): Unit =
    _remoteMergedBytesRead.setValue(v)
  private[spark] def setLocalMergedBytesRead(v: Long): Unit =
    _localMergedBytesRead.setValue(v)
  private[spark] def setRemoteReqsDuration(v: Long): Unit = _remoteReqsDuration.setValue(v)
  private[spark] def setRemoteMergedReqsDuration(v: Long): Unit =
    _remoteMergedReqsDuration.setValue(v)

  /**
   * Resets the value of the current metrics (`this`) and merges all the independent
   * [[TempShuffleReadMetrics]] into `this`.
   */
  private[spark] def setMergeValues(metrics: Seq[TempShuffleReadMetrics]): Unit = {
    _remoteBlocksFetched.setValue(0)
    _localBlocksFetched.setValue(0)
    _remoteBytesRead.setValue(0)
    _remoteBytesReadToDisk.setValue(0)
    _localBytesRead.setValue(0)
    _fetchWaitTime.setValue(0)
    _recordsRead.setValue(0)
    _corruptMergedBlockChunks.setValue(0)
    _mergedFetchFallbackCount.setValue(0)
    _remoteMergedBlocksFetched.setValue(0)
    _localMergedBlocksFetched.setValue(0)
    _remoteMergedChunksFetched.setValue(0)
    _localMergedChunksFetched.setValue(0)
    _remoteMergedBytesRead.setValue(0)
    _localMergedBytesRead.setValue(0)
    _remoteReqsDuration.setValue(0)
    _remoteMergedReqsDuration.setValue(0)
    metrics.foreach { metric =>
      _remoteBlocksFetched.add(metric.remoteBlocksFetched)
      _localBlocksFetched.add(metric.localBlocksFetched)
      _remoteBytesRead.add(metric.remoteBytesRead)
      _remoteBytesReadToDisk.add(metric.remoteBytesReadToDisk)
      _localBytesRead.add(metric.localBytesRead)
      _fetchWaitTime.add(metric.fetchWaitTime)
      _recordsRead.add(metric.recordsRead)
      _corruptMergedBlockChunks.add(metric.corruptMergedBlockChunks)
      _mergedFetchFallbackCount.add(metric.mergedFetchFallbackCount)
      _remoteMergedBlocksFetched.add(metric.remoteMergedBlocksFetched)
      _localMergedBlocksFetched.add(metric.localMergedBlocksFetched)
      _remoteMergedChunksFetched.add(metric.remoteMergedChunksFetched)
      _localMergedChunksFetched.add(metric.localMergedChunksFetched)
      _remoteMergedBytesRead.add(metric.remoteMergedBytesRead)
      _localMergedBytesRead.add(metric.localMergedBytesRead)
      _remoteReqsDuration.add(metric.remoteReqsDuration)
      _remoteMergedReqsDuration.add(metric.remoteMergedReqsDuration)
    }
  }
}


/**
 * A temporary shuffle read metrics holder that is used to collect shuffle read metrics for each
 * shuffle dependency, and all temporary metrics will be merged into the [[ShuffleReadMetrics]] at
 * last.
 */
private[spark] class TempShuffleReadMetrics extends ShuffleReadMetricsReporter {
  private[this] var _remoteBlocksFetched = 0L
  private[this] var _localBlocksFetched = 0L
  private[this] var _remoteBytesRead = 0L
  private[this] var _remoteBytesReadToDisk = 0L
  private[this] var _localBytesRead = 0L
  private[this] var _fetchWaitTime = 0L
  private[this] var _recordsRead = 0L
  private[this] var _corruptMergedBlockChunks = 0L
  private[this] var _mergedFetchFallbackCount = 0L
  private[this] var _remoteMergedBlocksFetched = 0L
  private[this] var _localMergedBlocksFetched = 0L
  private[this] var _remoteMergedChunksFetched = 0L
  private[this] var _localMergedChunksFetched = 0L
  private[this] var _remoteMergedBytesRead = 0L
  private[this] var _localMergedBytesRead = 0L
  private[this] var _remoteReqsDuration = 0L
  private[this] var _remoteMergedReqsDuration = 0L

  override def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched += v
  override def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched += v
  override def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead += v
  override def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk += v
  override def incLocalBytesRead(v: Long): Unit = _localBytesRead += v
  override def incFetchWaitTime(v: Long): Unit = _fetchWaitTime += v
  override def incRecordsRead(v: Long): Unit = _recordsRead += v
  override def incCorruptMergedBlockChunks(v: Long): Unit = _corruptMergedBlockChunks += v
  override def incMergedFetchFallbackCount(v: Long): Unit = _mergedFetchFallbackCount += v
  override def incRemoteMergedBlocksFetched(v: Long): Unit = _remoteMergedBlocksFetched += v
  override def incLocalMergedBlocksFetched(v: Long): Unit = _localMergedBlocksFetched += v
  override def incRemoteMergedChunksFetched(v: Long): Unit = _remoteMergedChunksFetched += v
  override def incLocalMergedChunksFetched(v: Long): Unit = _localMergedChunksFetched += v
  override def incRemoteMergedBytesRead(v: Long): Unit = _remoteMergedBytesRead += v
  override def incLocalMergedBytesRead(v: Long): Unit = _localMergedBytesRead += v
  override def incRemoteReqsDuration(v: Long): Unit = _remoteReqsDuration += v
  override def incRemoteMergedReqsDuration(v: Long): Unit = _remoteMergedReqsDuration += v

  def remoteBlocksFetched: Long = _remoteBlocksFetched
  def localBlocksFetched: Long = _localBlocksFetched
  def remoteBytesRead: Long = _remoteBytesRead
  def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk
  def localBytesRead: Long = _localBytesRead
  def fetchWaitTime: Long = _fetchWaitTime
  def recordsRead: Long = _recordsRead
  def corruptMergedBlockChunks: Long = _corruptMergedBlockChunks
  def mergedFetchFallbackCount: Long = _mergedFetchFallbackCount
  def remoteMergedBlocksFetched: Long = _remoteMergedBlocksFetched
  def localMergedBlocksFetched: Long = _localMergedBlocksFetched
  def remoteMergedChunksFetched: Long = _remoteMergedChunksFetched
  def localMergedChunksFetched: Long = _localMergedChunksFetched
  def remoteMergedBytesRead: Long = _remoteMergedBytesRead
  def localMergedBytesRead: Long = _localMergedBytesRead
  def remoteReqsDuration: Long = _remoteReqsDuration
  def remoteMergedReqsDuration: Long = _remoteMergedReqsDuration
}
