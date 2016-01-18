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


/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data read in a given task.
 */
@DeveloperApi
class ShuffleReadMetrics extends Serializable {
  /**
   * Number of remote blocks fetched in this shuffle by this task
   */
  private var _remoteBlocksFetched: Int = _
  def remoteBlocksFetched: Int = _remoteBlocksFetched
  private[spark] def incRemoteBlocksFetched(value: Int) = _remoteBlocksFetched += value
  private[spark] def decRemoteBlocksFetched(value: Int) = _remoteBlocksFetched -= value

  /**
   * Number of local blocks fetched in this shuffle by this task
   */
  private var _localBlocksFetched: Int = _
  def localBlocksFetched: Int = _localBlocksFetched
  private[spark] def incLocalBlocksFetched(value: Int) = _localBlocksFetched += value
  private[spark] def decLocalBlocksFetched(value: Int) = _localBlocksFetched -= value

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  private var _fetchWaitTime: Long = _
  def fetchWaitTime: Long = _fetchWaitTime
  private[spark] def incFetchWaitTime(value: Long) = _fetchWaitTime += value
  private[spark] def decFetchWaitTime(value: Long) = _fetchWaitTime -= value

  /**
   * Total number of remote bytes read from the shuffle by this task
   */
  private var _remoteBytesRead: Long = _
  def remoteBytesRead: Long = _remoteBytesRead
  private[spark] def incRemoteBytesRead(value: Long) = _remoteBytesRead += value
  private[spark] def decRemoteBytesRead(value: Long) = _remoteBytesRead -= value

  /**
   * Shuffle data that was read from the local disk (as opposed to from a remote executor).
   */
  private var _localBytesRead: Long = _
  def localBytesRead: Long = _localBytesRead
  private[spark] def incLocalBytesRead(value: Long) = _localBytesRead += value

  /**
   * Total bytes fetched in the shuffle by this task (both remote and local).
   */
  def totalBytesRead: Long = _remoteBytesRead + _localBytesRead

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local)
   */
  def totalBlocksFetched: Int = _remoteBlocksFetched + _localBlocksFetched

  /**
   * Total number of records read from the shuffle by this task
   */
  private var _recordsRead: Long = _
  def recordsRead: Long = _recordsRead
  private[spark] def incRecordsRead(value: Long) = _recordsRead += value
  private[spark] def decRecordsRead(value: Long) = _recordsRead -= value
}
