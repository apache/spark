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
 * Metrics pertaining to shuffle data written in a given task.
 */
@DeveloperApi
class ShuffleWriteMetrics extends Serializable {
  /**
   * Number of bytes written for the shuffle by this task
   */
  @volatile private var _shuffleBytesWritten: Long = _
  def shuffleBytesWritten: Long = _shuffleBytesWritten
  private[spark] def incShuffleBytesWritten(value: Long) = _shuffleBytesWritten += value
  private[spark] def decShuffleBytesWritten(value: Long) = _shuffleBytesWritten -= value

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds
   */
  @volatile private var _shuffleWriteTime: Long = _
  def shuffleWriteTime: Long = _shuffleWriteTime
  private[spark] def incShuffleWriteTime(value: Long) = _shuffleWriteTime += value
  private[spark] def decShuffleWriteTime(value: Long) = _shuffleWriteTime -= value

  /**
   * Total number of records written to the shuffle by this task
   */
  @volatile private var _shuffleRecordsWritten: Long = _
  def shuffleRecordsWritten: Long = _shuffleRecordsWritten
  private[spark] def incShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten += value
  private[spark] def decShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten -= value
  private[spark] def setShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten = value
}
