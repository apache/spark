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
  @volatile private var _bytesWritten: Long = _
  def bytesWritten: Long = _bytesWritten
  private[spark] def incBytesWritten(value: Long) = _bytesWritten += value
  private[spark] def decBytesWritten(value: Long) = _bytesWritten -= value

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds
   */
  @volatile private var _writeTime: Long = _
  def writeTime: Long = _writeTime
  private[spark] def incWriteTime(value: Long) = _writeTime += value
  private[spark] def decWriteTime(value: Long) = _writeTime -= value

  /**
   * Total number of records written to the shuffle by this task
   */
  @volatile private var _recordsWritten: Long = _
  def recordsWritten: Long = _recordsWritten
  private[spark] def incRecordsWritten(value: Long) = _recordsWritten += value
  private[spark] def decRecordsWritten(value: Long) = _recordsWritten -= value
  private[spark] def setRecordsWritten(value: Long) = _recordsWritten = value

  // Legacy methods for backward compatibility.
  // TODO: remove these once we make this class private.
  @deprecated("use bytesWritten instead", "2.0.0")
  def shuffleBytesWritten: Long = bytesWritten
  @deprecated("use writeTime instead", "2.0.0")
  def shuffleWriteTime: Long = writeTime
  @deprecated("use recordsWritten instead", "2.0.0")
  def shuffleRecordsWritten: Long = recordsWritten

}
