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

import org.apache.spark.{Accumulator, InternalAccumulator}
import org.apache.spark.annotation.DeveloperApi


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleWriteMetrics private (
    _bytesWritten: Accumulator[Long],
    _recordsWritten: Accumulator[Long],
    _writeTime: Accumulator[Long])
  extends Serializable {

  private[executor] def this(accumMap: Map[String, Accumulator[_]]) {
    this(
      TaskMetrics.getAccum[Long](accumMap, InternalAccumulator.shuffleWrite.BYTES_WRITTEN),
      TaskMetrics.getAccum[Long](accumMap, InternalAccumulator.shuffleWrite.RECORDS_WRITTEN),
      TaskMetrics.getAccum[Long](accumMap, InternalAccumulator.shuffleWrite.WRITE_TIME))
  }

  /**
   * Create a new [[ShuffleWriteMetrics]] that is not associated with any particular task.
   *
   * This mainly exists for legacy reasons, because we use dummy [[ShuffleWriteMetrics]] in
   * many places only to merge their values together later. In the future, we should revisit
   * whether this is needed.
   *
   * A better alternative is [[TaskMetrics.registerShuffleWriteMetrics]].
   */
  private[spark] def this() {
    this(InternalAccumulator.createShuffleWriteAccums().map { a => (a.name.get, a) }.toMap)
  }

  /**
   * Number of bytes written for the shuffle by this task.
   */
  def bytesWritten: Long = _bytesWritten.localValue

  /**
   * Total number of records written to the shuffle by this task.
   */
  def recordsWritten: Long = _recordsWritten.localValue

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   */
  def writeTime: Long = _writeTime.localValue

  private[spark] def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] def incWriteTime(v: Long): Unit = _writeTime.add(v)
  private[spark] def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }

  // Legacy methods for backward compatibility.
  // TODO: remove these once we make this class private.
  @deprecated("use bytesWritten instead", "2.0.0")
  def shuffleBytesWritten: Long = bytesWritten
  @deprecated("use writeTime instead", "2.0.0")
  def shuffleWriteTime: Long = writeTime
  @deprecated("use recordsWritten instead", "2.0.0")
  def shuffleRecordsWritten: Long = recordsWritten

}
