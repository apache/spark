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

import java.{lang => jl}

import scala.collection.JavaConverters._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator}


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends Serializable {
  private[executor] val _bytesWritten = new LongAccumulator
  private[executor] val _recordsWritten = new LongAccumulator
  private[executor] val _writeTime = new LongAccumulator
  private[executor] val _blockSizeDistribution = new Array[LongAccumulator](9)
  (0 until 9).foreach {
    case i => _blockSizeDistribution(i) = new LongAccumulator
  }
  private[executor] val _averageBlockSize = new LongAccumulator
  private[executor] val _underestimatedBlocksNum = new LongAccumulator
  private[executor] val _underestimatedBlocksSize = new LongAccumulator

  /**
   * Number of bytes written for the shuffle by this task.
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * Total number of records written to the shuffle by this task.
   */
  def recordsWritten: Long = _recordsWritten.sum

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   */
  def writeTime: Long = _writeTime.sum

  /**
   * Distribution of sizes in MapStatus. The ranges are: [0, 1k), [1k, 10k), [10k, 100k),
   * [100k, 1m), [1m, 10m), [10m, 100m), [100m, 1g), [1g, 10g), [10g, Long.MaxValue).
   */
  def blockSizeDistribution: Seq[jl.Long] = {
    _blockSizeDistribution.map(_.value).toSeq
  }

  /**
   * The average size of blocks in HighlyCompressedMapStatus.
   * This is not set if CompressedMapStatus is returned.
   */
  def averageBlockSize: Long = _averageBlockSize.value

  /**
   * The num of blocks whose sizes are underestimated in MapStatus.
   */
  def underestimatedBlocksNum: Long = _underestimatedBlocksNum.value

  /**
   * The total amount of blocks whose sizes are underestimated in MapStatus.
   */
  def underestimatedBlocksSize: Long = _underestimatedBlocksSize.value

  private[spark] def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] def incWriteTime(v: Long): Unit = _writeTime.add(v)
  private[spark] def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }

  private[spark] def incBlockSizeDistribution(len: Long): Unit = {
    len match {
      case len: Long if len >= 0L && len < 1024L => _blockSizeDistribution(0).add(1)
      case len: Long if len >= 1024L && len < 10240L => _blockSizeDistribution(1).add(1)
      case len: Long if len >= 10240L && len < 102400L => _blockSizeDistribution(2).add(1)
      case len: Long if len >= 102400L && len < 1048576L => _blockSizeDistribution(3).add(1)
      case len: Long if len >= 1048576L && len < 10485760L => _blockSizeDistribution(4).add(1)
      case len: Long if len >= 10485760L && len < 104857600L => _blockSizeDistribution(5).add(1)
      case len: Long if len >= 104857600L && len < 1073741824L => _blockSizeDistribution(6).add(1)
      case len: Long if len >= 1073741824L && len < 10737418240L => _blockSizeDistribution(7).add(1)
      case len: Long if len >= 10737418240L => _blockSizeDistribution(8).add(1)
    }
  }

  private[spark] def setAverageBlockSize(avg: Long): Unit = {
    _averageBlockSize.setValue(avg)
  }

  private[spark] def incUnderestimatedBlocksNum() = {
    _underestimatedBlocksNum.add(1)
  }

  private[spark] def incUnderestimatedBlocksSize(v: Long) = {
    _underestimatedBlocksSize.add(v)
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
