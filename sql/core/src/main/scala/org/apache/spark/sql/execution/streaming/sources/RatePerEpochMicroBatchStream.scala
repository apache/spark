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

package org.apache.spark.sql.execution.streaming.sources

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, ReadMaxRows, SupportsAdmissionControl}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class RatePerEpochMicroBatchStream(
    rowsPerEpoch: Long,
    numPartitions: Int = 1,
    startTimestamp: Long = 0,
    advanceMsPerEpoch: Int = 0,
    options: CaseInsensitiveStringMap)
  extends SupportsAdmissionControl with MicroBatchStream with Logging {

  override def initialOffset(): Offset = RatePerEpochStreamOffset(0L, startTimestamp)

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def getDefaultReadLimit: ReadLimit = {
    ReadLimit.maxRows(rowsPerEpoch)
  }

  private def extractOffsetAndTimestamp(offset: Offset): (Long, Long) = {
    offset match {
      case o: RatePerEpochStreamOffset => (o.offset, o.timestamp)
      case _ => throw new IllegalStateException("The type of Offset should be " +
        "RatePerEpochStreamOffset")
    }
  }

  override def latestOffset(startOffset: Offset, limit: ReadLimit): Offset = {
    val (startOffsetLong, timestampAtStartOffset) = extractOffsetAndTimestamp(startOffset)
    val numRows = limit.asInstanceOf[ReadMaxRows].maxRows()

    val endOffsetLong = Math.min(startOffsetLong + numRows, Long.MaxValue)
    val endOffset = RatePerEpochStreamOffset(endOffsetLong,
      timestampAtStartOffset + advanceMsPerEpoch)

    endOffset
  }

  override def deserializeOffset(json: String): Offset = {
    RatePerEpochStreamOffset.apply(json)
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val (startOffset, startTimestamp) = extractOffsetAndTimestamp(start)
    val (endOffset, endTimestamp) = extractOffsetAndTimestamp(end)

    assert(startOffset <= endOffset, s"startOffset($startOffset) > endOffset($endOffset)")
    assert(startTimestamp <= endTimestamp,
      s"startTimestamp($startTimestamp) > endTimestamp($endTimestamp)")
    logDebug(s"startOffset: $startOffset, startTimestamp: $startTimestamp, " +
      s"endOffset: $endOffset, endTimestamp: $endTimestamp")

    if (startOffset == endOffset) {
      Array.empty
    } else {
      (0 until numPartitions).map { p =>
        RatePerEpochStreamMicroBatchInputPartition(p, numPartitions, startOffset,
          startTimestamp, endOffset, endTimestamp)
      }.toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    RatePerEpochStreamMicroBatchReaderFactory
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {}

  override def toString: String = s"RatePerEpochStream[rowsPerEpoch=$rowsPerEpoch, " +
    s"numPartitions=$numPartitions, startTimestamp=$startTimestamp, " +
    s"advanceMsPerEpoch=$advanceMsPerEpoch]"
}

case class RatePerEpochStreamOffset(offset: Long, timestamp: Long) extends Offset {
  override def json(): String = {
    Serialization.write(this)(RatePerEpochStreamOffset.formats)
  }
}

object RatePerEpochStreamOffset {
  implicit val formats = Serialization.formats(NoTypeHints)

  def apply(json: String): RatePerEpochStreamOffset =
    Serialization.read[RatePerEpochStreamOffset](json)
}

case class RatePerEpochStreamMicroBatchInputPartition(
    partitionId: Int,
    numPartitions: Int,
    startOffset: Long,
    startTimestamp: Long,
    endOffset: Long,
    endTimestamp: Long
) extends InputPartition

object RatePerEpochStreamMicroBatchReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[RatePerEpochStreamMicroBatchInputPartition]
    new RatePerEpochStreamMicroBatchPartitionReader(p.partitionId, p.numPartitions,
      p.startOffset, p.startTimestamp, p.endOffset, p.endTimestamp)
  }
}

class RatePerEpochStreamMicroBatchPartitionReader(
    partitionId: Int,
    numPartitions: Int,
    startOffset: Long,
    startTimestamp: Long,
    endOffset: Long,
    endTimestamp: Long) extends PartitionReader[InternalRow] {
  private var count: Long = 0

  override def next(): Boolean = {
    startOffset + partitionId + numPartitions * count < endOffset
  }

  override def get(): InternalRow = {
    val currValue = startOffset + partitionId + numPartitions * count
    count += 1

    InternalRow(DateTimeUtils.millisToMicros(startTimestamp), currValue)
  }

  override def close(): Unit = {}
}
