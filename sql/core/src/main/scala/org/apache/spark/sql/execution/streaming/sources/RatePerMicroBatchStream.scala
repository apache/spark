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

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SupportsTriggerAvailableNow}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class RatePerMicroBatchStream(
    rowsPerBatch: Long,
    numPartitions: Int,
    startTimestamp: Long,
    advanceMsPerBatch: Int,
    options: CaseInsensitiveStringMap)
  extends SupportsTriggerAvailableNow with MicroBatchStream with Logging {

  override def initialOffset(): Offset = RatePerMicroBatchStreamOffset(0L, startTimestamp)

  override def latestOffset(): Offset = {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3166")
  }

  override def getDefaultReadLimit: ReadLimit = {
    ReadLimit.maxRows(rowsPerBatch)
  }

  private def extractOffsetAndTimestamp(offset: Offset): (Long, Long) = {
    offset match {
      case o: RatePerMicroBatchStreamOffset => (o.offset, o.timestamp)
      case _ => throw new IllegalStateException("The type of Offset should be " +
        "RatePerMicroBatchStreamOffset")
    }
  }

  private var isTriggerAvailableNow: Boolean = false
  private var offsetForTriggerAvailableNow: Offset = _

  override def prepareForTriggerAvailableNow(): Unit = {
    isTriggerAvailableNow = true
  }

  override def latestOffset(startOffset: Offset, limit: ReadLimit): Offset = {
    def calculateNextOffset(start: Offset): Offset = {
      val (startOffsetLong, timestampAtStartOffset) = extractOffsetAndTimestamp(start)
      // NOTE: This means the data source will provide a set of inputs for "single" batch if
      // the trigger is Trigger.Once.
      val numRows = rowsPerBatch

      val endOffsetLong = Math.min(startOffsetLong + numRows, Long.MaxValue)
      val endOffset = RatePerMicroBatchStreamOffset(endOffsetLong,
        timestampAtStartOffset + advanceMsPerBatch)

      endOffset
    }

    if (isTriggerAvailableNow) {
      if (offsetForTriggerAvailableNow == null) {
        offsetForTriggerAvailableNow = calculateNextOffset(startOffset)
      }

      offsetForTriggerAvailableNow
    } else {
      calculateNextOffset(startOffset)
    }
  }

  override def deserializeOffset(json: String): Offset = {
    RatePerMicroBatchStreamOffset.apply(json)
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
        RatePerMicroBatchStreamInputPartition(p, numPartitions, startOffset,
          startTimestamp, endOffset, endTimestamp)
      }.toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    RatePerMicroBatchStreamReaderFactory
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {}

  override def toString: String = s"RatePerMicroBatchStream[rowsPerBatch=$rowsPerBatch, " +
    s"numPartitions=$numPartitions, startTimestamp=$startTimestamp, " +
    s"advanceMsPerBatch=$advanceMsPerBatch]"
}

case class RatePerMicroBatchStreamOffset(offset: Long, timestamp: Long) extends Offset {
  override def json(): String = {
    Serialization.write(this)(RatePerMicroBatchStreamOffset.formats)
  }
}

object RatePerMicroBatchStreamOffset {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def apply(json: String): RatePerMicroBatchStreamOffset =
    Serialization.read[RatePerMicroBatchStreamOffset](json)
}

case class RatePerMicroBatchStreamInputPartition(
    partitionId: Int,
    numPartitions: Int,
    startOffset: Long,
    startTimestamp: Long,
    endOffset: Long,
    endTimestamp: Long) extends InputPartition

object RatePerMicroBatchStreamReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[RatePerMicroBatchStreamInputPartition]
    new RatePerMicroBatchStreamPartitionReader(p.partitionId, p.numPartitions,
      p.startOffset, p.startTimestamp, p.endOffset)
  }
}

class RatePerMicroBatchStreamPartitionReader(
    partitionId: Int,
    numPartitions: Int,
    startOffset: Long,
    startTimestamp: Long,
    endOffset: Long) extends PartitionReader[InternalRow] {
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
