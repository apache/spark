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

package org.apache.spark.sql.execution.datasources.v2

import java.util.Objects

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, SupportsRealTimeMode, SupportsRealTimeRead}
import org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead.RecordStatus
import org.apache.spark.sql.internal.connector.PartitionOffsetWithIndex
import org.apache.spark.util.{Clock, CollectionAccumulator, SystemClock}
import org.apache.spark.util.ArrayImplicits._

/* The singleton object to control the time in testing */
object LowLatencyClock {
  private var clock: Clock = new SystemClock

  def getClock: Clock = clock

  def getTimeMillis(): Long = {
    clock.getTimeMillis()
  }

  def waitTillTime(targetTime: Long): Unit = {
    clock.waitTillTime(targetTime)
  }

  def setClock(inputClock: Clock): Unit = {
    clock = inputClock
  }
}

/**
 * A wrap reader that turns a Partition Reader extending SupportsRealTimeRead to a
 * normal PartitionReader and follow the task termination time `lowLatencyEndTime`, and
 * report end offsets in the end to `endOffsets`.
 */
case class LowLatencyReaderWrap(
    reader: SupportsRealTimeRead[InternalRow],
    lowLatencyEndTime: Long,
    endOffsets: CollectionAccumulator[PartitionOffsetWithIndex])
    extends PartitionReader[InternalRow] {

  override def next(): Boolean = {
    val curTime = LowLatencyClock.getTimeMillis()
    val ret = if (curTime >= lowLatencyEndTime) {
      RecordStatus.newStatusWithoutArrivalTime(false)
    } else {
      reader.nextWithTimeout(lowLatencyEndTime - curTime)
    }

    if (!ret.hasRecord) {
      // The way of using TaskContext.get().partitionId() to map to a partition
      // may be fragile as it relies on thread locals.
      endOffsets.add(
        new PartitionOffsetWithIndex(TaskContext.get().partitionId(), reader.getOffset)
      )
    }
    ret.hasRecord
  }

  override def get(): InternalRow = {
    reader.get()
  }

  override def close(): Unit = {
    reader.close()
  }
}

/**
 * Wrapper factory that creates LowLatencyReaderWrap from reader as SupportsRealTimeRead
 */
case class LowLatencyReaderFactoryWrap(
    partitionReaderFactory: PartitionReaderFactory,
    lowLatencyEndTime: Long,
    endOffsets: CollectionAccumulator[PartitionOffsetWithIndex])
    extends PartitionReaderFactory
    with Logging {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val rowReader = partitionReaderFactory.createReader(partition)
    assert(rowReader.isInstanceOf[SupportsRealTimeRead[InternalRow]])
    logInfo(
      log"Creating low latency PartitionReader, stopping at " +
      log"${MDC(LogKeys.TO_TIME, lowLatencyEndTime)}"
    )
    LowLatencyReaderWrap(
      rowReader.asInstanceOf[SupportsRealTimeRead[InternalRow]],
      lowLatencyEndTime,
      endOffsets
    )
  }
}

/**
 * Physical plan node for Real-time Mode to scan/read data from a data source.
 */
case class RealTimeStreamScanExec(
    output: Seq[Attribute],
    @transient scan: Scan,
    @transient stream: MicroBatchStream,
    @transient start: Offset,
    batchDurationMs: Long)
    extends DataSourceV2ScanExecBase {

  assert(stream.isInstanceOf[SupportsRealTimeMode])

  override def keyGroupedPartitioning: Option[Seq[Expression]] = None

  override def ordering: Option[Seq[SortOrder]] = None

  val endOffsetsAccumulator: CollectionAccumulator[PartitionOffsetWithIndex] = {
    SparkContext.getActive.map(_.collectionAccumulator[PartitionOffsetWithIndex]).get
  }

  override def equals(other: Any): Boolean = other match {
    case other: RealTimeStreamScanExec =>
      this.stream == other.stream &&
      this.batchDurationMs == other.batchDurationMs
    case _ => false
  }

  override def hashCode(): Int = Objects.hashCode(stream, batchDurationMs)

  override lazy val readerFactory: PartitionReaderFactory = stream.createReaderFactory()

  override lazy val inputPartitions: Seq[InputPartition] = {
    val lls = stream.asInstanceOf[SupportsRealTimeMode]
    assert(lls != null)
    lls.planInputPartitions(start).toImmutableArraySeq
  }

  override def simpleString(maxFields: Int): String =
    s"${super.simpleString(maxFields)} [batchDurationMs=${batchDurationMs}ms]"

  override lazy val inputRDD: RDD[InternalRow] = {

    val inputRDD = new DataSourceRDD(
      sparkContext,
      partitions,
      LowLatencyReaderFactoryWrap(
        readerFactory,
        LowLatencyClock.getTimeMillis() + batchDurationMs,
        endOffsetsAccumulator
      ),
      supportsColumnar,
      customMetrics
    )
    postDriverMetrics()
    inputRDD
  }
}
