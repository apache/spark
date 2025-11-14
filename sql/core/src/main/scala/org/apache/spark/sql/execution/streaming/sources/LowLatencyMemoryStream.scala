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

package org.apache.spark.sql.execution.streaming

import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ListBuffer

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.sql.{Encoder, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.streaming.{
  Offset => OffsetV2,
  PartitionOffset,
  ReadLimit,
  SupportsRealTimeMode,
  SupportsRealTimeRead
}
import org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead.RecordStatus
import org.apache.spark.sql.execution.datasources.v2.LowLatencyClock
import org.apache.spark.sql.execution.streaming.runtime._
import org.apache.spark.util.{Clock, RpcUtils}

/**
 * A low latency memory source from memory, only for unit test purpose.
 * This class is very similar to ContinuousMemoryStream, except that it implements the
 * interface of SupportsRealTimeMode, rather than ContinuousStream
 * The overall strategy here is:
 *  * LowLatencyMemoryStream maintains a list of records for each partition. addData() will
 *    distribute records evenly-ish across partitions.
 *  * RecordEndpoint is set up as an endpoint for executor-side
 *    LowLatencyMemoryStreamInputPartitionReader instances to poll. It returns the record at
 *    the specified offset within the list, or null if that offset doesn't yet have a record.
 * This differs from the existing memory source implementation as data is sent once to
 * tasks as part of the Partition/Split metadata at the beginning of a batch.
 */
class LowLatencyMemoryStream[A: Encoder](
    id: Int,
    sparkSession: SparkSession,
    numPartitions: Int = 2,
    clock: Clock = LowLatencyClock.getClock)
    extends MemoryStreamBaseClass[A](0, sparkSession)
    with SupportsRealTimeMode {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  @GuardedBy("this")
  private val records = Seq.fill(numPartitions)(new ListBuffer[UnsafeRow])

  private val recordEndpoint = new ContinuousRecordEndpoint(records, this)
  @volatile private var endpointRef: RpcEndpointRef = _

  override def addData(data: IterableOnce[A]): Offset = synchronized {
    // Distribute data evenly among partition lists.
    data.iterator.to(Seq).zipWithIndex.map {
      case (item, index) =>
        records(index % numPartitions) += toRow(item).copy().asInstanceOf[UnsafeRow]
    }

    // The new target offset is the offset where all records in all partitions have been processed.
    LowLatencyMemoryStreamOffset((0 until numPartitions).map(i => (i, records(i).size)).toMap)
  }

  def addData(partitionId: Int, data: IterableOnce[A]): Offset = synchronized {
    require(
      partitionId >= 0 && partitionId < numPartitions,
      s"Partition ID $partitionId is out of bounds for $numPartitions partitions."
    )

    // Add data to the specified partition.
    records(partitionId) ++= data.iterator.map(item => toRow(item).copy().asInstanceOf[UnsafeRow])

    // The new target offset is the offset where all records in all partitions have been processed.
    LowLatencyMemoryStreamOffset((0 until numPartitions).map(i => (i, records(i).size)).toMap)
  }

  override def initialOffset(): OffsetV2 = {
    LowLatencyMemoryStreamOffset((0 until numPartitions).map(i => (i, 0)).toMap)
  }

  override def latestOffset(startOffset: OffsetV2, limit: ReadLimit): OffsetV2 = synchronized {
    LowLatencyMemoryStreamOffset((0 until numPartitions).map(i => (i, records(i).size)).toMap)
  }

  override def deserializeOffset(json: String): LowLatencyMemoryStreamOffset = {
    LowLatencyMemoryStreamOffset(Serialization.read[Map[Int, Int]](json))
  }

  override def mergeOffsets(offsets: Array[PartitionOffset]): LowLatencyMemoryStreamOffset = {
    LowLatencyMemoryStreamOffset(
      offsets.map {
        case ContinuousRecordPartitionOffset(part, num) => (part, num)
      }.toMap
    )
  }

  override def planInputPartitions(start: OffsetV2): Array[InputPartition] = {
    val startOffset = start.asInstanceOf[LowLatencyMemoryStreamOffset]
    synchronized {
      val endpointName = s"LowLatencyRecordEndpoint-${java.util.UUID.randomUUID()}-$id"
      endpointRef = recordEndpoint.rpcEnv.setupEndpoint(endpointName, recordEndpoint)

      startOffset.partitionNums.map {
        case (part, index) =>
          LowLatencyMemoryStreamInputPartition(
            endpointName,
            endpointRef.address,
            part,
            index,
            Int.MaxValue
          )
      }.toArray
    }
  }

  override def planInputPartitions(start: OffsetV2, end: OffsetV2): Array[InputPartition] = {
    val startOffset = start.asInstanceOf[LowLatencyMemoryStreamOffset]
    val endOffset = end.asInstanceOf[LowLatencyMemoryStreamOffset]
    synchronized {
      val endpointName = s"LowLatencyRecordEndpoint-${java.util.UUID.randomUUID()}-$id"
      endpointRef = recordEndpoint.rpcEnv.setupEndpoint(endpointName, recordEndpoint)

      startOffset.partitionNums.map {
        case (part, index) =>
          LowLatencyMemoryStreamInputPartition(
            endpointName,
            endpointRef.address,
            part,
            index,
            endOffset.partitionNums(part)
          )
      }.toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new LowLatencyMemoryStreamReaderFactory(clock)
  }

  override def stop(): Unit = {
    if (endpointRef != null) recordEndpoint.rpcEnv.stop(endpointRef)
  }

  override def commit(end: OffsetV2): Unit = {}

  override def reset(): Unit = synchronized {
    super.reset()
    records.foreach(_.clear())
  }
}

object LowLatencyMemoryStream extends LowPriorityLowLatencyMemoryStreamImplicits {
  protected val memoryStreamId = new AtomicInteger(0)

  def apply[A: Encoder](implicit sparkSession: SparkSession): LowLatencyMemoryStream[A] =
    new LowLatencyMemoryStream[A](memoryStreamId.getAndIncrement(), sparkSession)

  def apply[A: Encoder](numPartitions: Int)(
      implicit
      sparkSession: SparkSession): LowLatencyMemoryStream[A] =
    new LowLatencyMemoryStream[A](
      memoryStreamId.getAndIncrement(),
      sparkSession,
      numPartitions = numPartitions
    )

  def singlePartition[A: Encoder](implicit sparkSession: SparkSession): LowLatencyMemoryStream[A] =
    new LowLatencyMemoryStream[A](memoryStreamId.getAndIncrement(), sparkSession, 1)
}

/**
 * Provides lower-priority implicits for LowLatencyMemoryStream to prevent ambiguity when both
 * SparkSession and SQLContext are in scope. The implicits in the companion object,
 * which use SparkSession, take higher precedence.
 */
trait LowPriorityLowLatencyMemoryStreamImplicits {
  this: LowLatencyMemoryStream.type =>

  // Deprecated: Used when an implicit SQLContext is in scope
  @deprecated("Use LowLatencyMemoryStream with an implicit SparkSession " +
    "instead of SQLContext", "4.1.0")
  def apply[A: Encoder]()(implicit sqlContext: SQLContext): LowLatencyMemoryStream[A] =
    new LowLatencyMemoryStream[A](memoryStreamId.getAndIncrement(), sqlContext.sparkSession)

  @deprecated("Use LowLatencyMemoryStream with an implicit SparkSession " +
    "instead of SQLContext", "4.1.0")
  def apply[A: Encoder](numPartitions: Int)(implicit sqlContext: SQLContext):
  LowLatencyMemoryStream[A] =
    new LowLatencyMemoryStream[A](
      memoryStreamId.getAndIncrement(),
      sqlContext.sparkSession,
      numPartitions = numPartitions
    )

  @deprecated("Use LowLatencyMemoryStream.singlePartition with an implicit SparkSession " +
    "instead of SQLContext", "4.1.0")
  def singlePartition[A: Encoder]()(implicit sqlContext: SQLContext): LowLatencyMemoryStream[A] =
    new LowLatencyMemoryStream[A](memoryStreamId.getAndIncrement(), sqlContext.sparkSession, 1)
}

/**
 * An input partition for LowLatency memory stream.
 */
case class LowLatencyMemoryStreamInputPartition(
    driverEndpointName: String,
    driverEndpointAddress: RpcAddress,
    partition: Int,
    startOffset: Int,
    endOffset: Int)
    extends InputPartition

class LowLatencyMemoryStreamReaderFactory(clock: Clock) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[LowLatencyMemoryStreamInputPartition]
    new LowLatencyMemoryStreamPartitionReader(
      p.driverEndpointName,
      p.driverEndpointAddress,
      p.partition,
      p.startOffset,
      p.endOffset,
      clock
    )
  }
}

/**
 * An input partition reader for LowLatency memory stream.
 *
 * Polls the driver endpoint for new records.
 */
class LowLatencyMemoryStreamPartitionReader(
    driverEndpointName: String,
    driverEndpointAddress: RpcAddress,
    partition: Int,
    startOffset: Int,
    endOffset: Int,
    clock: Clock)
    extends SupportsRealTimeRead[InternalRow] {
  // Avoid tracking the ref, given that we create a new one for each partition reader
  // because a new driver endpoint is created for each LowLatencyMemoryStream. If we track the ref,
  // we can end up with a lot of refs (1000s) if a test suite has so many test cases and can lead to
  // issues with the tracking array. Causing the test suite to be flaky.
  private val endpoint = RpcUtils.makeDriverRef(
    driverEndpointName,
    driverEndpointAddress.host,
    driverEndpointAddress.port,
    SparkEnv.get.rpcEnv
  )

  private var currentOffset = startOffset
  private var current: Option[InternalRow] = None

  // Defense-in-depth against failing to propagate the task context. Since it's not inheritable,
  // we have to do a bit of error prone work to get it into every thread used by LowLatency
  // processing. We hope that some unit test will end up instantiating a LowLatency memory stream
  // in such cases.
  if (TaskContext.get() == null) {
    throw new IllegalStateException("Task context was not set!")
  }
  override def nextWithTimeout(timeout: java.lang.Long): RecordStatus = {
    val startReadTime = clock.nanoTime()
    var elapsedTimeMs = 0L
    current = getRecord
    while (current.isEmpty) {
      val POLL_TIME = 10L
      if (elapsedTimeMs >= timeout) {
        return RecordStatus.newStatusWithoutArrivalTime(false)
      }
      Thread.sleep(POLL_TIME)
      current = getRecord
      elapsedTimeMs = (clock.nanoTime() - startReadTime) / 1000 / 1000
    }
    currentOffset += 1
    RecordStatus.newStatusWithoutArrivalTime(true)
  }

  override def next(): Boolean = {
    current = getRecord
    if (current.isDefined) {
      currentOffset += 1
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = current.get

  override def close(): Unit = {}

  override def getOffset: ContinuousRecordPartitionOffset =
    ContinuousRecordPartitionOffset(partition, currentOffset)

  private def getRecord: Option[InternalRow] = {
    if (currentOffset >= endOffset) {
      return None
    }
    endpoint.askSync[Option[InternalRow]](
      GetRecord(ContinuousRecordPartitionOffset(partition, currentOffset))
    )
  }
}

case class LowLatencyMemoryStreamOffset(partitionNums: Map[Int, Int]) extends Offset {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)
  override def json(): String = Serialization.write(partitionNums)
}
