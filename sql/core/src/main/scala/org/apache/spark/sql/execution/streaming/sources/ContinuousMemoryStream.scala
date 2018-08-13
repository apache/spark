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

import java.{util => ju}
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.{Encoder, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions}
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousInputPartitionReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.RpcUtils

/**
 * The overall strategy here is:
 *  * ContinuousMemoryStream maintains a list of records for each partition. addData() will
 *    distribute records evenly-ish across partitions.
 *  * RecordEndpoint is set up as an endpoint for executor-side
 *    ContinuousMemoryStreamInputPartitionReader instances to poll. It returns the record at
 *    the specified offset within the list, or null if that offset doesn't yet have a record.
 */
class ContinuousMemoryStream[A : Encoder](id: Int, sqlContext: SQLContext, numPartitions: Int = 2)
  extends MemoryStreamBase[A](sqlContext) with ContinuousReader with ContinuousReadSupport {
  private implicit val formats = Serialization.formats(NoTypeHints)

  protected val logicalPlan =
    StreamingRelationV2(this, "memory", Map(), attributes, None)(sqlContext.sparkSession)

  // ContinuousReader implementation

  @GuardedBy("this")
  private val records = Seq.fill(numPartitions)(new ListBuffer[A])

  @GuardedBy("this")
  private var startOffset: ContinuousMemoryStreamOffset = _

  private val recordEndpoint = new ContinuousRecordEndpoint(records, this)
  @volatile private var endpointRef: RpcEndpointRef = _

  def addData(data: TraversableOnce[A]): Offset = synchronized {
    // Distribute data evenly among partition lists.
    data.toSeq.zipWithIndex.map {
      case (item, index) => records(index % numPartitions) += item
    }

    // The new target offset is the offset where all records in all partitions have been processed.
    ContinuousMemoryStreamOffset((0 until numPartitions).map(i => (i, records(i).size)).toMap)
  }

  override def setStartOffset(start: Optional[Offset]): Unit = synchronized {
    // Inferred initial offset is position 0 in each partition.
    startOffset = start.orElse {
      ContinuousMemoryStreamOffset((0 until numPartitions).map(i => (i, 0)).toMap)
    }.asInstanceOf[ContinuousMemoryStreamOffset]
  }

  override def getStartOffset: Offset = synchronized {
    startOffset
  }

  override def deserializeOffset(json: String): ContinuousMemoryStreamOffset = {
    ContinuousMemoryStreamOffset(Serialization.read[Map[Int, Int]](json))
  }

  override def mergeOffsets(offsets: Array[PartitionOffset]): ContinuousMemoryStreamOffset = {
    ContinuousMemoryStreamOffset(
      offsets.map {
        case ContinuousRecordPartitionOffset(part, num) => (part, num)
      }.toMap
    )
  }

  override def planInputPartitions(): ju.List[InputPartition[InternalRow]] = {
    synchronized {
      val endpointName = s"ContinuousMemoryStreamRecordEndpoint-${java.util.UUID.randomUUID()}-$id"
      endpointRef =
        recordEndpoint.rpcEnv.setupEndpoint(endpointName, recordEndpoint)

      startOffset.partitionNums.map {
        case (part, index) =>
          new ContinuousMemoryStreamInputPartition(
            endpointName, part, index): InputPartition[InternalRow]
      }.toList.asJava
    }
  }

  override def stop(): Unit = {
    if (endpointRef != null) recordEndpoint.rpcEnv.stop(endpointRef)
  }

  override def commit(end: Offset): Unit = {}

  // ContinuousReadSupport implementation
  // This is necessary because of how StreamTest finds the source for AddDataMemory steps.
  def createContinuousReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): ContinuousReader = {
    this
  }
}

object ContinuousMemoryStream {
  protected val memoryStreamId = new AtomicInteger(0)

  def apply[A : Encoder](implicit sqlContext: SQLContext): ContinuousMemoryStream[A] =
    new ContinuousMemoryStream[A](memoryStreamId.getAndIncrement(), sqlContext)

  def singlePartition[A : Encoder](implicit sqlContext: SQLContext): ContinuousMemoryStream[A] =
    new ContinuousMemoryStream[A](memoryStreamId.getAndIncrement(), sqlContext, 1)
}

/**
 * An input partition for continuous memory stream.
 */
class ContinuousMemoryStreamInputPartition(
    driverEndpointName: String,
    partition: Int,
    startOffset: Int) extends InputPartition[InternalRow] {
  override def createPartitionReader: ContinuousMemoryStreamInputPartitionReader =
    new ContinuousMemoryStreamInputPartitionReader(driverEndpointName, partition, startOffset)
}

/**
 * An input partition reader for continuous memory stream.
 *
 * Polls the driver endpoint for new records.
 */
class ContinuousMemoryStreamInputPartitionReader(
    driverEndpointName: String,
    partition: Int,
    startOffset: Int) extends ContinuousInputPartitionReader[InternalRow] {
  private val endpoint = RpcUtils.makeDriverRef(
    driverEndpointName,
    SparkEnv.get.conf,
    SparkEnv.get.rpcEnv)

  private var currentOffset = startOffset
  private var current: Option[InternalRow] = None

  // Defense-in-depth against failing to propagate the task context. Since it's not inheritable,
  // we have to do a bit of error prone work to get it into every thread used by continuous
  // processing. We hope that some unit test will end up instantiating a continuous memory stream
  // in such cases.
  if (TaskContext.get() == null) {
    throw new IllegalStateException("Task context was not set!")
  }

  override def next(): Boolean = {
    current = getRecord
    while (current.isEmpty) {
      Thread.sleep(10)
      current = getRecord
    }
    currentOffset += 1
    true
  }

  override def get(): InternalRow = current.get

  override def close(): Unit = {}

  override def getOffset: ContinuousRecordPartitionOffset =
    ContinuousRecordPartitionOffset(partition, currentOffset)

  private def getRecord: Option[InternalRow] =
    endpoint.askSync[Option[InternalRow]](
      GetRecord(ContinuousRecordPartitionOffset(partition, currentOffset)))
}

case class ContinuousMemoryStreamOffset(partitionNums: Map[Int, Int])
  extends Offset {
  private implicit val formats = Serialization.formats(NoTypeHints)
  override def json(): String = Serialization.write(partitionNums)
}
