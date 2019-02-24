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

import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ListBuffer

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.{Encoder, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset => _, _}
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.streaming._
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
  extends MemoryStreamBase[A](sqlContext) with ContinuousStream {

  private implicit val formats = Serialization.formats(NoTypeHints)

  // ContinuousReader implementation

  @GuardedBy("this")
  private val records = Seq.fill(numPartitions)(new ListBuffer[A])

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

  override def initialOffset(): Offset = {
    ContinuousMemoryStreamOffset((0 until numPartitions).map(i => (i, 0)).toMap)
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


  override def planInputPartitions(start: Offset): Array[InputPartition] = {
    val startOffset = start.asInstanceOf[ContinuousMemoryStreamOffset]
    synchronized {
      val endpointName = s"ContinuousMemoryStreamRecordEndpoint-${java.util.UUID.randomUUID()}-$id"
      endpointRef =
        recordEndpoint.rpcEnv.setupEndpoint(endpointName, recordEndpoint)

      startOffset.partitionNums.map {
        case (part, index) => ContinuousMemoryStreamInputPartition(endpointName, part, index)
      }.toArray
    }
  }

  override def createContinuousReaderFactory(): ContinuousPartitionReaderFactory = {
    ContinuousMemoryStreamReaderFactory
  }

  override def stop(): Unit = {
    if (endpointRef != null) recordEndpoint.rpcEnv.stop(endpointRef)
  }

  override def commit(end: Offset): Unit = {}
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
case class ContinuousMemoryStreamInputPartition(
    driverEndpointName: String,
    partition: Int,
    startOffset: Int) extends InputPartition

object ContinuousMemoryStreamReaderFactory extends ContinuousPartitionReaderFactory {
  override def createReader(partition: InputPartition): ContinuousPartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[ContinuousMemoryStreamInputPartition]
    new ContinuousMemoryStreamPartitionReader(p.driverEndpointName, p.partition, p.startOffset)
  }
}

/**
 * An input partition reader for continuous memory stream.
 *
 * Polls the driver endpoint for new records.
 */
class ContinuousMemoryStreamPartitionReader(
    driverEndpointName: String,
    partition: Int,
    startOffset: Int) extends ContinuousPartitionReader[InternalRow] {
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
