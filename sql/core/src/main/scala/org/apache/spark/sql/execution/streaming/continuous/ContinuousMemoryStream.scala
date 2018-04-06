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

package org.apache.spark.sql.execution.streaming.continuous

import java.{util => ju}
import java.util.Optional
import java.util.concurrent.ArrayBlockingQueue
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.apache.spark.SparkEnv

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.{Dataset, Encoder, Row, SQLContext}
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous.ContinuousMemoryStreamRecordBuffer.GetRecord
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, SupportsScanUnsafeRow}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousDataReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.RpcUtils

/**
 * The overall strategy here is:
 *  * ContinuousMemoryStream maintains a list of records for each partition. addData() will
 *    distribute records evenly-ish across partitions.
 *  * ContinuousMemoryStreamRecordBuffer is set up as an endpoint for executor-side
 *    ContinuousMemoryStreamDataReader instances to poll. It returns the record at the specified
 *    offset within the list, or null if that offset doesn't yet have a record.
 */
class ContinuousMemoryStream[A : Encoder](id: Int, sqlContext: SQLContext)
  extends MemoryStreamBase[A](sqlContext) with ContinuousReader with ContinuousReadSupport {
  private implicit val formats = Serialization.formats(NoTypeHints)
  val NUM_PARTITIONS = 2

  // ContinuousReader implementation

  @GuardedBy("this")
  private val records = Seq.fill(NUM_PARTITIONS)(new ListBuffer[A])

  private val recordBuffer = new ContinuousMemoryStreamRecordBuffer(this, records)

  def addData(data: TraversableOnce[A]): Offset = synchronized {
    // Distribute data evenly among partition lists.
    data.toSeq.zipWithIndex.map {
      case (item, index) => records(index % NUM_PARTITIONS) += item
    }

    // The new target offset is the offset where all records in all partitions have been processed.
    ContinuousMemoryStreamOffset((0 until NUM_PARTITIONS).map(i => (i, records(i).size)).toMap)
  }

  private var startOffset: ContinuousMemoryStreamOffset = _

  override def setStartOffset(start: Optional[Offset]): Unit = synchronized {
    // Inferred initial offset is position 0 in each partition.
    startOffset = start.orElse {
      ContinuousMemoryStreamOffset((0 until NUM_PARTITIONS).map(i => (i, 0)).toMap)
    }.asInstanceOf[ContinuousMemoryStreamOffset]
  }

  override def getStartOffset: Offset = startOffset

  override def deserializeOffset(json: String): ContinuousMemoryStreamOffset = {
    ContinuousMemoryStreamOffset(Serialization.read[Map[Int, Int]](json))
  }

  override def mergeOffsets(offsets: Array[PartitionOffset]): ContinuousMemoryStreamOffset = {
    ContinuousMemoryStreamOffset {
      offsets.map {
        case ContinuousMemoryStreamPartitionOffset(part, num) => (part, num)
      }.toMap
    }
  }

  var endpointRef: RpcEndpointRef = _
  override def createDataReaderFactories(): ju.List[DataReaderFactory[Row]] = {
    synchronized {
      endpointRef =
        recordBuffer.rpcEnv.setupEndpoint(ContinuousMemoryStream.recordBufferName(id), recordBuffer)

      startOffset.partitionNums.map {
        case (part, index) =>
          new ContinuousMemoryStreamDataReaderFactory(id, part, index): DataReaderFactory[Row]
      }.toList.asJava
    }
  }

  override def stop(): Unit = {
    if (endpointRef != null) recordBuffer.rpcEnv.stop(endpointRef)
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

  // MemoryStreamBase implementation

  protected val logicalPlan =
    StreamingRelationV2(this, "memory", Map(), attributes, None)(sqlContext.sparkSession)

  override def reset(): Unit = synchronized {
    records.foreach(_.clear())
    startOffset = ContinuousMemoryStreamOffset((0 until NUM_PARTITIONS).map(i => (i, 0)).toMap)
  }
}

object ContinuousMemoryStream {
  def recordBufferName(memoryStreamId: Int): String =
    s"ContinuousMemoryStreamRecordReceiver-$memoryStreamId"
}

/**
 * Endpoint for executors to poll for records.
 */
private class ContinuousMemoryStreamRecordBuffer[A](
    stream: ContinuousMemoryStream[A],
    partitionBuffers: Seq[ListBuffer[A]]) extends ThreadSafeRpcEndpoint {
  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetRecord(ContinuousMemoryStreamPartitionOffset(part, index)) => stream.synchronized {
      val buf = partitionBuffers(part)

      val record =
        if (buf.size <= index) {
          None
        } else {
          Some(buf(index))
        }
      context.reply(record.map(Row(_)))
    }
  }
}

object ContinuousMemoryStreamRecordBuffer {
  case class GetRecord(offset: ContinuousMemoryStreamPartitionOffset)
}

/**
 * Data reader factory for continuous memory stream.
 */
class ContinuousMemoryStreamDataReaderFactory(
    memoryStreamId: Int,
    partition: Int,
    startOffset: Int) extends DataReaderFactory[Row] {
  override def createDataReader: ContinuousMemoryStreamDataReader =
    new ContinuousMemoryStreamDataReader(memoryStreamId, partition, startOffset)
}

/**
 * Data reader for continuous memory stream.
 *
 * Polls the driver endpoint for new records.
 */
class ContinuousMemoryStreamDataReader(
    memoryStreamId: Int,
    partition: Int,
    startOffset: Int) extends ContinuousDataReader[Row] {
  private val endpoint = RpcUtils.makeDriverRef(
    ContinuousMemoryStream.recordBufferName(memoryStreamId),
    SparkEnv.get.conf,
    SparkEnv.get.rpcEnv)

  private var currentOffset = startOffset
  private var current: Option[Row] = None

  override def next(): Boolean = {
    current = None
    while (current.isEmpty) {
      Thread.sleep(10)
      current = endpoint.askSync[Option[Row]](
          GetRecord(ContinuousMemoryStreamPartitionOffset(partition, currentOffset)))
    }
    currentOffset += 1
    true
  }

  override def get(): Row = current.get

  override def close(): Unit = {}

  override def getOffset: ContinuousMemoryStreamPartitionOffset =
    ContinuousMemoryStreamPartitionOffset(partition, currentOffset)
}

case class ContinuousMemoryStreamOffset(partitionNums: Map[Int, Int])
  extends Offset {
  private implicit val formats = Serialization.formats(NoTypeHints)
  override def json(): String = Serialization.write(partitionNums)
}

case class ContinuousMemoryStreamPartitionOffset(partition: Int, numProcessed: Int)
  extends PartitionOffset
