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
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, SupportsScanUnsafeRow}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousDataReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.RpcUtils

case class GetNewRecords(partition: Int, offset: Int)

/**
 * We track the number of records processed for each partition ID.
 */
case class ContinuousMemoryStreamOffset(partitionNums: Map[Int, Int])
    extends Offset {
  private implicit val formats = Serialization.formats(NoTypeHints)
  override def json(): String = Serialization.write(partitionNums)
}

case class ContinuousMemoryStreamPartitionOffset(partition: Int, numProcessed: Int)
  extends PartitionOffset


private case class GetNewRecord(offset: ContinuousMemoryStreamPartitionOffset)

class ContinuousMemoryStreamRecordBuffer[A](
    stream: ContinuousMemoryStream[A],
    partitionToBuffer: Seq[ListBuffer[A]]) extends ThreadSafeRpcEndpoint {
  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetNewRecord(ContinuousMemoryStreamPartitionOffset(part, index)) => stream.synchronized {
      val buf = partitionToBuffer(part)

      val record =
        if (buf.size <= index) {
          null
        } else {
          buf(index)
        }
      context.reply(record)
    }
  }
}

class ContinuousMemoryStream[A : ClassTag : Encoder](id: Int, sqlContext: SQLContext)
    extends MemoryStreamBase[A](sqlContext) with ContinuousReader with ContinuousReadSupport {
  private implicit val formats = Serialization.formats(NoTypeHints)
  val NUM_PARTITIONS = 2

  def createContinuousReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): ContinuousReader = {
    this
  }
  override protected val logicalPlan =
    StreamingRelationV2(this, "memory", Map(), attributes, None)(sqlContext.sparkSession)

  @GuardedBy("this")
  private val inputs = Seq.fill(NUM_PARTITIONS)(new ListBuffer[A])

  private val recordBuffer = new ContinuousMemoryStreamRecordBuffer(this, inputs)

  def addData(data: TraversableOnce[A]): Offset = synchronized {
    // Distribute data evenly among partition lists.
    data.toSeq.zipWithIndex.map {
      case (item, index) => inputs(index % NUM_PARTITIONS) += item
    }

    ContinuousMemoryStreamOffset((0 until NUM_PARTITIONS).map(i => (i, inputs(i).size)).toMap)
  }

  private var startOffset: ContinuousMemoryStreamOffset = _
  private var partitionsInitialized = false

  override def setStartOffset(start: Optional[Offset]): Unit = synchronized {
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
          new ContinuousMemoryStreamDataReaderFactory[A](id, part, index): DataReaderFactory[Row]
      }.toList.asJava
    }
  }

  override def commit(end: Offset): Unit = {
    partitionsInitialized = true
  }

  override def readSchema(): StructType = encoderFor[A].schema

  override def stop(): Unit = {
    recordBuffer.rpcEnv.stop(endpointRef)
    partitionsInitialized = false
  }

  override def reset(): Unit = synchronized {
    inputs.foreach(_.clear())
    startOffset = ContinuousMemoryStreamOffset((0 until NUM_PARTITIONS).map(i => (i, 0)).toMap)
  }
}

class ContinuousMemoryStreamDataReaderFactory[A : ClassTag](
    memoryStreamId: Int,
    partition: Int,
    startOffset: Int) extends DataReaderFactory[Row] {
  override def createDataReader: ContinuousMemoryStreamDataReader[A] =
    new ContinuousMemoryStreamDataReader[A](memoryStreamId, partition, startOffset)
}

class ContinuousMemoryStreamDataReader[A : ClassTag](
    memoryStreamId: Int,
    partition: Int,
    startOffset: Int) extends ContinuousDataReader[Row] {
  private val endpoint = RpcUtils.makeDriverRef(
    ContinuousMemoryStream.recordBufferName(memoryStreamId),
    SparkEnv.get.conf,
    SparkEnv.get.rpcEnv)

  private var currentOffset = startOffset
  private var current: Option[A] = None

  override def next(): Boolean = {
    current = None
    while (current.isEmpty) {
      Thread.sleep(100)
      current = Option(
        endpoint.askSync[A](
          GetNewRecord(ContinuousMemoryStreamPartitionOffset(partition, currentOffset))))
    }
    currentOffset += 1
    true
  }

  override def get(): Row = Row(current.get)

  override def close(): Unit = {}

  override def getOffset: ContinuousMemoryStreamPartitionOffset =
    ContinuousMemoryStreamPartitionOffset(partition, currentOffset)
}

object ContinuousMemoryStream {
  def recordBufferName(memoryStreamId: Int): String =
    s"ContinuousMemoryStreamRecordReceiver-$memoryStreamId"
}
