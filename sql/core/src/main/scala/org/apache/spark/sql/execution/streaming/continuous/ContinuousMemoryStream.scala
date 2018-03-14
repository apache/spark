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

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkEnv
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.{Encoder, SQLContext}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, SupportsScanUnsafeRow}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousDataReader, ContinuousReader, Offset, PartitionOffset}

case class GetNewRecords(partition: Int, offset: Int)

/**
 * We track the number of records processed for each partition ID.
 */
case class ContinuousMemoryStreamOffset(partitionNums: Map[Int, Int])
  extends Offset {
  private implicit val formats = Serialization.formats(NoTypeHints)
  override def json(): String = Serialization.write(partitionNums)
}

case class ContinuousMemoryStreamPartitionOffset(numProcessed: Int)

class ContinuousMemoryStream[A : Encoder](id: Int, sqlContext: SQLContext)
    extends ContinuousReader with SupportsScanUnsafeRow {
  val NUM_PARTITIONS = 2

  @GuardedBy("this")
  private val currentPartitionOffsets = mutable.Map[Int, Int].apply {
    (0 until NUM_PARTITIONS).map((_, 0))
  }

  private var startOffset: ContinuousMemoryStreamOffset = _
  private var partitionsInitialized = false

  override def setStartOffset(start: Optional[Offset]): Unit = synchronized {
    startOffset = start.asInstanceOf[ContinuousMemoryStreamOffset]
    currentPartitionOffsets ++= startOffset.partitionNums
    partitionsInitialized = false
  }

  override def createUnsafeRowReaderFactories(): ju.List[DataReaderFactory[UnsafeRow]] = {
    synchronized {
      (0 until NUM_PARTITIONS).map { i =>
        new ContinuousMemoryStreamDataReaderFactory(i, id)
          .asInstanceOf[DataReaderFactory[UnsafeRow]]
      }.asJava
    }
  }

  override def commit(end: Offset): Unit = {
    partitionsInitialized = true
  }
}

class ContinuousMemoryStreamDataReaderFactory(
    partition: Int,
    memoryStreamId: Int) extends DataReaderFactory[UnsafeRow] {
  override def createDataReader: ContinuousMemoryStreamDataReader =
    new ContinuousMemoryStreamDataReader(partition, memoryStreamId)
}

class ContinuousMemoryStreamDataReader(
    partition: Int,
    memoryStreamId: Int) extends ContinuousDataReader[UnsafeRow] {
  private val BUFFER_SIZE = 100
  private val buffer = new ArrayBlockingQueue[UnsafeRow](BUFFER_SIZE)

  private val recordReceiver = new ContinuousMemoryStreamRecordReceiver(partition, buffer)
  private val recordReceiverRef = recordReceiver.rpcEnv.setupEndpoint(
    ContinuousMemoryStream.recordReceiverName(memoryStreamId, partition), recordReceiver)

  private var current = _

  override def next(): Boolean = {
    current = buffer.poll()
    true
  }

  override def get(): UnsafeRow = current

  override def close(): Unit = {
    recordReceiver.rpcEnv.stop(recordReceiverRef)
  }
}

private case class NewRecord(partition: Int, record: UnsafeRow)

class ContinuousMemoryStreamRecordReceiver(
    partition: Int,
    buffer: ArrayBlockingQueue[UnsafeRow]) extends ThreadSafeRpcEndpoint {
  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  override def receive: PartialFunction[Any, Unit] = {
    case NewRecord(part, record) =>
      assert(partition == part)
      buffer.put(record)
  }
}

object ContinuousMemoryStream {
  def recordReceiverName(memoryStreamId: Int, partition: Int): String =
    s"ContinuousMemoryStreamRecordReceiver-$memoryStreamId-$partition"
}