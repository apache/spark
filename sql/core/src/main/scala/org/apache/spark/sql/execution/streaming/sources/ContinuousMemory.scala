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

import java.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, SparkUnsupportedOperationException}
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.connector.catalog.{SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.write.{
  LogicalWriteInfo,
  PhysicalWriteInfo,
  Write,
  WriteBuilder,
  WriterCommitMessage
}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.internal.connector.SupportsStreamingUpdateAsAppend
import org.apache.spark.sql.types.StructType

/**
 * A sink that stores the results in memory. This [[org.apache.spark.sql.execution.streaming.Sink]]
 * is primarily intended for use in unit tests and does not provide durability.
 * This is mostly copied from MemorySink, except that the data needs to be available not in
 * commit() but after each write.
 */
class ContinuousMemorySink
    extends MemorySink
    with SupportsWrite {

  private val batches = new ArrayBuffer[Row]()
  override def name(): String = "ContinuousMemorySink"

  override def schema(): StructType = StructType(Nil)

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.STREAMING_WRITE)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder with SupportsStreamingUpdateAsAppend {
      private val inputSchema: StructType = info.schema()

      override def build(): Write = {
        new ContinuousMemoryWrite(batches, inputSchema)
      }
    }
  }

  /** Returns all rows that are stored in this [[Sink]]. */
  override def allData: Seq[Row] = {
    val batches = getBatches()
    batches.synchronized {
      batches.toSeq
    }
  }

  override def latestBatchId: Option[Long] = {
    None
  }

  override def latestBatchData: Seq[Row] = {
    throw new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_OPERATION_FOR_CONTINUOUS_MEMORY_SINK",
      messageParameters = Map("operation" -> "latestBatchData")
    )
  }

  override def dataSinceBatch(sinceBatchId: Long): Seq[Row] = {
    throw new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_OPERATION_FOR_CONTINUOUS_MEMORY_SINK",
      messageParameters = Map("operation" -> "dataSinceBatch")
    )
  }

  override def toDebugString: String = {
    s"${allData}"
  }

  override def write(batchId: Long, needTruncate: Boolean, newRows: Array[Row]): Unit = {
    throw new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_OPERATION_FOR_CONTINUOUS_MEMORY_SINK",
      messageParameters = Map("operation" -> "write")
    )
  }

  override def clear(): Unit = synchronized {
    batches.clear()
  }

  private def getBatches(): ArrayBuffer[Row] = {
    batches
  }

  override def toString(): String = "ContinuousMemorySink"
}

class ContinuousMemoryWrite(batches: ArrayBuffer[Row], schema: StructType) extends Write {
  override def toStreaming: StreamingWrite = {
    new ContinuousMemoryStreamingWrite(batches, schema)
  }
}

/**
 * An RPC endpoint that receives rows and stores them to the ArrayBuffer in real-time.
 */
class MemoryRealTimeRpcEndpoint(
    override val rpcEnv: RpcEnv,
    schema: StructType,
    batches: ArrayBuffer[Row]
) extends ThreadSafeRpcEndpoint {
  private val encoder = ExpressionEncoder(schema).resolveAndBind().createDeserializer()

  override def receive: PartialFunction[Any, Unit] = {
    case rows: Array[InternalRow] =>
      // synchronized block is optional here since ThreadSafeRpcEndpoint already, just to be safe
      batches.synchronized {
        rows.foreach { row =>
          batches += encoder(row)
        }
      }
  }
}

class ContinuousMemoryStreamingWrite(val batches: ArrayBuffer[Row], schema: StructType)
    extends StreamingWrite {

  private val memoryEndpoint =
    new MemoryRealTimeRpcEndpoint(
      SparkEnv.get.rpcEnv,
      schema,
      batches
    )
  @volatile private var endpointRef: RpcEndpointRef = _

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    val endpointName = s"MemoryRealTimeRpcEndpoint-${java.util.UUID.randomUUID()}"
    endpointRef = memoryEndpoint.rpcEnv.setupEndpoint(endpointName, memoryEndpoint)
    RealTimeRowWriterFactory(endpointName, endpointRef.address)
  }

  override def useCommitCoordinator(): Boolean = false

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    // We don't need to commit anything in this case, as the rows have already been printed
    if (endpointRef != null) {
      memoryEndpoint.rpcEnv.stop(endpointRef)
    }
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    if (endpointRef != null) {
      memoryEndpoint.rpcEnv.stop(endpointRef)
    }
  }
}
