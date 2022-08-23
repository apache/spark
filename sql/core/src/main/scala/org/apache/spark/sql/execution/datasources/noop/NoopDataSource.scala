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

package org.apache.spark.sql.execution.datasources.noop

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, SupportsTruncate, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.internal.connector.{SimpleTableProvider, SupportsStreamingUpdateAsAppend}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * This is no-op datasource. It does not do anything besides consuming its input.
 * This can be useful for benchmarking or to cache data without any additional overhead.
 */
class NoopDataSource extends SimpleTableProvider with DataSourceRegister {
  override def shortName(): String = "noop"
  override def getTable(options: CaseInsensitiveStringMap): Table = NoopTable
}

private[noop] object NoopTable extends Table with SupportsWrite {
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = NoopWriteBuilder
  override def name(): String = "noop-table"
  override def schema(): StructType = new StructType()
  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(
      TableCapability.BATCH_WRITE,
      TableCapability.STREAMING_WRITE,
      TableCapability.TRUNCATE,
      TableCapability.ACCEPT_ANY_SCHEMA)
  }
}

private[noop] object NoopWriteBuilder extends WriteBuilder
  with SupportsTruncate with SupportsStreamingUpdateAsAppend {
  override def truncate(): WriteBuilder = this
  override def build(): Write = NoopWrite
}

private[noop] object NoopWrite extends Write {
  override def toBatch: BatchWrite = NoopBatchWrite
  override def toStreaming: StreamingWrite = NoopStreamingWrite
}

private[noop] object NoopBatchWrite extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    NoopWriterFactory
  override def useCommitCoordinator(): Boolean = false
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

private[noop] object NoopWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = NoopWriter
}

private[noop] object NoopWriter extends DataWriter[InternalRow] {
  override def write(record: InternalRow): Unit = {}
  override def commit(): WriterCommitMessage = null
  override def abort(): Unit = {}
  override def close(): Unit = {}
}

private[noop] object NoopStreamingWrite extends StreamingWrite {
  override def createStreamingWriterFactory(
      info: PhysicalWriteInfo): StreamingDataWriterFactory = NoopStreamingDataWriterFactory
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}

private[noop] object NoopStreamingDataWriterFactory extends StreamingDataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = NoopWriter
}
