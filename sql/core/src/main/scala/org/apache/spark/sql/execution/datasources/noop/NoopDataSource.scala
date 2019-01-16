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

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.types.StructType

/**
 * This is no-op datasource. It does not do anything besides consuming its input.
 * This can be useful for benchmarking or to cache data without any additional overhead.
 */
class NoopDataSource
  extends DataSourceV2
  with BatchWriteSupportProvider
  with DataSourceRegister {

  override def shortName(): String = "noop"

  override def createBatchWriteSupport(
      queryId: String,
      schema: StructType,
      mode: SaveMode,
      options: DataSourceOptions): Optional[BatchWriteSupport] = {
    Optional.of(new NoopWriteSupport())
  }
}

private[noop] class NoopWriteSupport extends BatchWriteSupport {
  override def createBatchWriterFactory(): DataWriterFactory = {
    new NoopWriterFactory()
  }

  override def useCommitCoordinator(): Boolean = false
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

private[noop] class NoopWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new NoopWriter()
  }
}

private[noop] class NoopWriter extends DataWriter[InternalRow] {
  override def write(record: InternalRow): Unit = {}
  override def commit(): WriterCommitMessage = null
  override def abort(): Unit = {}
}

