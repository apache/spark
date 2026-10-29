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
package org.apache.spark.sql.execution.datasources.v2.columnar

import java.util

import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, TRUNCATE}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A data source that exchanges its data as columnar batches. [[ColumnarTable]] adapts it to Data
 * Source V2. It is implemented by the native data sources, see
 * `org.apache.spark.sql.execution.datasources.v2.ffi.NativeDataSource`.
 *
 * A new instance is created on the driver for each schema inference, scan and write.
 */
trait ColumnarDataSource {

  /** Returns the schema of the data, when the user does not specify it. */
  def schema(): StructType

  /**
   * Returns the capabilities that [[ColumnarTable]] reports to Data Source V2, which checks some
   * of them while it analyzes a query: by default, all the operations that this trait has.
   */
  def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, TRUNCATE)

  /** Returns the planner of a batch scan of the given schema. */
  def batchPlanner(schema: StructType): ColumnarBatchPlanner

  /**
   * Returns a reader for a micro-batch streaming scan of the given schema.
   *
   * @param checkpointLocation the location of a directory in the checkpoint of the streaming
   *                           query that is reserved for this source. It stays the same when the
   *                           query restarts from its checkpoint.
   */
  def streamReader(schema: StructType, checkpointLocation: String): ColumnarStreamReader

  /**
   * Returns a writer for a batch write.
   *
   * @param overwrite whether to replace the existing data (save mode "overwrite") instead of
   *                  appending to it
   */
  def writer(schema: StructType, overwrite: Boolean): ColumnarWriter

  /**
   * Returns a writer for a streaming write. A micro-batch streaming query creates a new one for
   * each micro-batch.
   *
   * @param overwrite whether to replace the existing data in each micro-batch (output mode
   *                  "complete") instead of appending to it
   * @param queryId the ID of the streaming query
   */
  def streamWriter(schema: StructType, overwrite: Boolean, queryId: String): ColumnarStreamWriter
}

/**
 * Plans a batch scan of a [[ColumnarDataSource]], on the driver. Spark pushes operations down by
 * calling, each at most once and in this order, `pushLimit` and `pruneColumns`, then plans the
 * scan with `plan`, at most once. Only the [[PartitionReaderFactory]] of the planned scan is
 * serialized to the executors.
 */
trait ColumnarBatchPlanner {

  /** Returns whether the reader returns at most `limit` rows. Spark applies it again anyway. */
  def pushLimit(limit: Int): Boolean = false

  /**
   * Returns whether the partitions return exactly the columns of `requiredSchema`, the top-level
   * columns that the query needs, including the columns that it filters on, instead of all the
   * columns of the read schema.
   */
  def pruneColumns(requiredSchema: StructType): Boolean = false

  /**
   * Plans the partitions of the scan, after the pushdown. The reader factory reads them on the
   * executors, with columnar readers whose batches only need to be valid until the next one.
   */
  def plan(): ColumnarPlannedBatch
}

/** The partitions of a planned batch scan, and the factory that reads them on the executors. */
case class ColumnarPlannedBatch(
    partitions: Array[InputPartition],
    readerFactory: PartitionReaderFactory)

/**
 * Reads the data of a [[ColumnarDataSource]] for a micro-batch streaming scan, with offsets in
 * JSON. Spark plans the partitions of each micro-batch on the driver, then serializes the reader
 * to the executors, where `read` is called for each partition.
 */
trait ColumnarStreamReader extends Serializable {
  def initialOffset(): String

  /**
   * Returns the latest offset available.
   *
   * @param start the offset that the next micro-batch starts after: the end of the last
   *              micro-batch, or the initial offset before the first one
   */
  def latestOffset(start: String): String

  /** Plans the partitions that read the data after `start` up to and including `end`. */
  def partitions(start: String, end: String): Array[InputPartition]

  def read(partition: InputPartition): PartitionReader[ColumnarBatch]

  /** Tells the data source that Spark has processed all the data up to and including `end`. */
  def commit(end: String): Unit = {}

  def stop(): Unit = {}
}

/**
 * Writes data to a [[ColumnarDataSource]] for a batch write. Spark serializes the writer to the
 * executors, where `createWriter` is called for each task, and then commits or aborts the write
 * on the driver. The batches passed to the data writers are backed by new Arrow vectors, with the
 * Arrow types of the schema, except for the time zones of timestamps and the `keysSorted` flag of
 * maps, though not necessarily with its names, nullability and metadata, nor the Spark types that
 * `ArrowColumnVector` infers from them. A batch is only valid during the call, but nothing else
 * writes to the buffers of its vectors, so a data writer may take ownership of them to keep its
 * data.
 */
trait ColumnarWriter extends Serializable {
  def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch]

  def commit(messages: Array[WriterCommitMessage]): Unit

  def abort(messages: Array[WriterCommitMessage]): Unit
}

/** Writes data to a [[ColumnarDataSource]] for a streaming write, one micro-batch at a time. */
trait ColumnarStreamWriter extends Serializable {
  def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[ColumnarBatch]

  def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit

  def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit
}
