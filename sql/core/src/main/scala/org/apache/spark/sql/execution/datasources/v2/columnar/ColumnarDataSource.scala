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
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.ReadLimit
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

  /**
   * Returns whether the batch planners of this data source use the domains of the columns that
   * the filters of a query constrain, see [[ColumnarBatchPlanner.pushDomains]]. Spark derives the
   * domains, and creates the planner of a scan to push them down to, only when it is true: by
   * default, a data source takes no domain.
   */
  def supportsDomainPushdown(): Boolean = false

  /** Returns the planner of a batch scan of the given schema. */
  def batchPlanner(schema: StructType): ColumnarBatchPlanner

  /**
   * Returns a reader for a micro-batch streaming scan of the given schema: the columns to read,
   * which may be only the top-level columns of the schema of the data that the query needs. The
   * partitions return exactly these columns.
   *
   * @param checkpointLocation the location of a directory in the checkpoint of the streaming
   *                           query that is reserved for this source. It stays the same when the
   *                           query restarts from its checkpoint. The names that start with
   *                           `_spark` are reserved for Spark there.
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
 * calling, each at most once and in this order, `pushDomains`, `pushLimit` and `pruneColumns`,
 * then plans the scan with `plan`, at most once. Only the [[PartitionReaderFactory]] of the
 * planned scan is serialized to the executors.
 */
trait ColumnarBatchPlanner {

  /**
   * Returns whether the reader uses each domain to skip data: the domains of the columns that the
   * filters of the query constrain, at least one. Spark evaluates the filters after reading
   * anyway, and shows the filters of the used domains as pushed in the plan. Spark only calls it
   * for a data source whose [[ColumnarDataSource.supportsDomainPushdown]] is true.
   */
  def pushDomains(domains: Seq[ColumnDomain]): Seq[Boolean] = domains.map(_ => false)

  /** Returns whether the reader returns at most `limit` rows. Spark applies it again anyway. */
  def pushLimit(limit: Int): Boolean = false

  /**
   * Prunes the columns to read to `requiredSchema`, the top-level columns that the query needs,
   * including the columns that it filters on: the batches of the partitions then have exactly
   * these columns, in this order. Spark calls it only when the query needs fewer columns than the
   * read schema.
   */
  def pruneColumns(requiredSchema: StructType): Unit

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
 * JSON. Spark plans the partitions of each micro-batch on the driver with this reader, which stays
 * there, and serializes the reader factory that `readerFactory` returns to the executors, which
 * read the partitions with it.
 */
trait ColumnarStreamReader {
  /**
   * Returns the offset to start from when the streaming query has no checkpoint. Spark keeps it in
   * the checkpoint of the source, and only asks for it when the checkpoint does not have it yet.
   */
  def initialOffset(): String

  /**
   * Returns the limit on the data of each micro-batch, which Spark asks for once, when the query
   * starts: `ReadLimit.allAvailable()` by default, or a `ReadLimit.maxRows`.
   */
  def defaultReadLimit(): ReadLimit = ReadLimit.allAvailable()

  /**
   * Returns the latest offset available, within a limit.
   *
   * @param start the offset that the next micro-batch starts after: the end of the last
   *              micro-batch, or the initial offset before the first one
   * @param limit `ReadLimit.allAvailable()`, or the default read limit: the latest offset is then
   *              the end of the data of the next micro-batch. Spark only passes the default read
   *              limit when the latest offset without a limit is not `start`
   * @param runEnd with `Trigger.AvailableNow` and a limit, the offset that the run reads up to,
   *               which the latest offset does not go beyond
   */
  def latestOffset(start: String, limit: ReadLimit, runEnd: Option[String]): String

  /**
   * Plans the partitions that read the data after `start` up to and including `end`. Spark plans
   * each range once, also for a micro-batch that reads the source several times.
   */
  def partitions(start: String, end: String): Array[InputPartition]

  /**
   * Returns the reader factory of a micro-batch, which Spark serializes to the executors. It may
   * also implement other interfaces of reader factories, such as
   * `org.apache.spark.sql.connector.read.SupportsArrowStreamRead`.
   */
  def readerFactory(): PartitionReaderFactory

  /** Tells the data source that Spark has processed all the data up to and including `end`. */
  def commit(end: String): Unit = {}

  def stop(): Unit = {}
}

/**
 * Writes data to a [[ColumnarDataSource]] for a batch write. Spark serializes the writer to the
 * executors, where `createWriter` is called for each task, and then commits or aborts the write
 * on the driver. Spark does not abort a write whose commit succeeded. The batches passed to the
 * data writers are backed by new Arrow vectors, with the Arrow types of the schema, except for the
 * time zones of timestamps and the `keysSorted` flag of maps, though not necessarily with its
 * names, nullability and metadata, nor the Spark types that `ArrowColumnVector` infers from them. A
 * batch is only valid during the call, but nothing else writes to the buffers of its vectors, so a
 * data writer may take ownership of them to keep its data, until it is closed: the buffers belong
 * to the data writer, which must release them by the time its `close` returns. The columns that
 * Spark copies from the Arrow batches of a scan have the values that the scan returned: they are
 * not checked as converted values are, for example against the precision of a decimal. The batches
 * have at most `spark.sql.execution.arrow.maxRecordsPerBatch` rows, and may have fewer, since the
 * batches that Spark copies end with the batches of the scan.
 */
trait ColumnarWriter extends Serializable {
  def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch]

  def commit(messages: Array[WriterCommitMessage]): Unit

  def abort(messages: Array[WriterCommitMessage]): Unit
}

/**
 * Writes data to a [[ColumnarDataSource]] for a streaming write, for a single micro-batch: Spark
 * creates a new writer for each micro-batch, and does not call a writer after it committed or
 * aborted its micro-batch. Spark does not abort a micro-batch once the writer committed it, but a
 * restarted query may abort a micro-batch that a writer of an earlier run of the query committed.
 */
trait ColumnarStreamWriter extends Serializable {
  def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[ColumnarBatch]

  def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit

  def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit
}
