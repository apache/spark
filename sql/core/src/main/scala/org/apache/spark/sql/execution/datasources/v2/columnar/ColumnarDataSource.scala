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

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
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

  /** Returns a reader for a batch scan of the given schema. */
  def reader(schema: StructType): ColumnarReader

  /** Returns a reader for a micro-batch streaming scan of the given schema. */
  def streamReader(schema: StructType): ColumnarStreamReader

  /**
   * Returns a writer for a batch write.
   *
   * @param overwrite whether to replace the existing data (save mode "overwrite") instead of
   *                  appending to it
   */
  def writer(schema: StructType, overwrite: Boolean): ColumnarWriter

  /**
   * Returns a writer for a streaming write.
   *
   * @param overwrite whether to replace the existing data in each micro-batch (output mode
   *                  "complete") instead of appending to it
   */
  def streamWriter(schema: StructType, overwrite: Boolean): ColumnarStreamWriter
}

/**
 * Reads the data of a [[ColumnarDataSource]] for a batch scan. Spark pushes operations down by
 * calling, each at most once and in this order, `pushPredicates`, `pushLimit` and `pruneColumns`,
 * and plans the scan with `partitions`, on the driver. Then it serializes the reader to the
 * executors, where `read` is called for each partition.
 */
trait ColumnarReader extends Serializable {

  /** Returns the predicates that Spark still has to evaluate after reading. */
  def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = predicates

  /** Returns whether the reader returns at most `limit` rows. Spark applies it again anyway. */
  def pushLimit(limit: Int): Boolean = false

  /**
   * Returns whether `read` returns exactly the columns of `requiredSchema`, the top-level columns
   * that the query needs, instead of all the columns of the read schema.
   */
  def pruneColumns(requiredSchema: StructType): Boolean = false

  def partitions(): Array[InputPartition]

  /** Reads a partition on an executor. A batch only needs to be valid until the next one. */
  def read(partition: InputPartition): PartitionReader[ColumnarBatch]
}

/**
 * Reads the data of a [[ColumnarDataSource]] for a micro-batch streaming scan, with offsets in
 * JSON. Spark plans the partitions of each micro-batch on the driver, then serializes the reader
 * to the executors, where `read` is called for each partition.
 */
trait ColumnarStreamReader extends Serializable {
  def initialOffset(): String

  def latestOffset(): String

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
 * on the driver. The batches passed to the data writers are backed by Arrow, and are only valid
 * during the call.
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
