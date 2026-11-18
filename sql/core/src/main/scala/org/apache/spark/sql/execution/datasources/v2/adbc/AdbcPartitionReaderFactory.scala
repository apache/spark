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

package org.apache.spark.sql.execution.datasources.v2.adbc

import java.nio.ByteBuffer

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcDatabase, AdbcStatement}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

case class AdbcPartitionReaderFactory(readSchema: StructType, options: AdbcOptions)
  extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw SparkException.internalError("The ADBC data source only supports columnar reads.")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    new AdbcPartitionReader(readSchema, options, partition.asInstanceOf[AdbcPartition])
}

/**
 * Runs a partition's query and returns the driver's Arrow batches as [[ColumnarBatch]]es,
 * wrapping their vectors without copying.
 */
class AdbcPartitionReader(readSchema: StructType, options: AdbcOptions, partition: AdbcPartition)
  extends PartitionReader[ColumnarBatch] with Logging {

  private var allocator: BufferAllocator = _
  private var database: AdbcDatabase = _
  // Read by the thread that kills the task, to cancel the query.
  @volatile private var connection: AdbcConnection = _
  @volatile private var statement: AdbcStatement = _
  @volatile private var closed = false
  private var result: AdbcStatement.QueryResult = _
  private var reader: ArrowReader = _
  private var currentBatch: ColumnarBatch = _

  private def execute(): Unit = {
    allocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"ADBC reader for ${options.driver}", 0, Long.MaxValue)
    database = AdbcUtils.openDatabase(options, allocator)
    connection = database.connect()
    val context = Option(TaskContext.get())
    context.foreach(_.addTaskInterruptListener((_, _) => cancel()))
    reader = partition match {
      case AdbcQueryPartition(query) =>
        statement = connection.createStatement()
        statement.setSqlQuery(query)
        // A kill before the statement existed couldn't cancel it.
        context.foreach(_.killTaskIfInterrupted())
        result = statement.executeQuery()
        result.getReader
      case AdbcDescriptorPartition(descriptor) =>
        context.foreach(_.killTaskIfInterrupted())
        connection.readPartition(ByteBuffer.wrap(descriptor))
    }
    // A kill while the query started may not have cancelled it, if the driver can't cancel a
    // query that isn't running yet: stop before reading its result.
    context.foreach(_.killTaskIfInterrupted())
    checkResultSchema()
  }

  /**
   * The vectors are read according to `readSchema`, which was resolved on another connection when
   * the query was planned. Fail if the result differs, rather than misread its buffers. Only the
   * types of the first `readSchema.length` columns matter: the columns are read by position, and
   * a scan of no columns has an extra one.
   */
  private def checkResultSchema(): Unit = {
    val fields = reader.getVectorSchemaRoot.getSchema.getFields
    val actual = ArrowUtils.fromArrowSchema(
      new Schema(fields.subList(0, math.min(fields.size, readSchema.length))))
    if (!DataType.equalsStructurally(readSchema, actual, ignoreNullability = true)) {
      throw QueryExecutionErrors.adbcSchemaMismatchError(readSchema, actual)
    }
  }

  override def next(): Boolean = {
    if (reader == null) execute()
    if (reader.loadNextBatch()) {
      val root = reader.getVectorSchemaRoot
      val columns = Array.tabulate[ColumnVector](readSchema.length) { i =>
        new ArrowColumnVector(root.getVector(i))
      }
      currentBatch = new ColumnarBatch(columns, root.getRowCount)
      true
    } else {
      false
    }
  }

  override def get(): ColumnarBatch = currentBatch

  /**
   * Cancels the query when the task is killed, rather than wait for a blocked read to return.
   * Called from another thread, so it may race with `close()`: a failure is only logged.
   */
  private def cancel(): Unit = synchronized {
    if (!closed) {
      try {
        if (statement != null) statement.cancel() else if (connection != null) connection.cancel()
      } catch {
        case e: Exception =>
          logWarning(log"Failed to cancel the query of the ADBC driver " +
            log"${MDC(LogKeys.CLASS_NAME, options.driver)}", e)
      }
    }
  }

  override def close(): Unit = synchronized {
    closed = true
    // Closing the result also closes its reader.
    if (result != null) Utils.closeQuietly(result) else Utils.closeQuietly(reader)
    Utils.closeQuietly(statement)
    Utils.closeQuietly(connection)
    Utils.closeQuietly(database)
    if (allocator != null) AdbcUtils.closeAllocator(allocator, options.driver)
  }
}
