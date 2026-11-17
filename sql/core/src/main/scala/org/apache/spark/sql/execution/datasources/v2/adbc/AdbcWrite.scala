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

import scala.util.Using

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcDatabase, AdbcException, AdbcStatement, AdbcStatusCode, BulkIngestMode, IngestOption}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo, SupportsTruncate, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

/**
 * The table that a write ingests into.
 *
 * @param name the table name, passed to the driver's bulk ingestion as a single identifier
 * @param dbSchema the database schema of the table, or None for the connection's current schema
 * @param sqlName the table as it appears in SQL statements, quoted
 */
case class AdbcWriteTarget(name: String, dbSchema: Option[String], sqlName: String) {

  /** Returns a statement that ingests the data bound to it into the table. */
  def bulkIngest(connection: AdbcConnection, mode: BulkIngestMode): AdbcStatement =
    dbSchema match {
      case Some(s) => connection.bulkIngest(name, mode, IngestOption.targetDbSchema(s))
      case None => connection.bulkIngest(name, mode)
    }
}

class AdbcWriteBuilder(schema: StructType, options: AdbcOptions, target: AdbcWriteTarget)
  extends WriteBuilder with SupportsTruncate {

  private var truncateTable = false

  override def truncate(): WriteBuilder = {
    truncateTable = true
    this
  }

  override def build(): Write = {
    val timeZone = SQLConf.get.sessionLocalTimeZone
    new AdbcWrite(schema, options, target, truncateTable, timeZone)
  }
}

/**
 * Appends the rows of each task to the table with the driver's bulk ingestion. Each task writes
 * in its own transaction, if the driver supports transactions, and commits it when it's done.
 * There is no transaction across tasks: if the job fails, the rows of the tasks that committed
 * stay in the table. To overwrite the table, its rows are deleted before the tasks start, as the
 * JDBC data source truncates it, so a query that reads the table itself reads no rows.
 */
class AdbcWrite(
    schema: StructType,
    options: AdbcOptions,
    target: AdbcWriteTarget,
    truncate: Boolean,
    timeZone: String)
  extends Write with BatchWrite {

  override def toBatch: BatchWrite = this

  override def description(): String = s"ADBC write to ${target.sqlName}"

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    // Called once, before the tasks start.
    if (truncate) {
      AdbcUtils.withConnection(options) { conn =>
        Using.resource(conn.createStatement()) { stmt =>
          stmt.setSqlQuery(s"DELETE FROM ${target.sqlName}")
          stmt.executeUpdate()
        }
      }
    }
    AdbcDataWriterFactory(schema, options, target, timeZone)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  // Committed tasks can't be undone, and the others have rolled back.
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

case class AdbcDataWriterFactory(
    schema: StructType,
    options: AdbcOptions,
    target: AdbcWriteTarget,
    timeZone: String)
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new AdbcDataWriter(schema, options, target, timeZone)
}

case object AdbcWriterCommitMessage extends WriterCommitMessage

/** Buffers rows in Arrow vectors, and ingests them every `batchSize` rows. */
class AdbcDataWriter(
    schema: StructType,
    options: AdbcOptions,
    target: AdbcWriteTarget,
    timeZone: String)
  extends DataWriter[InternalRow] {

  private val allocator: BufferAllocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"ADBC writer for ${options.driver}", 0, Long.MaxValue)
  private var database: AdbcDatabase = _
  private var connection: AdbcConnection = _
  private var root: VectorSchemaRoot = _
  private var arrowWriter: ArrowWriter = _
  private var transactional = false
  private var rowsInBatch = 0

  try {
    database = AdbcUtils.openDatabase(options, allocator)
    connection = database.connect()
    transactional = try {
      connection.setAutoCommit(false)
      true
    } catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED => false
    }
    val arrowSchema = ArrowUtils.toArrowSchema(
      schema, timeZone, errorOnDuplicatedFieldNames = true, largeVarTypes = false)
    root = VectorSchemaRoot.create(arrowSchema, allocator)
    arrowWriter = ArrowWriter.create(root)
  } catch {
    case e: Throwable =>
      close()
      throw e
  }

  override def write(record: InternalRow): Unit = {
    arrowWriter.write(record)
    rowsInBatch += 1
    if (rowsInBatch >= options.batchSize) flush()
  }

  private def flush(): Unit = {
    if (rowsInBatch > 0) {
      arrowWriter.finish()
      Using.resource(target.bulkIngest(connection, BulkIngestMode.APPEND)) { stmt =>
        stmt.bind(root)
        stmt.executeUpdate()
      }
      arrowWriter.reset()
      rowsInBatch = 0
    }
  }

  override def commit(): WriterCommitMessage = {
    flush()
    if (transactional) connection.commit()
    AdbcWriterCommitMessage
  }

  override def abort(): Unit = {
    if (transactional) connection.rollback()
  }

  override def close(): Unit = {
    Utils.closeQuietly(root)
    Utils.closeQuietly(connection)
    Utils.closeQuietly(database)
    AdbcUtils.closeAllocator(allocator, options.driver)
  }
}
