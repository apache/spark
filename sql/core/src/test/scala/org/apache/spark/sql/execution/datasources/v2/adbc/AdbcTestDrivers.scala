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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcDatabase, AdbcDriver, AdbcException, AdbcStatement, BulkIngestMode, IngestOption, PartitionDescriptor, StandardSchemas}
import org.apache.arrow.adbc.driver.jdbc.{JdbcDriver, JdbcDriverFactory, JdbcQuirks}
import org.apache.arrow.adbc.drivermanager.AdbcDriverFactory
import org.apache.arrow.adbc.sql.SqlQuirks
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{UInt4Vector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.ipc.{ArrowReader, ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo.Schema

/**
 * Fake ADBC drivers for tests. Each wraps the JDBC bridge driver and changes how it behaves, to
 * act like other drivers.
 */
abstract class WrappingDriverFactory extends AdbcDriverFactory {
  protected def wrap(connection: AdbcConnection, allocator: BufferAllocator): AdbcConnection =
    connection

  /** Database parameters passed to the JDBC bridge driver, in addition to the given ones. */
  protected def extraParameters: Map[String, AnyRef] = Map.empty

  override def getDriver(allocator: BufferAllocator): AdbcDriver = {
    val jdbc = new JdbcDriverFactory().getDriver(allocator)
    new AdbcDriver {
      override def open(parameters: util.Map[String, AnyRef]): AdbcDatabase = {
        val database = jdbc.open((parameters.asScala ++ extraParameters).asJava)
        new AdbcDatabase {
          override def connect(): AdbcConnection = wrap(database.connect(), allocator)
          override def close(): Unit = database.close()
        }
      }
    }
  }
}

private object WrappingDriverFactory {
  /** Writes the batches that `write` produces as an Arrow stream, and returns its reader. */
  def toReader(allocator: BufferAllocator, root: VectorSchemaRoot)(write: (() => Unit) => Unit)
      : ArrowReader = {
    val out = new ByteArrayOutputStream()
    Using.resource(new ArrowStreamWriter(root, null, out)) { writer =>
      writer.start()
      write(() => writer.writeBatch())
      writer.end()
    }
    new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray), allocator)
  }
}

/** Delegates every call to another connection. */
class WrappingConnection(delegate: AdbcConnection) extends AdbcConnection {
  override def createStatement(): AdbcStatement = delegate.createStatement()
  override def getInfo(infoCodes: Array[Int]): ArrowReader = delegate.getInfo(infoCodes)
  override def getObjects(
      depth: AdbcConnection.GetObjectsDepth,
      catalogPattern: String,
      dbSchemaPattern: String,
      tableNamePattern: String,
      tableTypes: Array[String],
      columnNamePattern: String): ArrowReader = delegate.getObjects(
    depth, catalogPattern, dbSchemaPattern, tableNamePattern, tableTypes, columnNamePattern)
  override def bulkIngest(targetTableName: String, mode: BulkIngestMode): AdbcStatement =
    delegate.bulkIngest(targetTableName, mode)
  override def bulkIngest(
      targetTableName: String, mode: BulkIngestMode, options: IngestOption*): AdbcStatement =
    delegate.bulkIngest(targetTableName, mode, options: _*)
  override def readPartition(descriptor: ByteBuffer): ArrowReader =
    delegate.readPartition(descriptor)
  override def getCurrentCatalog: String = delegate.getCurrentCatalog
  override def getCurrentDbSchema: String = delegate.getCurrentDbSchema
  override def getAutoCommit: Boolean = delegate.getAutoCommit
  override def setAutoCommit(enableAutoCommit: Boolean): Unit =
    delegate.setAutoCommit(enableAutoCommit)
  override def commit(): Unit = delegate.commit()
  override def rollback(): Unit = delegate.rollback()
  override def cancel(): Unit = delegate.cancel()
  override def close(): Unit = delegate.close()
}

/** Delegates every call to another statement. */
class WrappingStatement(delegate: AdbcStatement) extends AdbcStatement {
  override def setSqlQuery(query: String): Unit = delegate.setSqlQuery(query)
  override def bind(root: VectorSchemaRoot): Unit = delegate.bind(root)
  override def executeQuery(): AdbcStatement.QueryResult = delegate.executeQuery()
  override def executeSchema(): Schema = delegate.executeSchema()
  override def executeUpdate(): AdbcStatement.UpdateResult = delegate.executeUpdate()
  override def executePartitioned(): AdbcStatement.PartitionResult = delegate.executePartitioned()
  override def prepare(): Unit = delegate.prepare()
  override def cancel(): Unit = delegate.cancel()
  override def close(): Unit = delegate.close()
}

/**
 * Reports `"` for SQL_IDENTIFIER_QUOTE_CHAR, like drivers for Flight SQL servers do. The JDBC
 * bridge driver doesn't report it.
 */
class QuoteReportingDriverFactory extends WrappingDriverFactory {
  override protected def wrap(conn: AdbcConnection, allocator: BufferAllocator): AdbcConnection =
    new QuoteReportingConnection(conn, allocator)
}

private class QuoteReportingConnection(delegate: AdbcConnection, allocator: BufferAllocator)
  extends WrappingConnection(delegate) {

  override def getInfo(infoCodes: Array[Int]): ArrowReader = {
    if (!infoCodes.contains(AdbcUtils.SQL_IDENTIFIER_QUOTE_CHAR)) {
      return delegate.getInfo(infoCodes)
    }
    QuoteReportingConnection.quoteLookups.incrementAndGet()
    Using.resource(VectorSchemaRoot.create(StandardSchemas.GET_INFO_SCHEMA, allocator)) { root =>
      root.getVector(0).asInstanceOf[UInt4Vector].setSafe(0, AdbcUtils.SQL_IDENTIFIER_QUOTE_CHAR)
      val values = root.getVector(1).asInstanceOf[DenseUnionVector]
      val stringTypeId: Byte = 0
      val strings = values.getVarCharVector(stringTypeId)
      values.setValueCount(1)
      values.setTypeId(0, stringTypeId)
      values.getOffsetBuffer.setInt(0, 0)
      strings.setSafe(0, "\"".getBytes(StandardCharsets.UTF_8))
      strings.setValueCount(1)
      root.setRowCount(1)
      WrappingDriverFactory.toReader(allocator, root)(writeBatch => writeBatch())
    }
  }
}

private object QuoteReportingConnection {
  val quoteLookups = new AtomicInteger(0)
}

/** Fails to report the identifier quote string. */
class FailingInfoDriverFactory extends WrappingDriverFactory {
  override protected def wrap(conn: AdbcConnection, allocator: BufferAllocator): AdbcConnection =
    new WrappingConnection(conn) {
      override def getInfo(infoCodes: Array[Int]): ArrowReader =
        throw AdbcException.io("connection lost")
    }
}

/** Doesn't implement `executeSchema`, so the schema is taken from a query's result. */
class NoExecuteSchemaDriverFactory extends WrappingDriverFactory {
  override protected def wrap(conn: AdbcConnection, allocator: BufferAllocator): AdbcConnection =
    new WrappingConnection(conn) {
      override def createStatement(): AdbcStatement =
        new WrappingStatement(conn.createStatement()) {
          override def setSqlQuery(query: String): Unit = {
            NoExecuteSchemaDriverFactory.queries.add(query)
            super.setSqlQuery(query)
          }
          override def executeSchema(): Schema =
            throw AdbcException.notImplemented("executeSchema")
        }
    }
}

object NoExecuteSchemaDriverFactory {
  val queries = new util.concurrent.ConcurrentLinkedQueue[String]()
}

/**
 * Implements `executePartitioned`, splitting each result into two partitions. A descriptor is
 * the SQL query that reads its partition.
 */
class PartitioningDriverFactory extends WrappingDriverFactory {
  override protected def wrap(conn: AdbcConnection, allocator: BufferAllocator): AdbcConnection =
    new PartitioningConnection(conn, allocator)
}

private class PartitioningConnection(delegate: AdbcConnection, allocator: BufferAllocator)
  extends WrappingConnection(delegate) {

  override def createStatement(): AdbcStatement = {
    val statement = delegate.createStatement()
    new WrappingStatement(statement) {
      private var query: String = _
      override def setSqlQuery(query: String): Unit = {
        this.query = query
        statement.setSqlQuery(query)
      }
      override def executePartitioned(): AdbcStatement.PartitionResult = {
        PartitioningConnection.executions.incrementAndGet()
        val descriptors = Seq(
          s"SELECT * FROM ($query) P OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY",
          s"SELECT * FROM ($query) P OFFSET 1 ROWS").map { q =>
          new PartitionDescriptor(ByteBuffer.wrap(q.getBytes(StandardCharsets.UTF_8)))
        }
        new AdbcStatement.PartitionResult(statement.executeSchema(), -1, descriptors.asJava)
      }
    }
  }

  override def readPartition(descriptor: ByteBuffer): ArrowReader = {
    val query = StandardCharsets.UTF_8.decode(descriptor).toString
    Using.resource(delegate.createStatement()) { statement =>
      statement.setSqlQuery(query)
      Using.resource(statement.executeQuery()) { result =>
        val reader = result.getReader
        WrappingDriverFactory.toReader(allocator, reader.getVectorSchemaRoot) { writeBatch =>
          while (reader.loadNextBatch()) writeBatch()
        }
      }
    }
  }
}

private object PartitioningConnection {
  val executions = new AtomicInteger(0)
}

/**
 * Blocks in `executeQuery` until the statement is cancelled, and in `readPartition` until the
 * connection is cancelled. `executePartitioned` returns a single partition.
 */
class BlockingDriverFactory extends WrappingDriverFactory {
  override protected def wrap(conn: AdbcConnection, allocator: BufferAllocator): AdbcConnection =
    new WrappingConnection(conn) {
      override def createStatement(): AdbcStatement =
        new WrappingStatement(conn.createStatement()) {
          override def executeQuery(): AdbcStatement.QueryResult = BlockingDriverFactory.block()
          override def executePartitioned(): AdbcStatement.PartitionResult = {
            val descriptor = new PartitionDescriptor(ByteBuffer.wrap(Array[Byte](0)))
            new AdbcStatement.PartitionResult(executeSchema(), -1, util.List.of(descriptor))
          }
          override def cancel(): Unit = BlockingDriverFactory.cancelled.countDown()
        }
      override def readPartition(descriptor: ByteBuffer): ArrowReader =
        BlockingDriverFactory.block()
      override def cancel(): Unit = BlockingDriverFactory.cancelled.countDown()
    }
}

object BlockingDriverFactory {
  @volatile var executing = new CountDownLatch(1)
  @volatile var cancelled = new CountDownLatch(1)

  def reset(): Unit = {
    executing = new CountDownLatch(1)
    cancelled = new CountDownLatch(1)
  }

  def block(): Nothing = {
    executing.countDown()
    if (!cancelled.await(1, TimeUnit.MINUTES)) {
      throw AdbcException.invalidState("the query was not cancelled")
    }
    throw AdbcException.io("the query was cancelled")
  }
}

/**
 * The JDBC bridge driver, configured to create tables in H2 with bulk ingestion. Like native
 * drivers, it quotes the table name of an ingestion, and takes its schema as an option.
 */
class H2DriverFactory extends WrappingDriverFactory {
  override protected def extraParameters: Map[String, AnyRef] = Map(
    JdbcDriver.PARAM_JDBC_QUIRKS -> JdbcQuirks.builder("H2")
      .sqlQuirks(SqlQuirks.builder().build())
      .build())

  override protected def wrap(conn: AdbcConnection, allocator: BufferAllocator): AdbcConnection =
    new WrappingConnection(conn) {
      override def bulkIngest(targetTableName: String, mode: BulkIngestMode): AdbcStatement =
        bulkIngest(targetTableName, mode, Seq.empty: _*)

      override def bulkIngest(
          targetTableName: String, mode: BulkIngestMode, options: IngestOption*): AdbcStatement = {
        val schema = options.collectFirst {
          case o: IngestOption.TargetNamespaceIngestOption => o.getTargetDbSchema
        }
        // The JDBC bridge driver puts the name into its SQL unchanged.
        val name = (schema.toSeq :+ targetTableName)
          .map(AdbcUtils.quoteIdentifier(_, "\"")).mkString(".")
        conn.bulkIngest(name, mode)
      }
    }
}

/** Reports a schema with a type Spark can't read, set in the companion object. */
class UnreadableSchemaDriverFactory extends WrappingDriverFactory {
  override protected def wrap(conn: AdbcConnection, allocator: BufferAllocator): AdbcConnection =
    new WrappingConnection(conn) {
      override def createStatement(): AdbcStatement =
        new WrappingStatement(conn.createStatement()) {
          override def executeSchema(): Schema = UnreadableSchemaDriverFactory.schema
        }
    }
}

object UnreadableSchemaDriverFactory {
  @volatile var schema: Schema = _
}

/** Not a valid driver: it has no public no-argument constructor. */
class NoDefaultConstructorDriverFactory(name: String) extends AdbcDriverFactory {
  override def getDriver(allocator: BufferAllocator): AdbcDriver =
    throw new UnsupportedOperationException(name)
}
