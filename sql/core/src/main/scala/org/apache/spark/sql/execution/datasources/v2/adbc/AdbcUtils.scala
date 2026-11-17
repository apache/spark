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

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcDatabase, AdbcException, AdbcStatusCode}
import org.apache.arrow.adbc.drivermanager.AdbcDriverFactory
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.UInt4Vector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.errors.{ExecutionErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

private[adbc] object AdbcUtils extends Logging {

  // Flight SQL SqlInfo code for the string used to quote SQL identifiers.
  val SQL_IDENTIFIER_QUOTE_CHAR = 504

  /**
   * Opens a database with the driver named by `options`. The driver factory is loaded through
   * Spark's class loader, so drivers added with `--jars` or `--packages` are found.
   */
  def openDatabase(options: AdbcOptions, allocator: BufferAllocator): AdbcDatabase = {
    val factory = newDriverFactory(options.driver)
    val params: Map[String, AnyRef] = options.databaseParameters
    factory.getDriver(allocator).open(params.asJava)
  }

  private def newDriverFactory(driver: String): AdbcDriverFactory = {
    val cls = try {
      Utils.classForName[AnyRef](driver)
    } catch {
      case e: ClassNotFoundException =>
        throw QueryExecutionErrors.adbcDriverClassNotFoundError(driver, e)
    }
    val factoryClass = classOf[AdbcDriverFactory]
    if (!factoryClass.isAssignableFrom(cls)) {
      throw QueryExecutionErrors.adbcInvalidDriverClassError(driver, factoryClass.getName)
    }
    val constructor = try {
      cls.getConstructor()
    } catch {
      case _: NoSuchMethodException =>
        throw QueryExecutionErrors.adbcInvalidDriverClassError(driver, factoryClass.getName)
    }
    constructor.newInstance().asInstanceOf[AdbcDriverFactory]
  }

  /** Runs `f` with a new connection, closing it and its database afterwards. */
  def withConnection[T](options: AdbcOptions)(f: AdbcConnection => T): T = {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"ADBC planning connection for ${options.driver}", 0, Long.MaxValue)
    try {
      Using.resource(openDatabase(options, allocator)) { database =>
        Using.resource(database.connect())(f)
      }
    } finally {
      closeAllocator(allocator, options.driver)
    }
  }

  /**
   * Closes an allocator after its driver's resources are closed. This fails if memory is still
   * allocated, e.g. leaked by the driver. Log it rather than failing a query whose work is
   * already complete.
   */
  def closeAllocator(allocator: BufferAllocator, driver: String): Unit = {
    try {
      allocator.close()
    } catch {
      case e: IllegalStateException =>
        logWarning(log"Arrow memory was not released after using the ADBC driver " +
          log"${MDC(LogKeys.CLASS_NAME, driver)}", e)
    }
  }

  /** Returns the relation's schema, as reported by the driver for a query of all its columns. */
  def getSchema(options: AdbcOptions): StructType = withConnection(options) { conn =>
    // Only a table is quoted, and it always has a quote string.
    val quote = if (options.table.isDefined) {
      options.quoteChar(getIdentifierQuoteChar(conn)).get
    } else {
      ""
    }
    val schema = try {
      Using.resource(conn.createStatement()) { stmt =>
        stmt.setSqlQuery(options.selectAllQuery(quote))
        stmt.executeSchema()
      }
    } catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED =>
        // Run a query that returns no rows, and take the schema of its result.
        Using.resource(conn.createStatement()) { stmt =>
          stmt.setSqlQuery(s"SELECT * FROM ${options.fromClause(quote)} WHERE 1 = 0")
          Using.resource(stmt.executeQuery())(_.getReader.getVectorSchemaRoot.getSchema)
        }
    }
    schema.getFields.asScala.foreach(checkReadable)
    val sparkSchema = ArrowUtils.fromArrowSchema(schema)
    // Fails for preview types, such as TIME and nanosecond timestamps, unless they're enabled.
    TypeUtils.failUnsupportedDataType(sparkSchema, SQLConf.get)
    sparkSchema
  }

  /**
   * Fails for a column that maps to a Spark type, but that Spark can't read: decimals of other
   * widths than 128 bits, and dictionary-encoded columns, whose values are read as indices.
   */
  private def checkReadable(field: Field): Unit = {
    field.getType match {
      case d: ArrowType.Decimal if d.getBitWidth != 128 =>
        throw ExecutionErrors.unsupportedArrowTypeError(d)
      case _ =>
    }
    if (field.getDictionary != null) {
      throw QueryExecutionErrors.adbcDictionaryEncodedError(field.getType.toString)
    }
    field.getChildren.asScala.foreach(checkReadable)
  }

  /**
   * Executes `query` with `executePartitioned` and returns its partition descriptors, or None if
   * the driver doesn't implement it.
   */
  def executePartitioned(conn: AdbcConnection, query: String): Option[Seq[Array[Byte]]] = {
    try {
      Using.resource(conn.createStatement()) { stmt =>
        stmt.setSqlQuery(query)
        val descriptors = stmt.executePartitioned().getPartitionDescriptors.asScala.toSeq
        Some(descriptors.map { d =>
          val buffer = d.getDescriptor.duplicate()
          val bytes = new Array[Byte](buffer.remaining())
          buffer.get(bytes)
          bytes
        })
      }
    } catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED => None
    }
  }

  /**
   * Returns the string the driver reports for quoting identifiers, or None if it doesn't
   * report one. A failure to get it is logged, since a default can be used instead.
   */
  def getIdentifierQuoteChar(conn: AdbcConnection): Option[String] = {
    try {
      Using.resource(conn.getInfo(Array(SQL_IDENTIFIER_QUOTE_CHAR))) { reader =>
        var quote: Option[String] = None
        while (quote.isEmpty && reader.loadNextBatch()) {
          val root = reader.getVectorSchemaRoot
          val names = root.getVector("info_name").asInstanceOf[UInt4Vector]
          val values = root.getVector("info_value")
          (0 until root.getRowCount)
            .find(i => !names.isNull(i) && names.get(i) == SQL_IDENTIFIER_QUOTE_CHAR)
            .foreach(i => quote = Option(values.getObject(i)).map(_.toString))
        }
        // JDBC reports a space when identifiers can't be quoted; drivers bridging it may too.
        quote.filter(_.trim.nonEmpty)
      }
    } catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED => None
      case e: AdbcException =>
        logWarning("Failed to get the identifier quote string from the ADBC driver.", e)
        None
    }
  }

  /** Quotes `name` with `quote`, doubling any occurrences of `quote` inside it. */
  def quoteIdentifier(name: String, quote: String): String =
    quote + name.replace(quote, quote + quote) + quote
}
