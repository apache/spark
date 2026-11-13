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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, DatabaseMetaData, PreparedStatement, SQLException}

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import org.apache.spark.{SparkFunSuite, SparkRuntimeException, SparkSQLException, SparkThrowable, TaskContext}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.jdbc.{JdbcDialect, NoLegacyJDBCError}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/** Tests destination write classification and propagation of upstream JDBC failures. */
class JdbcWriteExceptionSuite extends SparkFunSuite {

  private val testJdbcUrl = "jdbc:write:test"

  private def newWriteOptions(extra: Map[String, String] = Map.empty): JdbcOptionsInWrite = {
    new JdbcOptionsInWrite(CaseInsensitiveMap(Map(
      "url" -> testJdbcUrl,
      "dbtable" -> "t",
      "driver" -> "org.h2.Driver"
    ) ++ extra))
  }

  private class WriteFixture {
    val conn: Connection = mock(classOf[Connection])
    val stmt: PreparedStatement = mock(classOf[PreparedStatement])
    val context = TaskContext.empty()
    val options: JdbcOptionsInWrite = newWriteOptions()
    when(conn.prepareStatement(anyString())).thenReturn(stmt)

    // savePartition receives its dialect directly; no global dialect registration is needed.
    val dialect: JdbcDialect = new JdbcDialect with NoLegacyJDBCError {
      override def canHandle(url: String): Boolean = url == testJdbcUrl
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
    }

    def enableTransactions(): Unit = {
      val metadata = mock(classOf[DatabaseMetaData])
      when(conn.getMetaData).thenReturn(metadata)
      when(metadata.supportsTransactions()).thenReturn(true)
      when(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED))
        .thenReturn(true)
    }

    def savePartition(
        iterator: Iterator[Row] = Iterator(Row(1)),
        dialect: JdbcDialect = this.dialect,
        isolationLevel: Int = Connection.TRANSACTION_NONE,
        options: JDBCOptions = this.options): Unit = {
      JdbcUtils.savePartition(
        "t", iterator, StructType(Seq(StructField("a", IntegerType))),
        "INSERT INTO t (a) VALUES (?)", 1, dialect, isolationLevel, options)
    }
  }

  private def withWriteFixture[T](f: WriteFixture => T): T = {
    val fixture = new WriteFixture
    TaskContext.setTaskContext(fixture.context)
    try {
      f(fixture)
    } finally {
      TaskContext.unset()
    }
  }

  private def savePartitionWithSourceError(iterator: Iterator[Row]): SQLException = {
    withWriteFixture { fixture =>
      intercept[SQLException] {
        fixture.savePartition(iterator)
      }
    }
  }

  test("savePartition does not classify a later source hasNext failure as a JDBC write") {
    val sourceFailure = new SQLException("source read failed", "08006")
    val nextFailure = new SQLException("source connection detail", "08001")
    sourceFailure.setNextException(nextFailure)
    val iterator = new Iterator[Row] {
      private var readFirstRow = false
      override def hasNext: Boolean = {
        if (readFirstRow) throw sourceFailure
        true
      }
      override def next(): Row = {
        readFirstRow = true
        Row(1)
      }
    }

    assert(savePartitionWithSourceError(iterator) eq sourceFailure)
    assert(sourceFailure.getSQLState == "08006")
    assert(sourceFailure.getNextException eq nextFailure)
    assert(sourceFailure.getCause eq nextFailure)
  }

  test("savePartition preserves a classified later source next failure") {
    val sourceFailure = new SparkSQLException(
      "FAILED_JDBC.CONNECTION",
      Map("url" -> "jdbc:source"),
      Some("08001"))
    val iterator = new Iterator[Row] {
      private var rowsRead = 0
      override def hasNext: Boolean = rowsRead < 2
      override def next(): Row = {
        rowsRead += 1
        if (rowsRead == 1) Row(1) else throw sourceFailure
      }
    }

    val error = savePartitionWithSourceError(iterator)
    assert(error eq sourceFailure)
    checkError(
      exception = sourceFailure,
      condition = "FAILED_JDBC.CONNECTION",
      sqlState = Some("08001"),
      parameters = Map("url" -> "jdbc:source"))
  }

  test("savePartition classifies JDBC row binding failures") {
    withWriteFixture { fixture =>
      import fixture._
      val bindingFailure = new SQLException("binding rejected", "22001")
      doThrow(bindingFailure).when(stmt).setInt(anyInt(), anyInt())

      val error = intercept[SparkRuntimeException] {
        savePartition()
      }
      checkError(
        exception = error,
        condition = "FAILED_JDBC.WRITE",
        sqlState = Some("HV000"),
        parameters = Map("url" -> options.getRedactUrl(), "tableName" -> "t"))
      assert(error.getCause eq bindingFailure)
      verify(stmt, never()).executeBatch()
    }
  }

  test("savePartition classifies JDBC batch write failures") {
    withWriteFixture { fixture =>
      import fixture._
      val batchFailure = new SQLException("batch rejected", "23000", 2627)
      val nextFailure = new SQLException("duplicate key detail")
      batchFailure.setNextException(nextFailure)
      when(stmt.executeBatch()).thenThrow(batchFailure)

      val error = intercept[SparkRuntimeException] {
        savePartition()
      }
      checkError(
        exception = error,
        condition = "FAILED_JDBC.WRITE",
        sqlState = Some("HV000"),
        parameters = Map("url" -> options.getRedactUrl(), "tableName" -> "t"))
      assert(error.getCause eq batchFailure)
      assert(batchFailure.getNextException eq nextFailure)
      assert(batchFailure.getCause eq nextFailure)
    }
  }

  test("savePartition classifies JDBC commit failures") {
    withWriteFixture { fixture =>
      import fixture._
      enableTransactions()
      val commitFailure = new SQLException("commit rejected", "40001")
      doThrow(commitFailure).when(conn).commit()

      val error = intercept[SparkRuntimeException] {
        savePartition(isolationLevel = Connection.TRANSACTION_READ_COMMITTED)
      }
      checkError(
        exception = error,
        condition = "FAILED_JDBC.WRITE",
        sqlState = Some("HV000"),
        parameters = Map("url" -> options.getRedactUrl(), "tableName" -> "t"))
      assert(error.getCause eq commitFailure)
      verify(conn).rollback()
    }
  }

  test("savePartition preserves an unclassified legacy JDBC write failure") {
    withWriteFixture { fixture =>
      import fixture._
      val batchFailure = new SQLException("batch rejected", "23000", 2627)
      when(stmt.executeBatch()).thenThrow(batchFailure)
      val legacyDialect = new JdbcDialect {
        override def canHandle(url: String): Boolean = url == testJdbcUrl
        override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
      }

      val error = intercept[SQLException] {
        savePartition(dialect = legacyDialect)
      }
      assert(error eq batchFailure)
      assert(error.getSQLState == "23000")
    }
  }

  gridTest("savePartition preserves specific dialect mappings")(Seq(true, false)) { legacy =>
    withWriteFixture { fixture =>
      import fixture._
      val batchFailure = new SQLException("table already exists", "42P07")
      when(stmt.executeBatch()).thenThrow(batchFailure)
      val mappedFailure = new AnalysisException(
        errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
        messageParameters = Map("relationName" -> "`t`"),
        cause = Some(batchFailure))
      val mappedDialect = new JdbcDialect {
        override def canHandle(url: String): Boolean = url == testJdbcUrl
        override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
        override def classifyException(message: String, e: Throwable): AnalysisException = {
          mappedFailure
        }
        override def classifyException(
            e: Throwable,
            condition: String,
            messageParameters: Map[String, String],
            description: String,
            isRuntime: Boolean): Throwable with SparkThrowable = {
          if (legacy) {
            super.classifyException(e, condition, messageParameters, description, isRuntime)
          } else {
            mappedFailure
          }
        }
      }
      val error = intercept[AnalysisException] {
        savePartition(dialect = mappedDialect)
      }
      assert(error eq mappedFailure)
      assert(error.getCause eq batchFailure)
    }
  }

  gridTest("savePartition handles connection acquisition failures")(
      for (stage <- Seq("factory", "connect"); kind <- Seq("plain", "interrupted", "classified"))
        yield (stage, kind)) { case (stage, kind) =>
    withWriteFixture { fixture =>
      import fixture._
      val connectionFailure = if (kind == "classified") {
        new SparkSQLException("FAILED_JDBC.CONNECTION", Map("url" -> "jdbc:destination"),
          Some("08001"))
      } else {
        new SQLException("connection rejected", "08001")
      }
      val nextFailure = new SQLException("connection detail")
      connectionFailure.setNextException(nextFailure)
      val failingDialect = new JdbcDialect with NoLegacyJDBCError {
        override def canHandle(url: String): Boolean = url.startsWith(testJdbcUrl)
        override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
          if (kind == "interrupted") context.markInterrupted("test")
          if (stage == "factory") throw connectionFailure
          _ => throw connectionFailure
        }
      }
      val options = newWriteOptions(Map("url" -> s"$testJdbcUrl?password=secret"))
      val error = intercept[Exception] {
        savePartition(dialect = failingDialect, options = options)
      }
      if (kind == "plain") {
        checkError(
          exception = error.asInstanceOf[SparkRuntimeException],
          condition = "FAILED_JDBC.WRITE",
          sqlState = Some("HV000"),
          parameters = Map("url" -> options.getRedactUrl(), "tableName" -> "t"))
        assert(!error.getMessage.contains("secret"))
        assert(error.getCause eq connectionFailure)
      } else {
        assert(error eq connectionFailure)
      }
      assert(connectionFailure.getNextException eq nextFailure)
      assert(connectionFailure.getCause eq nextFailure)
    }
  }

  gridTest("savePartition classifies JDBC write setup failures")(
      Seq("autoCommit", "isolation", "prepareStatement", "queryTimeout")) { operation =>
    withWriteFixture { fixture =>
      import fixture._
      enableTransactions()
      val setupFailure = new SQLException("write setup rejected", "08006")
      operation match {
        case "autoCommit" => doThrow(setupFailure).when(conn).setAutoCommit(false)
        case "isolation" =>
          doThrow(setupFailure).when(conn)
            .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
        case "prepareStatement" => when(conn.prepareStatement(anyString())).thenThrow(setupFailure)
        case "queryTimeout" => doThrow(setupFailure).when(stmt).setQueryTimeout(anyInt())
      }
      val error = intercept[SparkRuntimeException] {
        savePartition(isolationLevel = Connection.TRANSACTION_READ_COMMITTED)
      }
      checkError(
        exception = error,
        condition = "FAILED_JDBC.WRITE",
        sqlState = Some("HV000"),
        parameters = Map("url" -> options.getRedactUrl(), "tableName" -> "t"))
      assert(error.getCause eq setupFailure)
      verify(conn).rollback()
      verify(conn).close()
      verify(stmt, never()).executeBatch()
      verify(conn, never()).commit()
      if (operation == "queryTimeout") verify(stmt).close()
    }
  }

  test("savePartition preserves a plain source next failure and its chained exception") {
    val sourceFailure = new SQLException("source next failed", "08006")
    val nextFailure = new SQLException("source connection detail", "08001")
    sourceFailure.setNextException(nextFailure)
    val iterator = new Iterator[Row] {
      private var rowsRead = 0
      override def hasNext: Boolean = rowsRead < 2
      override def next(): Row = {
        rowsRead += 1
        if (rowsRead == 1) Row(1) else throw sourceFailure
      }
    }

    assert(savePartitionWithSourceError(iterator) eq sourceFailure)
    assert(sourceFailure.getSQLState == "08006")
    assert(sourceFailure.getNextException eq nextFailure)
    assert(sourceFailure.getCause eq nextFailure)
  }

  gridTest("statement cleanup preserves the write failure")(Seq("binding", "batch")) { operation =>
    withWriteFixture { fixture =>
      import fixture._
      enableTransactions()
      val writeFailure = new SQLException("write rejected", "23000")
      if (operation == "binding") {
        doThrow(writeFailure).when(stmt).setInt(anyInt(), anyInt())
      } else {
        when(stmt.executeBatch()).thenThrow(writeFailure)
      }
      doThrow(new SQLException("statement close failed", "08006")).when(stmt).close()
      val error = intercept[SparkRuntimeException] {
        savePartition(isolationLevel = Connection.TRANSACTION_READ_COMMITTED)
      }
      checkError(
        exception = error,
        condition = "FAILED_JDBC.WRITE",
        sqlState = Some("HV000"),
        parameters = Map("url" -> options.getRedactUrl(), "tableName" -> "t"))
      assert(error.getCause eq writeFailure)
      verify(conn).rollback()
      verify(conn, never()).commit()
      verify(stmt).close()
    }
  }

  test("connection cleanup preserves the write failure") {
    withWriteFixture { fixture =>
      import fixture._
      enableTransactions()
      val writeFailure = new SQLException("write rejected", "23000")
      when(stmt.executeBatch()).thenThrow(writeFailure)
      doThrow(new SQLException("connection close failed", "08006")).when(conn).close()

      val error = intercept[SparkRuntimeException] {
        savePartition(isolationLevel = Connection.TRANSACTION_READ_COMMITTED)
      }
      checkError(
        exception = error,
        condition = "FAILED_JDBC.WRITE",
        sqlState = Some("HV000"),
        parameters = Map("url" -> options.getRedactUrl(), "tableName" -> "t"))
      assert(error.getCause eq writeFailure)
      verify(conn).rollback()
      verify(conn).close()
    }
  }

  test("statement cleanup does not abort a successful write before commit") {
    withWriteFixture { fixture =>
      import fixture._
      enableTransactions()
      doThrow(new SQLException("statement close failed", "08006")).when(stmt).close()
      savePartition(isolationLevel = Connection.TRANSACTION_READ_COMMITTED)
      verify(stmt).executeBatch()
      verify(stmt).close()
      verify(conn).commit()
      verify(conn, never()).rollback()
      assert(context.taskMetrics.outputMetrics.recordsWritten == 1)
    }
  }
}
