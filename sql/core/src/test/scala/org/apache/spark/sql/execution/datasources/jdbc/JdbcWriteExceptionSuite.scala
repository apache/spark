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

import org.apache.spark.{SparkFunSuite, SparkRuntimeException, SparkSQLException, TaskContext}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, NoLegacyJDBCError}
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

  private def registerDialect(dialect: JdbcDialect): Unit = {
    JdbcDialects.unregisterDialect(dialect)
    JdbcDialects.registerDialect(dialect)
  }

  private def savePartitionWithSourceError(iterator: Iterator[Row]): SQLException = {
    val conn = mock(classOf[Connection])
    val stmt = mock(classOf[PreparedStatement])
    when(conn.prepareStatement(anyString())).thenReturn(stmt)

    val dialect = new JdbcDialect with NoLegacyJDBCError {
      override def canHandle(url: String): Boolean = url == testJdbcUrl
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
    }
    registerDialect(dialect)
    TaskContext.setTaskContext(TaskContext.empty())
    try {
      intercept[SQLException] {
        JdbcUtils.savePartition(
          "t",
          iterator,
          StructType(Seq(StructField("a", IntegerType))),
          "INSERT INTO t (a) VALUES (?)",
          batchSize = 1,
          dialect,
          Connection.TRANSACTION_NONE,
          newWriteOptions())
      }
    } finally {
      TaskContext.unset()
      JdbcDialects.unregisterDialect(dialect)
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
    val conn = mock(classOf[Connection])
    val stmt = mock(classOf[PreparedStatement])
    when(conn.prepareStatement(anyString())).thenReturn(stmt)
    val bindingFailure = new SQLException("binding rejected", "22001")
    doThrow(bindingFailure).when(stmt).setInt(anyInt(), anyInt())

    val dialect = new JdbcDialect with NoLegacyJDBCError {
      override def canHandle(url: String): Boolean = url == testJdbcUrl
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
    }
    registerDialect(dialect)
    TaskContext.setTaskContext(TaskContext.empty())
    try {
      val options = newWriteOptions()
      val error = intercept[SparkRuntimeException] {
        JdbcUtils.savePartition(
          "t",
          Iterator(Row(1)),
          StructType(Seq(StructField("a", IntegerType))),
          "INSERT INTO t (a) VALUES (?)",
          batchSize = 1,
          dialect,
          Connection.TRANSACTION_NONE,
          options)
      }
      checkError(
        exception = error,
        condition = "FAILED_JDBC.WRITE",
        sqlState = Some("HV000"),
        parameters = Map("url" -> options.getRedactUrl(), "tableName" -> "t"))
      assert(error.getCause eq bindingFailure)
      verify(stmt, never()).executeBatch()
    } finally {
      TaskContext.unset()
      JdbcDialects.unregisterDialect(dialect)
    }
  }

  test("savePartition classifies JDBC batch write failures") {
    val conn = mock(classOf[Connection])
    val metadata = mock(classOf[DatabaseMetaData])
    val stmt = mock(classOf[PreparedStatement])
    when(conn.getMetaData).thenReturn(metadata)
    when(metadata.supportsTransactions()).thenReturn(false)
    when(conn.prepareStatement(anyString())).thenReturn(stmt)

    val batchFailure = new SQLException("batch rejected", "23000", 2627)
    val nextFailure = new SQLException("duplicate key detail")
    batchFailure.setNextException(nextFailure)
    when(stmt.executeBatch()).thenThrow(batchFailure)

    val dialect = new JdbcDialect with NoLegacyJDBCError {
      override def canHandle(url: String): Boolean = url == testJdbcUrl
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
    }
    registerDialect(dialect)
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try {
      val options = newWriteOptions()
      val error = intercept[SparkRuntimeException] {
        JdbcUtils.savePartition(
          "t",
          Iterator(Row(1)),
          StructType(Seq(StructField("a", IntegerType))),
          "INSERT INTO t (a) VALUES (?)",
          batchSize = 1,
          dialect,
          Connection.TRANSACTION_NONE,
          options)
      }
      checkError(
        exception = error,
        condition = "FAILED_JDBC.WRITE",
        sqlState = Some("HV000"),
        parameters = Map("url" -> options.getRedactUrl(), "tableName" -> "t"))
      assert(error.getCause eq batchFailure)
      assert(batchFailure.getNextException eq nextFailure)
      assert(batchFailure.getCause eq nextFailure)
    } finally {
      TaskContext.unset()
      JdbcDialects.unregisterDialect(dialect)
    }
  }

  test("savePartition classifies JDBC commit failures") {
    val conn = mock(classOf[Connection])
    val metadata = mock(classOf[DatabaseMetaData])
    val stmt = mock(classOf[PreparedStatement])
    when(conn.getMetaData).thenReturn(metadata)
    when(metadata.supportsTransactions()).thenReturn(true)
    when(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED))
      .thenReturn(true)
    when(conn.prepareStatement(anyString())).thenReturn(stmt)

    val commitFailure = new SQLException("commit rejected", "40001")
    doThrow(commitFailure).when(conn).commit()
    val dialect = new JdbcDialect with NoLegacyJDBCError {
      override def canHandle(url: String): Boolean = url == testJdbcUrl
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
    }
    registerDialect(dialect)
    TaskContext.setTaskContext(TaskContext.empty())
    try {
      val options = newWriteOptions()
      val error = intercept[SparkRuntimeException] {
        JdbcUtils.savePartition(
          "t",
          Iterator(Row(1)),
          StructType(Seq(StructField("a", IntegerType))),
          "INSERT INTO t (a) VALUES (?)",
          batchSize = 1,
          dialect,
          Connection.TRANSACTION_READ_COMMITTED,
          options)
      }
      checkError(
        exception = error,
        condition = "FAILED_JDBC.WRITE",
        sqlState = Some("HV000"),
        parameters = Map("url" -> options.getRedactUrl(), "tableName" -> "t"))
      assert(error.getCause eq commitFailure)
      verify(conn).rollback()
    } finally {
      TaskContext.unset()
      JdbcDialects.unregisterDialect(dialect)
    }
  }

  test("savePartition preserves classification from a legacy JDBC dialect") {
    val conn = mock(classOf[Connection])
    val metadata = mock(classOf[DatabaseMetaData])
    val stmt = mock(classOf[PreparedStatement])
    when(conn.getMetaData).thenReturn(metadata)
    when(metadata.supportsTransactions()).thenReturn(false)
    when(conn.prepareStatement(anyString())).thenReturn(stmt)

    val batchFailure = new SQLException("batch rejected", "23000", 2627)
    when(stmt.executeBatch()).thenThrow(batchFailure)

    val dialect = new JdbcDialect {
      override def canHandle(url: String): Boolean = url == testJdbcUrl
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
    }
    registerDialect(dialect)
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try {
      val error = intercept[AnalysisException] {
        JdbcUtils.savePartition(
          "t",
          Iterator(Row(1)),
          StructType(Seq(StructField("a", IntegerType))),
          "INSERT INTO t (a) VALUES (?)",
          batchSize = 1,
          dialect,
          Connection.TRANSACTION_NONE,
          newWriteOptions())
      }
      checkError(
        exception = error,
        condition = "FAILED_JDBC.UNCLASSIFIED",
        sqlState = Some("HV000"),
        parameters = Map("url" -> "jdbc:", "message" -> "Write rows to the table t."))
      assert(error.getCause eq batchFailure)
    } finally {
      TaskContext.unset()
      JdbcDialects.unregisterDialect(dialect)
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
    val conn = mock(classOf[Connection])
    val metadata = mock(classOf[DatabaseMetaData])
    val stmt = mock(classOf[PreparedStatement])
    when(conn.getMetaData).thenReturn(metadata)
    when(metadata.supportsTransactions()).thenReturn(true)
    when(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED))
      .thenReturn(true)
    when(conn.prepareStatement(anyString())).thenReturn(stmt)
    val writeFailure = new SQLException("write rejected", "23000")
    if (operation == "binding") {
      doThrow(writeFailure).when(stmt).setInt(anyInt(), anyInt())
    } else {
      when(stmt.executeBatch()).thenThrow(writeFailure)
    }
    doThrow(new SQLException("statement close failed", "08006")).when(stmt).close()
    val dialect = new JdbcDialect with NoLegacyJDBCError {
      override def canHandle(url: String): Boolean = url == testJdbcUrl
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
    }
    TaskContext.setTaskContext(TaskContext.empty())
    try {
      val options = newWriteOptions()
      val error = intercept[SparkRuntimeException] {
        JdbcUtils.savePartition(
          "t",
          Iterator(Row(1)),
          StructType(Seq(StructField("a", IntegerType))),
          "INSERT INTO t (a) VALUES (?)",
          batchSize = 1,
          dialect,
          Connection.TRANSACTION_READ_COMMITTED,
          options)
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
    } finally {
      TaskContext.unset()
    }
  }

  test("statement cleanup does not abort a successful write before commit") {
    val conn = mock(classOf[Connection])
    val metadata = mock(classOf[DatabaseMetaData])
    val stmt = mock(classOf[PreparedStatement])
    when(conn.getMetaData).thenReturn(metadata)
    when(metadata.supportsTransactions()).thenReturn(true)
    when(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED))
      .thenReturn(true)
    when(conn.prepareStatement(anyString())).thenReturn(stmt)
    doThrow(new SQLException("statement close failed", "08006")).when(stmt).close()
    val dialect = new JdbcDialect with NoLegacyJDBCError {
      override def canHandle(url: String): Boolean = url == testJdbcUrl
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
    }
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try {
      JdbcUtils.savePartition(
        "t",
        Iterator(Row(1)),
        StructType(Seq(StructField("a", IntegerType))),
        "INSERT INTO t (a) VALUES (?)",
        batchSize = 1,
        dialect,
        Connection.TRANSACTION_READ_COMMITTED,
        newWriteOptions())
      verify(stmt).executeBatch()
      verify(stmt).close()
      verify(conn).commit()
      verify(conn, never()).rollback()
      assert(context.taskMetrics.outputMetrics.recordsWritten == 1)
    } finally {
      TaskContext.unset()
    }
  }
}
