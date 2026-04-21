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

import java.sql.{Connection, DatabaseMetaData, PreparedStatement, ResultSet, SQLException}
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import org.apache.spark.{SparkFunSuite, TaskContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Tests for JDBC task interrupt handling (SPARK-56019):
 * write path (`JdbcUtils.savePartition`) and read path (`JDBCRDD.compute`).
 * When a Spark task is killed, native JDBC socket reads (e.g. socketRead0) do not respond
 * to `Thread.interrupt()`; closing the JDBC connection from the `TaskInterruptListener`
 * unblocks the thread by causing the socket to throw.
 */
class JdbcTaskInterruptSuite extends SparkFunSuite with SharedSparkSession {

  private val testJdbcUrl = "jdbc:taskinterrupt:test"

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

  test("SPARK-56019: savePartition closes JDBC connection when task is interrupted") {
    val conn = mock(classOf[Connection])
    val metaData = mock(classOf[DatabaseMetaData])
    when(metaData.supportsTransactions()).thenReturn(false)
    when(conn.getMetaData).thenReturn(metaData)
    doNothing().when(conn).setAutoCommit(anyBoolean())
    doNothing().when(conn).setTransactionIsolation(anyInt())
    val stmt = mock(classOf[PreparedStatement])
    when(conn.prepareStatement(anyString())).thenReturn(stmt)
    doNothing().when(stmt).setQueryTimeout(anyInt())
    doNothing().when(stmt).setNull(anyInt(), anyInt())
    doNothing().when(stmt).setInt(anyInt(), anyInt())
    doNothing().when(stmt).setString(anyInt(), anyString())
    doNothing().when(stmt).addBatch()
    doNothing().when(stmt).close()

    // Block inside executeBatch to simulate a thread stuck in a native JDBC socket read.
    val blockInExecuteBatch = new CountDownLatch(1)
    val enteredExecuteBatch = new CountDownLatch(1)
    when(stmt.executeBatch()).thenAnswer { _ =>
      enteredExecuteBatch.countDown()
      blockInExecuteBatch.await(60, TimeUnit.SECONDS)
      // Simulate real driver behaviour: executeBatch throws when the connection is closed
      // underneath it (the interrupt listener calls conn.close() while this is blocking).
      throw new SQLException("Connection closed by interrupt")
    }

    val connClosedLatch = new CountDownLatch(1)
    doAnswer { _ => connClosedLatch.countDown(); null }.when(conn).close()

    val dialect = new JdbcDialect {
      override def canHandle(url: String): Boolean = url == testJdbcUrl
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
    }
    registerDialect(dialect)
    try {
      val options = newWriteOptions()
      val schema = StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", StringType)))
      val insertStmt = "INSERT INTO t (a, b) VALUES (?, ?)"
      val context = TaskContext.empty()
      var thrown: Option[Throwable] = None
      val saveThread = new Thread(() => {
        TaskContext.setTaskContext(context)
        try {
          JdbcUtils.savePartition(
            "t",
            Iterator(Row(1, "x")),
            schema,
            insertStmt,
            batchSize = 1,
            dialect,
            Connection.TRANSACTION_READ_COMMITTED,
            options)
        } catch {
          case e: Throwable => thrown = Some(e)
        } finally {
          TaskContext.unset()
        }
      })
      saveThread.start()
      assert(
        enteredExecuteBatch.await(10, TimeUnit.SECONDS),
        "savePartition should reach executeBatch")
      context.markInterrupted("test")
      assert(
        connClosedLatch.await(10, TimeUnit.SECONDS),
        "interrupt listener should close the JDBC connection")
      blockInExecuteBatch.countDown()
      saveThread.join(5000)
      assert(thrown.isDefined, "savePartition should throw after connection is closed")
      assert(
        thrown.get.isInstanceOf[SQLException] &&
          thrown.get.getMessage.contains("Connection closed by interrupt"),
        s"Expected SQLException from closed connection, got: ${thrown.get}")
    } finally {
      JdbcDialects.unregisterDialect(dialect)
    }
  }

  test("SPARK-56019: savePartition handles rollback exception when connection already closed") {
    val conn = mock(classOf[Connection])
    val metaData = mock(classOf[DatabaseMetaData])
    when(metaData.supportsTransactions()).thenReturn(true)
    when(metaData.getDefaultTransactionIsolation).thenReturn(Connection.TRANSACTION_READ_COMMITTED)
    when(metaData.supportsTransactionIsolationLevel(anyInt())).thenReturn(true)
    when(conn.getMetaData).thenReturn(metaData)
    when(conn.getAutoCommit).thenReturn(true)
    doNothing().when(conn).setAutoCommit(anyBoolean())
    doNothing().when(conn).setTransactionIsolation(anyInt())

    val stmt = mock(classOf[PreparedStatement])
    when(conn.prepareStatement(anyString())).thenReturn(stmt)
    doNothing().when(stmt).setQueryTimeout(anyInt())
    doNothing().when(stmt).setNull(anyInt(), anyInt())
    doNothing().when(stmt).setInt(anyInt(), anyInt())
    doNothing().when(stmt).setString(anyInt(), anyString())
    doNothing().when(stmt).addBatch()
    doNothing().when(stmt).close()

    var closedByListener = false
    doAnswer { _ =>
      closedByListener = true
      null
    }.when(conn).close()
    when(conn.isClosed).thenAnswer { _ => closedByListener }
    doThrow(new SQLException("Connection is closed")).when(conn).rollback()

    val blockBeforeExecuteBatch = new CountDownLatch(1)
    val enteredExecuteBatch = new CountDownLatch(1)
    when(stmt.executeBatch()).thenAnswer { _ =>
      enteredExecuteBatch.countDown()
      blockBeforeExecuteBatch.await(60, TimeUnit.SECONDS)
      throw new SQLException("executeBatch failed")
    }

    val dialect = new JdbcDialect {
      override def canHandle(url: String): Boolean = url == testJdbcUrl
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = _ => conn
    }
    registerDialect(dialect)
    try {
      val options = newWriteOptions()
      val schema = StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", StringType)))
      val insertStmt = "INSERT INTO t (a, b) VALUES (?, ?)"
      val context = TaskContext.empty()
      var thrown: Option[Throwable] = None
      val saveThread = new Thread(() => {
        TaskContext.setTaskContext(context)
        try {
          JdbcUtils.savePartition(
            "t",
            Iterator(Row(1, "x")),
            schema,
            insertStmt,
            batchSize = 1,
            dialect,
            Connection.TRANSACTION_READ_COMMITTED,
            options)
        } catch {
          case e: Throwable => thrown = Some(e)
        } finally {
          TaskContext.unset()
        }
      })
      saveThread.start()
      assert(
        enteredExecuteBatch.await(10, TimeUnit.SECONDS),
        "savePartition should reach executeBatch")
      context.markInterrupted("test")
      blockBeforeExecuteBatch.countDown()
      saveThread.join(5000)
      assert(
        closedByListener,
        "TaskInterruptListener should close the connection before executeBatch can finish")
      verify(conn, atLeast(1)).close()
      assert(thrown.isDefined, "savePartition should throw")
      assert(
        thrown.get.isInstanceOf[SQLException] &&
          thrown.get.getMessage.contains("executeBatch failed"),
        s"Original executeBatch exception should propagate, not rollback: ${thrown.get}")
    } finally {
      JdbcDialects.unregisterDialect(dialect)
    }
  }

  test("SPARK-56019: JDBCRDD.compute closes JDBC connection when task is interrupted") {
    val metadataConn = mock(classOf[Connection])
    val metadataMetaData = mock(classOf[DatabaseMetaData])
    when(metadataConn.getMetaData).thenReturn(metadataMetaData)
    when(metadataMetaData.getDatabaseMajorVersion).thenReturn(1)
    when(metadataMetaData.getDatabaseMinorVersion).thenReturn(0)
    when(metadataMetaData.getDriverMajorVersion).thenReturn(1)
    when(metadataMetaData.getDriverMinorVersion).thenReturn(0)
    when(metadataMetaData.getDatabaseProductName).thenReturn("Test")
    when(metadataMetaData.getDriverName).thenReturn("Test Driver")

    val partitionConn = mock(classOf[Connection])
    val part = JDBCPartition(whereClause = "1=1", idx = 0)
    val getConnection: Int => Connection = {
      case -1 => metadataConn
      case 0 => partitionConn
      case i => throw new IllegalArgumentException(s"Unexpected partition index: $i")
    }
    val metaData = mock(classOf[DatabaseMetaData])
    when(partitionConn.getMetaData).thenReturn(metaData)
    when(partitionConn.getAutoCommit).thenReturn(true)
    val stmt = mock(classOf[PreparedStatement])
    when(partitionConn.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(stmt)
    doNothing().when(stmt).setFetchSize(anyInt())
    doNothing().when(stmt).setQueryTimeout(anyInt())
    val rs = mock(classOf[ResultSet])
    when(stmt.executeQuery()).thenReturn(rs)
    val blockInNext = new CountDownLatch(1)
    val enteredNext = new CountDownLatch(1)
    when(rs.next()).thenAnswer { _ =>
      enteredNext.countDown()
      blockInNext.await(60, TimeUnit.SECONDS)
      false
    }
    doNothing().when(rs).close()
    doNothing().when(stmt).close()

    val connClosedLatch = new CountDownLatch(1)
    doAnswer { _ => connClosedLatch.countDown(); null }.when(partitionConn).close()

    val schema = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", StringType)))
    val options = new JDBCOptions(Map(
      "url" -> testJdbcUrl,
      "dbtable" -> "t",
      "driver" -> "org.h2.Driver"))
    val dialect = new JdbcDialect {
      override def canHandle(url: String): Boolean = url == testJdbcUrl
    }
    registerDialect(dialect)
    try {
      val rdd = new JDBCRDD(
        sc = spark.sparkContext,
        getConnection = getConnection,
        schema = schema,
        columns = Array("a", "b"),
        predicates = Array.empty,
        partitions = Array(part),
        url = testJdbcUrl,
        options = options,
        databaseMetadata = JDBCDatabaseMetadata.fromJDBCConnectionFactory(getConnection),
        groupByColumns = None,
        sample = None,
        limit = 0,
        sortOrders = Array.empty,
        offset = 0,
        additionalMetrics = Map.empty)
      val context = TaskContext.empty()
      val computeThread = new Thread(() => {
        TaskContext.setTaskContext(context)
        try {
          val iter = rdd.compute(part, context)
          iter.hasNext
        } catch {
          case NonFatal(_) => // expected when connection is closed
        } finally {
          TaskContext.unset()
        }
      })
      computeThread.start()
      assert(
        enteredNext.await(10, TimeUnit.SECONDS),
        "compute should reach rs.next()")
      context.markInterrupted("test")
      assert(
        connClosedLatch.await(10, TimeUnit.SECONDS),
        "interrupt listener should close the JDBC connection")
      blockInNext.countDown()
      computeThread.join(5000)
    } finally {
      JdbcDialects.unregisterDialect(dialect)
    }
  }
}
