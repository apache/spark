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


package org.apache.spark.sql.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.mockito.ArgumentMatchers.{anyInt, anyString}
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

class PostgresDialectSuite extends SparkFunSuite with MockitoSugar {

  private val dialect = PostgresDialect()

  private def createJDBCOptions(extraOptions: Map[String, String]): JDBCOptions = {
    new JDBCOptions(Map(
      "url" -> "jdbc:postgresql://localhost:5432/test",
      "dbtable" -> "test_table"
    ) ++ extraOptions)
  }

  test("beforeFetch sets autoCommit=false with lowercase fetchsize") {
    val conn = mock[Connection]
    dialect.beforeFetch(conn, createJDBCOptions(Map("fetchsize" -> "100")))
    verify(conn).setAutoCommit(false)
  }

  test("beforeFetch sets autoCommit=false with camelCase fetchSize") {
    val conn = mock[Connection]
    dialect.beforeFetch(conn, createJDBCOptions(Map("fetchSize" -> "100")))
    verify(conn).setAutoCommit(false)
  }

  test("beforeFetch sets autoCommit=false with uppercase FETCHSIZE") {
    val conn = mock[Connection]
    dialect.beforeFetch(conn, createJDBCOptions(Map("FETCHSIZE" -> "100")))
    verify(conn).setAutoCommit(false)
  }

  test("beforeFetch does not set autoCommit when fetchSize is 0") {
    val conn = mock[Connection]
    dialect.beforeFetch(conn, createJDBCOptions(Map("fetchsize" -> "0")))
    verify(conn, never()).setAutoCommit(false)
  }

  test("effectiveFetchSize: returns user-specified value when set") {
    assert(dialect.effectiveFetchSize(createJDBCOptions(Map("fetchsize" -> "500"))) === 500)
  }

  test("effectiveFetchSize: returns 0 when user explicitly sets 0") {
    assert(dialect.effectiveFetchSize(createJDBCOptions(Map("fetchsize" -> "0"))) === 0)
  }

  test("effectiveFetchSize: returns defaultFetchSize (1000) when not set") {
    assert(dialect.effectiveFetchSize(createJDBCOptions(Map.empty)) === 1000)
  }

  test("effectiveFetchSize: is case-insensitive for option key") {
    Seq("fetchsize", "fetchSize", "FETCHSIZE").foreach { key =>
      assert(dialect.effectiveFetchSize(createJDBCOptions(Map(key -> "200"))) === 200,
        s"Failed for key: $key")
    }
  }

  test("effectiveFetchSize: base dialect returns 0 when not set") {
    val baseDialect = new JdbcDialect {
      override def canHandle(url: String): Boolean = true
    }
    assert(baseDialect.defaultFetchSize === 0)
    assert(baseDialect.effectiveFetchSize(createJDBCOptions(Map.empty)) === 0)
  }

  /**
   * Simulates the JDBCRDD.compute() call sequence to verify that beforeFetch (connection setup)
   * and effectiveFetchSize (statement setup) work correctly together.
   *
   * In JDBCRDD.compute():
   *   1. dialect.beforeFetch(conn, options)      -- sets autoCommit=false for Postgres
   *   2. conn.prepareStatement(...)
   *   3. stmt.setFetchSize(dialect.effectiveFetchSize(options))
   */
  private def simulateJdbcRead(options: JDBCOptions): (Connection, PreparedStatement) = {
    val conn = mock[Connection]
    val stmt = mock[PreparedStatement]
    val rs = mock[ResultSet]
    when(conn.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(stmt)
    when(stmt.executeQuery()).thenReturn(rs)
    when(rs.next()).thenReturn(false)

    // Simulate JDBCRDD.compute() sequence
    dialect.beforeFetch(conn, options)
    stmt.setFetchSize(dialect.effectiveFetchSize(options))
    stmt.executeQuery()

    (conn, stmt)
  }

  test("JDBCRDD compute simulation: no fetchSize specified uses dialect default") {
    val options = createJDBCOptions(Map.empty)
    val (conn, stmt) = simulateJdbcRead(options)

    val order = inOrder(conn, stmt)
    order.verify(conn).setAutoCommit(false)
    order.verify(stmt).setFetchSize(1000)
  }

  test("JDBCRDD compute simulation: explicit fetchSize overrides dialect default") {
    val options = createJDBCOptions(Map("fetchsize" -> "50"))
    val (conn, stmt) = simulateJdbcRead(options)

    val order = inOrder(conn, stmt)
    order.verify(conn).setAutoCommit(false)
    order.verify(stmt).setFetchSize(50)
  }

  test("JDBCRDD compute simulation: explicit fetchSize=0 disables cursor fetching") {
    val options = createJDBCOptions(Map("fetchsize" -> "0"))
    val (conn, stmt) = simulateJdbcRead(options)

    verify(conn, never()).setAutoCommit(false)
    verify(stmt).setFetchSize(0)
  }
}
