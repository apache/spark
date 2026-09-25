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

import java.sql.{Connection, SQLException, SQLTransientConnectionException}

import org.mockito.Mockito._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.jdbc.JdbcDialect

/**
 * Tests for the connection retry wrapper in `JdbcUtils.createConnectionFactory`, driven by the
 * `connectionRetryAttempts` and `connectionRetryDelayMs` options.
 */
class JDBCConnectionRetrySuite extends SparkFunSuite {

  private val testJdbcUrl = "jdbc:connectionretry:test"

  // An explicit driver keeps JDBCOptions from resolving the fake URL through DriverManager.
  private def options(extra: (String, String)*): JDBCOptions = new JDBCOptions(Map(
    "url" -> testJdbcUrl,
    "dbtable" -> "t",
    "driver" -> "org.h2.Driver") ++ extra)

  /**
   * A dialect whose connection factory calls `connect` with the 1-based attempt number, so a test
   * can fail the first few attempts and then assert how many times it was called.
   */
  private class CountingDialect(connect: Int => Connection) extends JdbcDialect {
    var attempts = 0
    override def canHandle(url: String): Boolean = url == testJdbcUrl
    override def createConnectionFactory(options: JDBCOptions): Int => Connection = { _ =>
      attempts += 1
      connect(attempts)
    }
  }

  test("retries are off by default, so the first transient failure propagates") {
    val dialect = new CountingDialect(n => throw new SQLTransientConnectionException(s"fail $n"))
    val factory = JdbcUtils.createConnectionFactory(dialect, options())
    val e = intercept[SQLTransientConnectionException](factory(-1))
    assert(dialect.attempts === 1)
    assert(e.getMessage === "fail 1")
  }

  test("retries a SQLTransientConnectionException until it succeeds") {
    val conn = mock(classOf[Connection])
    val dialect = new CountingDialect(
      n => if (n < 3) throw new SQLTransientConnectionException(s"fail $n") else conn)
    val factory = JdbcUtils.createConnectionFactory(dialect,
      options("connectionRetryAttempts" -> "3", "connectionRetryDelayMs" -> "1"))
    assert(factory(-1) === conn)
    assert(dialect.attempts === 3)
  }

  test("retries SQLState class 08 (connection exception) until it succeeds") {
    val conn = mock(classOf[Connection])
    val dialect = new CountingDialect(
      n => if (n == 1) throw new SQLException("failover in progress", "08001") else conn)
    val factory = JdbcUtils.createConnectionFactory(dialect,
      options("connectionRetryAttempts" -> "2", "connectionRetryDelayMs" -> "1"))
    assert(factory(-1) === conn)
    assert(dialect.attempts === 2)
  }

  test("does not retry a failure that retrying cannot fix, such as bad credentials") {
    val dialect = new CountingDialect(_ => throw new SQLException("invalid password", "28000"))
    val factory = JdbcUtils.createConnectionFactory(dialect,
      options("connectionRetryAttempts" -> "5", "connectionRetryDelayMs" -> "1"))
    val e = intercept[SQLException](factory(-1))
    assert(dialect.attempts === 1)
    assert(e.getMessage === "invalid password")
  }

  test("propagates the last failure once the retries are exhausted") {
    val dialect = new CountingDialect(n => throw new SQLTransientConnectionException(s"fail $n"))
    val factory = JdbcUtils.createConnectionFactory(dialect,
      options("connectionRetryAttempts" -> "2", "connectionRetryDelayMs" -> "1"))
    val e = intercept[SQLTransientConnectionException](factory(-1))
    // The initial attempt plus the two retries.
    assert(dialect.attempts === 3)
    assert(e.getMessage === "fail 3")
  }

  test("rejects a negative retry count and a negative delay") {
    val attempts = intercept[IllegalArgumentException] {
      options("connectionRetryAttempts" -> "-1")
    }
    assert(attempts.getMessage.contains("connectionRetryAttempts"))
    val delay = intercept[IllegalArgumentException] {
      options("connectionRetryDelayMs" -> "-1")
    }
    assert(delay.getMessage.contains("connectionRetryDelayMs"))
  }
}
