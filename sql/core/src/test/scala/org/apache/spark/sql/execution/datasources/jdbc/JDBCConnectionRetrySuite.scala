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
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.sql.jdbc.JdbcDialects

class JDBCConnectionRetrySuite extends SparkFunSuite with MockitoSugar {

  private val url = "jdbc:mock://localhost:1234/test"
  private val dialect = JdbcDialects.get(url)

  test("No retries by default when connection succeeds on attempt 1") {
    var attempts = 0
    val mockConn = mock[Connection]
    val options = new JDBCOptions(Map("url" -> url, "dbtable" -> "t"))
    val factory = JdbcUtils.createConnectionFactory(dialect, options)

    // Override raw dialect connection factory for testing
    val rawFactory: Int => Connection = _ => {
      attempts += 1
      mockConn
    }
    val conn = rawFactory(-1)
    assert(attempts === 1)
    assert(conn === mockConn)
  }

  test("Retries on SQLTransientConnectionException and succeeds") {
    var attempts = 0
    val mockConn = mock[Connection]
    val options = new JDBCOptions(Map(
      "url" -> url,
      "dbtable" -> "t",
      "connectionRetryAttempts" -> "3",
      "connectionRetryDelayMs" -> "1"
    ))

    val rawDialect = new org.apache.spark.sql.jdbc.JdbcDialect {
      override def canHandle(url: String): Boolean = true
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
        _ => {
          attempts += 1
          if (attempts < 3) {
            throw new SQLTransientConnectionException("Transient connection failure")
          }
          mockConn
        }
      }
    }

    val factory = JdbcUtils.createConnectionFactory(rawDialect, options)
    val conn = factory(-1)
    assert(attempts === 3)
    assert(conn === mockConn)
  }

  test("Retries on connection SQLState (08001) and succeeds") {
    var attempts = 0
    val mockConn = mock[Connection]
    val options = new JDBCOptions(Map(
      "url" -> url,
      "dbtable" -> "t",
      "connectionRetryAttempts" -> "2",
      "connectionRetryDelayMs" -> "1"
    ))

    val rawDialect = new org.apache.spark.sql.jdbc.JdbcDialect {
      override def canHandle(url: String): Boolean = true
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
        _ => {
          attempts += 1
          if (attempts == 1) {
            throw new SQLException("Unable to establish connection", "08001")
          }
          mockConn
        }
      }
    }

    val factory = JdbcUtils.createConnectionFactory(rawDialect, options)
    val conn = factory(-1)
    assert(attempts === 2)
    assert(conn === mockConn)
  }

  test("Does NOT retry on non-transient auth error (SQLState 28000)") {
    var attempts = 0
    val options = new JDBCOptions(Map(
      "url" -> url,
      "dbtable" -> "t",
      "connectionRetryAttempts" -> "5",
      "connectionRetryDelayMs" -> "1"
    ))

    val rawDialect = new org.apache.spark.sql.jdbc.JdbcDialect {
      override def canHandle(url: String): Boolean = true
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
        _ => {
          attempts += 1
          throw new SQLException("Invalid password", "28000")
        }
      }
    }

    val factory = JdbcUtils.createConnectionFactory(rawDialect, options)
    val ex = intercept[SQLException] {
      factory(-1)
    }
    assert(attempts === 1)
    assert(ex.getMessage === "Invalid password")
  }

  test("Exhausted retries preserves original exception") {
    var attempts = 0
    val options = new JDBCOptions(Map(
      "url" -> url,
      "dbtable" -> "t",
      "connectionRetryAttempts" -> "2",
      "connectionRetryDelayMs" -> "1"
    ))

    val rawDialect = new org.apache.spark.sql.jdbc.JdbcDialect {
      override def canHandle(url: String): Boolean = true
      override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
        _ => {
          attempts += 1
          throw new SQLTransientConnectionException(s"Failure attempt $attempts")
        }
      }
    }

    val factory = JdbcUtils.createConnectionFactory(rawDialect, options)
    val ex = intercept[SQLTransientConnectionException] {
      factory(-1)
    }
    assert(attempts === 3) // Initial attempt + 2 retries
    assert(ex.getMessage === "Failure attempt 3")
  }
}
