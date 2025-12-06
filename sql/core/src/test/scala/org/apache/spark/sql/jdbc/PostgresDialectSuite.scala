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

import java.sql.Connection

import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

class PostgresDialectSuite extends SparkFunSuite with MockitoSugar {

  private def createJDBCOptions(extraOptions: Map[String, String]): JDBCOptions = {
    new JDBCOptions(Map(
      "url" -> "jdbc:postgresql://localhost:5432/test",
      "dbtable" -> "test_table"
    ) ++ extraOptions)
  }

  test("beforeFetch sets autoCommit=false with lowercase fetchsize") {
    val conn = mock[Connection]
    val dialect = PostgresDialect()
    dialect.beforeFetch(conn, createJDBCOptions(Map("fetchsize" -> "100")))
    verify(conn).setAutoCommit(false)
  }

  test("beforeFetch sets autoCommit=false with camelCase fetchSize") {
    val conn = mock[Connection]
    val dialect = PostgresDialect()
    dialect.beforeFetch(conn, createJDBCOptions(Map("fetchSize" -> "100")))
    verify(conn).setAutoCommit(false)
  }

  test("beforeFetch sets autoCommit=false with uppercase FETCHSIZE") {
    val conn = mock[Connection]
    val dialect = PostgresDialect()
    dialect.beforeFetch(conn, createJDBCOptions(Map("FETCHSIZE" -> "100")))
    verify(conn).setAutoCommit(false)
  }

  test("beforeFetch does not set autoCommit when fetchSize is 0") {
    val conn = mock[Connection]
    val dialect = PostgresDialect()
    dialect.beforeFetch(conn, createJDBCOptions(Map("fetchsize" -> "0")))
    verify(conn, never()).setAutoCommit(false)
  }
}
