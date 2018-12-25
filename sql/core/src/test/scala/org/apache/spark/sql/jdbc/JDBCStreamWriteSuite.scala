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

import java.sql.DriverManager

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamTest}
import org.apache.spark.util.Utils

private case class TestData(name: String, value: Long)

class JDBCStreamWriteSuite extends StreamTest with BeforeAndAfter {
  import testImplicits._

  val url = "jdbc:h2:mem:testdb"
  val jdbcTableName = "stream_test_table"
  val driverClassName = "org.h2.Driver"
  val createTableSql = s"""
      |CREATE TABLE ${jdbcTableName}(
      | name VARCHAR(32),
      | value LONG,
      | PRIMARY KEY (name)
      |)""".stripMargin

  var conn: java.sql.Connection = null

  val testH2Dialect = new JdbcDialect {
    override def canHandle(url: String) : Boolean = url.startsWith("jdbc:h2")
    override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
  }

  before {
    Utils.classForName(driverClassName)
    conn = DriverManager.getConnection(url)
    conn.prepareStatement(createTableSql).executeUpdate()
  }

  after {
    conn.close()
  }

  test("Basic Write") {
    withTempDir { checkpointDir => {
        val input = MemoryStream[Int]
        val query = input.toDF().map { row =>
          val value = row.getInt(0)
          TestData(s"name_$value", value.toLong)
        }.writeStream
          .format("jdbc")
          .option(JDBCOptions.JDBC_URL, url)
          .option(JDBCOptions.JDBC_TABLE_NAME, jdbcTableName)
          .option(JDBCOptions.JDBC_DRIVER_CLASS, driverClassName)
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .start()
        try {
          input.addData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          query.processAllAvailable()
        } finally {
          query.stop()
        }
      }
    }
    val result = conn
      .prepareStatement(s"select count(*) as count from $jdbcTableName")
      .executeQuery()
    assert(result.next())
    assert(result.getInt("count") == 10)
  }

  test("Write sub columns") {
    withTempDir { checkpointDir => {
      val input = MemoryStream[Int]
      val query = input.toDF().map { row =>
        val value = row.getInt(0)
        TestData(s"name_$value", value.toLong)
      }.select("name").writeStream // write just one `name` column
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL, url)
        .option(JDBCOptions.JDBC_TABLE_NAME, jdbcTableName)
        .option(JDBCOptions.JDBC_DRIVER_CLASS, driverClassName)
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start()
      try {
        input.addData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        query.processAllAvailable()
      } finally {
        query.stop()
      }
    }
    }
    val result = conn
      .prepareStatement(s"select count(*) as count from $jdbcTableName")
      .executeQuery()
    assert(result.next())
    assert(result.getInt("count") == 10)
  }

  test("Write same data") {
    withTempDir { checkpointDir => {
      val input = MemoryStream[Int]
      val query = input.toDF().map { row =>
        val value = row.getInt(0)
        TestData(s"name_$value", value.toLong)
      }.writeStream
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL, url)
        .option(JDBCOptions.JDBC_TABLE_NAME, jdbcTableName)
        .option(JDBCOptions.JDBC_DRIVER_CLASS, driverClassName)
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start()
      try {
        input.addData(1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        query.processAllAvailable()
      } finally {
        query.stop()
      }
    }
    }
    val result = conn
      .prepareStatement(s"select count(*) as count from $jdbcTableName")
      .executeQuery()
    assert(result.next())
    assert(result.getInt("count") == 1)
  }

  test("Write without required parameter") {
    // without jdbc url
    val thrown = intercept[StreamingQueryException] {
      withTempDir { checkpointDir => {
          val input = MemoryStream[Int]
          val query = input.toDF().map { row =>
            val value = row.getInt(0)
            TestData(s"name_$value", value.toLong)
          }.writeStream
            .format("jdbc")
            .option(JDBCOptions.JDBC_TABLE_NAME, jdbcTableName)
            .option(JDBCOptions.JDBC_DRIVER_CLASS, driverClassName)
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .start()
          try {
            input.addData(1, 2, 3)
            query.processAllAvailable()
          } finally {
            query.stop()
          }
        }
      }
    }
    assert(thrown.getMessage.contains("requirement failed: Option 'url' is required."))
    // without table name
    val thrown2 = intercept[StreamingQueryException] {
      withTempDir { checkpointDir => {
          val input = MemoryStream[Int]
          val query = input.toDF().map { row =>
            val value = row.getInt(0)
            TestData(s"name_$value", value.toLong)
          }.writeStream
            .format("jdbc")
            .option(JDBCOptions.JDBC_URL, url)
            .option(JDBCOptions.JDBC_DRIVER_CLASS, driverClassName)
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .start()
          try {
            input.addData(1, 2, 3)
            query.processAllAvailable()
          } finally {
            query.stop()
          }
        }
      }
    }
    assert(thrown2.getMessage
      .contains("Option 'dbtable' or 'query' is required."))
  }
}
