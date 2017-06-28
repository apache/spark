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

package org.apache.spark.sql.sources

import java.io.File
import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfter

class JdbcInsertSuite extends DataSourceTest with BeforeAndAfter with SharedSQLContext {
  import testImplicits._

  val url = "jdbc:h2:mem:testdb0"
  val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
  var conn: java.sql.Connection = null


  protected override lazy val sql = spark.sql _

  before {
    Utils.classForName("org.h2.Driver")
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")

    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement(
      "create table test.timestamp_test (id bigint(11) DEFAULT NULL, time_stamp TIMESTAMP NOT NULL)").
      executeUpdate()

    conn.commit()

    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW timestamp_test
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$url', dbtable 'test.timestamp_test', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))
  }

  after {
    spark.catalog.dropTempView("jdbcTable")

    conn.prepareStatement("drop table test.timestamp_test").executeUpdate()
    conn.prepareStatement("drop schema test").executeUpdate()

    conn.commit()
    conn.close()
  }

  test("SPARK-19726 - Faild to insert null timestamp value to mysql using spark jdbc") {

    val message = intercept[Exception] {
      sql(
        s"""
           |INSERT INTO timestamp_test values(111, null)
      """.stripMargin)
    }.getMessage

    assert(
      message.contains("NULL not allowed for column \"TIME_STAMP\""),
      "It is not allowed to insert null into timestamp column which is defined not null."
    )
  }
}

