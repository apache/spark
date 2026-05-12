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

import java.math.BigDecimal
import java.sql.{Connection, Date, Timestamp}
import java.util.Properties

import scala.util.Using

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., starrocks/allin1-ubuntu:4.0.6):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 STARROCKS_DOCKER_IMAGE_NAME=starrocks/allin1-ubuntu:4.0.6
 *     ./build/sbt -Pdocker-integration-tests
 *     "docker-integration-tests/testOnly org.apache.spark.sql.jdbc.StarRocksIntegrationSuite"
 * }}}
 */
@DockerTest
class StarRocksIntegrationSuite extends SharedJDBCIntegrationSuite {
  override val db = new StarRocksDatabaseOnDocker

  override def sleepBeforeTesting(): Unit = Thread.sleep(60000)

  override def createSharedTable(conn: Connection): Unit = {
    val batchStmt = conn.createStatement()
    batchStmt.execute("CREATE TABLE tbl_shared (x INTEGER) DISTRIBUTED BY HASH(x)")
    batchStmt.execute("INSERT INTO tbl_shared VALUES(1)")
    batchStmt.close()
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE DATABASE foo").executeUpdate()
    conn.prepareStatement("USE foo").executeUpdate()
    conn.prepareStatement("CREATE TABLE tbl (x INTEGER, y VARCHAR(8)) DISTRIBUTED BY HASH(x)")
      .executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (42,'fred')").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (17,'dave')").executeUpdate()

    conn.prepareStatement("CREATE TABLE numbers (nor INT, big BIGINT, deci DECIMAL(38,18), "
      + "dbl DOUBLE, tiny TINYINT) DISTRIBUTED BY HASH(nor)").executeUpdate()

    conn.prepareStatement("INSERT INTO numbers VALUES (123456789, 123456789012345, "
      + "123456789012345.123456789012345, 1.0000000000000002, -128)").executeUpdate()

    conn.prepareStatement("CREATE TABLE dates (d DATE, dt DATETIME) DISTRIBUTED BY HASH(d)")
      .executeUpdate()
    conn.prepareStatement("INSERT INTO dates VALUES ('1991-11-09', '1996-01-01 01:23:45')")
      .executeUpdate()

    conn.prepareStatement("CREATE TABLE strings (a CHAR(10), b VARCHAR(10), c STRING) " +
      "DISTRIBUTED BY HASH(a)").executeUpdate()
    conn.prepareStatement("INSERT INTO strings VALUES ('the', 'quick', 'brown')").executeUpdate()
  }

  def testConnection(): Unit = {
    Using.resource(getConnection()) { conn =>
      assert(conn.getClass.getName === "com.mysql.cj.jdbc.ConnectionImpl")
    }
  }

  test("Ensure MySQL jdbc driver") {
    testConnection()
  }

  test("Basic test") {
    val df = spark.read.jdbc(jdbcUrl, "foo.tbl", new Properties)
    val rows = df.collect()
    assert(rows.length == 2)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 2)
    assert(types(0).equals("class java.lang.Integer"))
    assert(types(1).equals("class java.lang.String"))
  }

  test("Numeric types") {
    val row = spark.read.jdbc(jdbcUrl, "foo.numbers", new Properties).head()
    assert(row.length === 5)
    assert(row(0).isInstanceOf[Int])
    assert(row(1).isInstanceOf[Long])
    assert(row(2).isInstanceOf[BigDecimal])
    assert(row(3).isInstanceOf[Double])
    assert(row(4).isInstanceOf[Byte])
    assert(row.getInt(0) == 123456789)
    assert(row.getLong(1) == 123456789012345L)
    val bd = new BigDecimal("123456789012345.1234567890123450")
    assert(row.getAs[BigDecimal](2).equals(bd))
    assert(row.getDouble(3) == 1.0000000000000002)
    assert(row.getByte(4) == -128)
  }

  test("Date types") {
    withDefaultTimeZone(UTC) {
      val df = spark.read.jdbc(jdbcUrl, "foo.dates", new Properties)
      checkAnswer(df, Row(
        Date.valueOf("1991-11-09"),
        Timestamp.valueOf("1996-01-01 01:23:45")))
    }
  }

  test("String types") {
    val df = spark.read.jdbc(jdbcUrl, "foo.strings", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 3)
    assert(types(0).equals("class java.lang.String"))
    assert(types(1).equals("class java.lang.String"))
    assert(types(2).equals("class java.lang.String"))
    assert(rows(0).getString(0).equals("the"))
    assert(rows(0).getString(1).equals("quick"))
    assert(rows(0).getString(2).equals("brown"))
  }
}
