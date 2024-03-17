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
import java.time.LocalDateTime
import java.util.Properties

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., mysql:8.0.31):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MYSQL_DOCKER_IMAGE_NAME=mysql:8.0.31
 *     ./build/sbt -Pdocker-integration-tests
 *     "docker-integration-tests/testOnly org.apache.spark.sql.jdbc.MySQLIntegrationSuite"
 * }}}
 */
@DockerTest
class MySQLIntegrationSuite extends DockerJDBCIntegrationSuite {
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("MYSQL_DOCKER_IMAGE_NAME", "mysql:8.0.31")
    override val env = Map(
      "MYSQL_ROOT_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 3306
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:mysql://$ip:$port/mysql?user=root&password=rootpass"
  }

  override def dataPreparation(conn: Connection): Unit = {
    // Since MySQL 5.7.14+, we need to disable strict mode
    conn.prepareStatement("SET GLOBAL sql_mode = ''").executeUpdate()
    conn.prepareStatement("CREATE DATABASE foo").executeUpdate()
    conn.prepareStatement("CREATE TABLE tbl (x INTEGER, y TEXT(8))").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (42,'fred')").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (17,'dave')").executeUpdate()

    conn.prepareStatement("CREATE TABLE numbers (onebit BIT(1), tenbits BIT(10), "
      + "small SMALLINT, med MEDIUMINT, nor INT, big BIGINT, deci DECIMAL(40,20), flt FLOAT, "
      + "dbl DOUBLE, tiny TINYINT)").executeUpdate()
    conn.prepareStatement("INSERT INTO numbers VALUES (b'0', b'1000100101', "
      + "17, 77777, 123456789, 123456789012345, 123456789012345.123456789012345, "
      + "42.75, 1.0000000000000002, -128)").executeUpdate()

    conn.prepareStatement("CREATE TABLE dates (d DATE, t TIME, dt DATETIME, ts TIMESTAMP, "
      + "yr YEAR)").executeUpdate()
    conn.prepareStatement("INSERT INTO dates VALUES ('1991-11-09', '13:31:24', "
      + "'1996-01-01 01:23:45', '2009-02-13 23:31:30', '2001')").executeUpdate()

    // TODO: Test locale conversion for strings.
    conn.prepareStatement("CREATE TABLE strings (a CHAR(10), b VARCHAR(10), c TINYTEXT, "
      + "d TEXT, e MEDIUMTEXT, f LONGTEXT, g BINARY(4), h VARBINARY(10), i BLOB, j JSON)"
    ).executeUpdate()
    conn.prepareStatement("INSERT INTO strings VALUES ('the', 'quick', 'brown', 'fox', " +
      "'jumps', 'over', 'the', 'lazy', 'dog', '{\"status\": \"merrily\"}')").executeUpdate()
  }

  test("Basic test") {
    val df = sqlContext.read.jdbc(jdbcUrl, "tbl", new Properties)
    val rows = df.collect()
    assert(rows.length == 2)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 2)
    assert(types(0).equals("class java.lang.Integer"))
    assert(types(1).equals("class java.lang.String"))
  }

  test("Numeric types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "numbers", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 10)
    assert(types(0).equals("class java.lang.Boolean"))
    assert(types(1).equals("class java.lang.Long"))
    assert(types(2).equals("class java.lang.Integer"))
    assert(types(3).equals("class java.lang.Integer"))
    assert(types(4).equals("class java.lang.Integer"))
    assert(types(5).equals("class java.lang.Long"))
    assert(types(6).equals("class java.math.BigDecimal"))
    assert(types(7).equals("class java.lang.Double"))
    assert(types(8).equals("class java.lang.Double"))
    assert(types(9).equals("class java.lang.Byte"))
    assert(rows(0).getBoolean(0) == false)
    assert(rows(0).getLong(1) == 0x225)
    assert(rows(0).getInt(2) == 17)
    assert(rows(0).getInt(3) == 77777)
    assert(rows(0).getInt(4) == 123456789)
    assert(rows(0).getLong(5) == 123456789012345L)
    val bd = new BigDecimal("123456789012345.12345678901234500000")
    assert(rows(0).getAs[BigDecimal](6).equals(bd))
    assert(rows(0).getDouble(7) == 42.75)
    assert(rows(0).getDouble(8) == 1.0000000000000002)
    assert(rows(0).getByte(9) == 0x80.toByte)
  }

  test("Date types") {
    withDefaultTimeZone(UTC) {
      val df = sqlContext.read.jdbc(jdbcUrl, "dates", new Properties)
      val rows = df.collect()
      assert(rows.length == 1)
      val types = rows(0).toSeq.map(x => x.getClass.toString)
      assert(types.length == 5)
      assert(types(0).equals("class java.sql.Date"))
      assert(types(1).equals("class java.sql.Timestamp"))
      assert(types(2).equals("class java.sql.Timestamp"))
      assert(types(3).equals("class java.sql.Timestamp"))
      assert(types(4).equals("class java.sql.Date"))
      assert(rows(0).getAs[Date](0).equals(Date.valueOf("1991-11-09")))
      assert(
        rows(0).getAs[Timestamp](1) === Timestamp.valueOf("1970-01-01 13:31:24"))
      assert(rows(0).getAs[Timestamp](2).equals(Timestamp.valueOf("1996-01-01 01:23:45")))
      assert(rows(0).getAs[Timestamp](3).equals(Timestamp.valueOf("2009-02-13 23:31:30")))
      assert(rows(0).getAs[Date](4).equals(Date.valueOf("2001-01-01")))
    }
  }

  test("SPARK-47406: MySQL datetime types with preferTimestampNTZ") {
    withDefaultTimeZone(UTC) {
      val df = sqlContext.read.option("preferTimestampNTZ", true)
        .jdbc(jdbcUrl, "dates", new Properties)
      checkAnswer(df, Row(
        Date.valueOf("1991-11-09"),
        LocalDateTime.of(1970, 1, 1, 13, 31, 24),
        LocalDateTime.of(1996, 1, 1, 1, 23, 45),
        Timestamp.valueOf("2009-02-13 23:31:30"),
        Date.valueOf("2001-01-01")))
    }
  }

  test("String types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "strings", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 10)
    assert(types(0).equals("class java.lang.String"))
    assert(types(1).equals("class java.lang.String"))
    assert(types(2).equals("class java.lang.String"))
    assert(types(3).equals("class java.lang.String"))
    assert(types(4).equals("class java.lang.String"))
    assert(types(5).equals("class java.lang.String"))
    assert(types(6).equals("class [B"))
    assert(types(7).equals("class [B"))
    assert(types(8).equals("class [B"))
    assert(types(9).equals("class java.lang.String"))
    assert(rows(0).getString(0).equals("the".padTo(10, ' ')))
    assert(rows(0).getString(1).equals("quick"))
    assert(rows(0).getString(2).equals("brown"))
    assert(rows(0).getString(3).equals("fox"))
    assert(rows(0).getString(4).equals("jumps"))
    assert(rows(0).getString(5).equals("over"))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](6), Array[Byte](116, 104, 101, 0)))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](7), Array[Byte](108, 97, 122, 121)))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](8), Array[Byte](100, 111, 103)))
    assert(rows(0).getString(9).equals("{\"status\": \"merrily\"}"))
  }

  test("Basic write test") {
    val df1 = sqlContext.read.jdbc(jdbcUrl, "numbers", new Properties)
    val df2 = sqlContext.read.jdbc(jdbcUrl, "dates", new Properties)
    val df3 = sqlContext.read.jdbc(jdbcUrl, "strings", new Properties)
    df1.write.jdbc(jdbcUrl, "numberscopy", new Properties)
    df2.write.jdbc(jdbcUrl, "datescopy", new Properties)
    df3.write.jdbc(jdbcUrl, "stringscopy", new Properties)
  }

  test("query JDBC option") {
    val expectedResult = Set(
      (42, "fred"),
      (17, "dave")
    ).map { case (x, y) =>
      Row(Integer.valueOf(x), String.valueOf(y))
    }

    val query = "SELECT x, y FROM tbl WHERE x > 10"
    // query option to pass on the query string.
    val df = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("query", query)
      .load()
    assert(df.collect().toSet === expectedResult)

    // query option in the create table path.
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW queryOption
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$jdbcUrl', query '$query')
       """.stripMargin.replaceAll("\n", " "))
    assert(sql("select x, y from queryOption").collect().toSet == expectedResult)
  }
}
