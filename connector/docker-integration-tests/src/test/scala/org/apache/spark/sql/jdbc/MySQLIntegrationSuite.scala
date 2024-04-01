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

import scala.util.Using

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., mysql:8.3.0):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MYSQL_DOCKER_IMAGE_NAME=mysql:8.3.0
 *     ./build/sbt -Pdocker-integration-tests
 *     "docker-integration-tests/testOnly org.apache.spark.sql.jdbc.MySQLIntegrationSuite"
 * }}}
 */
@DockerTest
class MySQLIntegrationSuite extends DockerJDBCIntegrationSuite {
  override val db = new MySQLDatabaseOnDocker

  override def dataPreparation(conn: Connection): Unit = {
    // Since MySQL 5.7.14+, we need to disable strict mode
    conn.prepareStatement("SET GLOBAL sql_mode = ''").executeUpdate()
    conn.prepareStatement("CREATE DATABASE foo").executeUpdate()
    conn.prepareStatement("CREATE TABLE tbl (x INTEGER, y TEXT(8))").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (42,'fred')").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (17,'dave')").executeUpdate()

    conn.prepareStatement("CREATE TABLE bools (b1 BOOLEAN, b2 BIT(1), b3 TINYINT(1))")
      .executeUpdate()
    conn.prepareStatement("INSERT INTO bools VALUES (TRUE, b'1', 1)").executeUpdate()

    conn.prepareStatement("CREATE TABLE numbers (onebit BIT(1), tenbits BIT(10), "
      + "small SMALLINT, med MEDIUMINT, nor INT, big BIGINT, deci DECIMAL(40,20), flt FLOAT, "
      + "dbl DOUBLE, tiny TINYINT)").executeUpdate()

    conn.prepareStatement("INSERT INTO numbers VALUES (b'0', b'1000100101', "
      + "17, 77777, 123456789, 123456789012345, 123456789012345.123456789012345, "
      + "42.75, 1.0000000000000002, -128)").executeUpdate()
    conn.prepareStatement("INSERT INTO numbers VALUES (null, null, null, null, null," +
      "null, null, null, null, null)").executeUpdate()

    conn.prepareStatement("CREATE TABLE unsigned_numbers (" +
      "tiny TINYINT UNSIGNED, small SMALLINT UNSIGNED, med MEDIUMINT UNSIGNED," +
      "nor INT UNSIGNED, big BIGINT UNSIGNED, deci DECIMAL(40,20) UNSIGNED," +
      "dbl DOUBLE UNSIGNED, tiny1u TINYINT(1) UNSIGNED)").executeUpdate()

    conn.prepareStatement("INSERT INTO unsigned_numbers VALUES (255, 65535, 16777215, 4294967295," +
      "9223372036854775808, 123456789012345.123456789012345, 1.0000000000000002, 0)")
      .executeUpdate()

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

    conn.prepareStatement("CREATE TABLE floats (f1 FLOAT, f2 FLOAT(10), f3 FLOAT(53), " +
      "f4 FLOAT UNSIGNED, f5 FLOAT(10) UNSIGNED, f6 FLOAT(53) UNSIGNED)").executeUpdate()
    conn.prepareStatement("INSERT INTO floats VALUES (1.23, 4.56, 7.89, 1.23, 4.56, 7.89)")
      .executeUpdate()

    conn.prepareStatement("CREATE TABLE collections (" +
        "a SET('cap', 'hat', 'helmet'), b ENUM('S', 'M', 'L', 'XL'))").executeUpdate()
    conn.prepareStatement("INSERT INTO collections VALUES ('cap,hat', 'M')").executeUpdate()

    conn.prepareStatement("CREATE TABLE TBL_GEOMETRY (col0 GEOMETRY)").executeUpdate()
    conn.prepareStatement("INSERT INTO TBL_GEOMETRY VALUES (ST_GeomFromText('POINT(0 0)'))")
      .executeUpdate()
  }

  def testConnection(): Unit = {
    Using.resource(getConnection()) { conn =>
      assert(conn.getClass.getName === "com.mysql.cj.jdbc.ConnectionImpl")
    }
  }

  test("SPARK-47537: ensure use the right jdbc driver") {
    testConnection()
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
    val row = sqlContext.read.jdbc(jdbcUrl, "numbers", new Properties).head()
    assert(row.length === 10)
    assert(row(0).isInstanceOf[Boolean])
    assert(row(1).isInstanceOf[Array[Byte]])
    assert(row(2).isInstanceOf[Short])
    assert(row(3).isInstanceOf[Int])
    assert(row(4).isInstanceOf[Int])
    assert(row(5).isInstanceOf[Long])
    assert(row(6).isInstanceOf[BigDecimal])
    assert(row(7).isInstanceOf[Float])
    assert(row(8).isInstanceOf[Double])
    assert(row(9).isInstanceOf[Byte])
    assert(!row.getBoolean(0))
    assert(java.util.Arrays.equals(row.getAs[Array[Byte]](1),
      Array[Byte](49, 48, 49, 48, 48, 49, 48, 49)))
    assert(row.getShort(2) == 17)
    assert(row.getInt(3) == 77777)
    assert(row.getInt(4) == 123456789)
    assert(row.getLong(5) == 123456789012345L)
    val bd = new BigDecimal("123456789012345.12345678901234500000")
    assert(row.getAs[BigDecimal](6).equals(bd))
    assert(row.getFloat(7) == 42.75)
    assert(row.getDouble(8) == 1.0000000000000002)
    assert(row.getByte(9) == 0x80.toByte)
    withSQLConf(SQLConf.LEGACY_MYSQL_BIT_ARRAY_MAPPING_ENABLED.key -> "true") {
      val row = sqlContext.read.jdbc(jdbcUrl, "numbers", new Properties).head()
      assert(row(1).isInstanceOf[Long])
      assert(row.getLong(1) == 0x225)
    }
  }

  test("SPARK-47462: Unsigned numeric types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "unsigned_numbers", new Properties)
    val rows = df.head()
    assert(rows.get(0).isInstanceOf[Short])
    assert(rows.get(1).isInstanceOf[Integer])
    assert(rows.get(2).isInstanceOf[Integer])
    assert(rows.get(3).isInstanceOf[Long])
    assert(rows.get(4).isInstanceOf[BigDecimal])
    assert(rows.get(5).isInstanceOf[BigDecimal])
    assert(rows.get(6).isInstanceOf[Double])
    // Unlike MySQL, MariaDB seems not to distinguish signed and unsigned tinyint(1).
    val isMaria = jdbcUrl.indexOf("disableMariaDbDriver") == -1
    if (isMaria) {
      assert(rows.get(7).isInstanceOf[Boolean])
    } else {
      assert(rows.get(7).isInstanceOf[Short])
    }
    assert(rows.getShort(0) === 255)
    assert(rows.getInt(1) === 65535)
    assert(rows.getInt(2) === 16777215)
    assert(rows.getLong(3) === 4294967295L)
    assert(rows.getAs[BigDecimal](4).equals(new BigDecimal("9223372036854775808")))
    assert(rows.getAs[BigDecimal](5).equals(new BigDecimal("123456789012345.12345678901234500000")))
    assert(rows.getDouble(6) === 1.0000000000000002)
    if (isMaria) {
      assert(rows.getBoolean(7) === false)
    } else {
      assert(rows.getShort(7) === 0)
    }
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
    val df = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("query", "select yr from dates")
      .option("yearIsDateType", false)
      .load()
    checkAnswer(df, Row(2001))
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


  test("SPARK-47478: all boolean synonyms read-write roundtrip") {
    val df = sqlContext.read.jdbc(jdbcUrl, "bools", new Properties)
    checkAnswer(df, Row(true, true, true))

    val properties0 = new Properties()
    properties0.setProperty("transformedBitIsBoolean", "false")
    properties0.setProperty("tinyInt1isBit", "true")

    checkAnswer(spark.read.jdbc(jdbcUrl, "bools", properties0), Row(true, true, true))
    val properties1 = new Properties()
    properties1.setProperty("transformedBitIsBoolean", "true")
    properties1.setProperty("tinyInt1isBit", "true")
    checkAnswer(spark.read.jdbc(jdbcUrl, "bools", properties1), Row(true, true, true))

    val properties2 = new Properties()
    properties2.setProperty("tinyInt1isBit", "false")
    checkAnswer(sqlContext.read.jdbc(jdbcUrl, "bools", properties2), Row(1, true, 1))

    df.write.mode("append").jdbc(jdbcUrl, "bools", new Properties)
    checkAnswer(df, Seq(Row(true, true, true), Row(true, true, true)))
  }

  test("SPARK-47515: Save TimestampNTZType as DATETIME in MySQL") {
    val expected = sql("select timestamp_ntz'2018-11-17 13:33:33' as col0")
    expected.write.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "TBL_DATETIME_NTZ")
      .save()

    val answer = spark.read
      .option("preferTimestampNTZ", true).jdbc(jdbcUrl, "TBL_DATETIME_NTZ", new Properties)
    checkAnswer(answer, expected)
  }

  test("SPARK-47522: Read MySQL FLOAT as FloatType to keep consistent with the write side") {
    val df = spark.read.jdbc(jdbcUrl, "floats", new Properties)
    checkAnswer(df, Row(1.23f, 4.56f, 7.89d, 1.23d, 4.56d, 7.89d))
  }

  test("SPARK-47557: MySQL ENUM/SET types contains only java.sq.Types.CHAR information") {
    val df = spark.read.jdbc(jdbcUrl, "collections", new Properties)
    checkAnswer(df, Row("cap,hat       ", "M "))
    df.write.mode("append").jdbc(jdbcUrl, "collections", new Properties)
    withSQLConf(SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "true") {
      checkAnswer(spark.read.jdbc(jdbcUrl, "collections", new Properties),
        Row("cap,hat", "M") :: Row("cap,hat", "M") :: Nil)
    }
  }

  test("SPARK-47616: Read GEOMETRY from MySQL") {
    val df = spark.read.jdbc(jdbcUrl, "TBL_GEOMETRY", new Properties)
    checkAnswer(df,
      Row(Array[Byte](0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
  }

  test("SPARK-47666: Check nulls for result set getters") {
    Seq("true", "false").foreach { flag =>
      withSQLConf(SQLConf.LEGACY_MYSQL_BIT_ARRAY_MAPPING_ENABLED.key -> flag) {
        val nulls = spark.read.jdbc(jdbcUrl, "numbers", new Properties).tail(1).head
        assert(nulls === Row(null, null, null, null, null, null, null, null, null, null))
      }
    }
  }
}


/**
 * To run this test suite for a specific version (e.g., mysql:8.3.0):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MYSQL_DOCKER_IMAGE_NAME=mysql:8.3.0
 *     ./build/sbt -Pdocker-integration-tests
 *     "docker-integration-tests/testOnly *MySQLOverMariaConnectorIntegrationSuite"
 * }}}
 */
@DockerTest
class MySQLOverMariaConnectorIntegrationSuite extends MySQLIntegrationSuite {

  override val db = new MySQLDatabaseOnDocker {
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:mysql://$ip:$port/mysql?user=root&password=rootpass&allowPublicKeyRetrieval=true" +
        s"&useSSL=false"
  }

  override def testConnection(): Unit = {
    Using.resource(getConnection()) { conn =>
      assert(conn.getClass.getName === "org.mariadb.jdbc.MariaDbConnection")
    }
  }
}
