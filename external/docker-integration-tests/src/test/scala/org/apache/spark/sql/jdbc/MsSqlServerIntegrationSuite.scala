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

import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., 2019-CU13-ubuntu-20.04):
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 MSSQLSERVER_DOCKER_IMAGE_NAME=2019-CU13-ubuntu-20.04
 *     ./build/sbt -Pdocker-integration-tests
 *     "testOnly org.apache.spark.sql.jdbc.MsSqlServerIntegrationSuite"
 * }}}
 */
@DockerTest
class MsSqlServerIntegrationSuite extends DockerJDBCIntegrationSuite {
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("MSSQLSERVER_DOCKER_IMAGE_NAME",
      "mcr.microsoft.com/mssql/server:2019-CU13-ubuntu-20.04")
    override val env = Map(
      "SA_PASSWORD" -> "Sapass123",
      "ACCEPT_EULA" -> "Y"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 1433

    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:sqlserver://$ip:$port;user=sa;password=Sapass123;"
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE TABLE tbl (x INT, y VARCHAR (50))").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (42,'fred')").executeUpdate()
    conn.prepareStatement("INSERT INTO tbl VALUES (17,'dave')").executeUpdate()

    conn.prepareStatement(
      """
        |CREATE TABLE numbers (
        |a BIT,
        |b TINYINT, c SMALLINT, d INT, e BIGINT,
        |f FLOAT, f1 FLOAT(24),
        |g REAL,
        |h DECIMAL(5,2), i NUMERIC(10,5),
        |j MONEY, k SMALLMONEY)
      """.stripMargin).executeUpdate()
    conn.prepareStatement(
      """
        |INSERT INTO numbers VALUES (
        |0,
        |255, 32767, 2147483647, 9223372036854775807,
        |123456789012345.123456789012345, 123456789012345.123456789012345,
        |123456789012345.123456789012345,
        |123, 12345.12,
        |922337203685477.58, 214748.3647)
      """.stripMargin).executeUpdate()

    conn.prepareStatement(
      """
        |CREATE TABLE dates (
        |a DATE, b DATETIME, c DATETIME2,
        |d DATETIMEOFFSET, e SMALLDATETIME,
        |f TIME)
      """.stripMargin).executeUpdate()
    conn.prepareStatement(
      """
        |INSERT INTO dates VALUES (
        |'1991-11-09', '1999-01-01 13:23:35', '9999-12-31 23:59:59',
        |'1901-05-09 23:59:59 +14:00', '1996-01-01 23:23:45',
        |'13:31:24')
      """.stripMargin).executeUpdate()

    conn.prepareStatement(
      """
        |CREATE TABLE strings (
        |a CHAR(10), b VARCHAR(10),
        |c NCHAR(10), d NVARCHAR(10),
        |e BINARY(4), f VARBINARY(4),
        |g TEXT, h NTEXT,
        |i IMAGE)
      """.stripMargin).executeUpdate()
    conn.prepareStatement(
      """
        |INSERT INTO strings VALUES (
        |'the', 'quick',
        |'brown', 'fox',
        |123456, 123456,
        |'the', 'lazy',
        |'dog')
      """.stripMargin).executeUpdate()
    conn.prepareStatement(
      """
        |CREATE TABLE spatials (
        |point geometry,
        |line geometry,
        |circle geometry,
        |curve geography,
        |polygon geometry,
        |curve_polygon geography,
        |multi_point geometry,
        |multi_line geometry,
        |multi_polygon geometry,
        |geometry_collection geometry)
      """.stripMargin).executeUpdate()
    conn.prepareStatement(
      """
        |INSERT INTO spatials VALUES (
        |'POINT(3 4 7 2.5)',
        |'LINESTRING(1 0, 0 1, -1 0)',
        |'CIRCULARSTRING(
        |  -122.358 47.653, -122.348 47.649, -122.348 47.658, -122.358 47.658, -122.358 47.653)',
        |'COMPOUNDCURVE(
        |  CIRCULARSTRING(-122.358 47.653, -122.348 47.649,
        |    -122.348 47.658, -122.358 47.658, -122.358 47.653))',
        |'POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (10 0, 0 10, 0 -10, 10 0))',
        |'CURVEPOLYGON((-122.3 47, 122.3 47, 125.7 49, 121 38, -122.3 47))',
        |'MULTIPOINT((2 3), (7 8 9.5))',
        |'MULTILINESTRING((0 2, 1 1), (1 0, 1 1))',
        |'MULTIPOLYGON(((2 2, 2 -2, -2 -2, -2 2, 2 2)),((1 1, 3 1, 3 3, 1 3, 1 1)))',
        |'GEOMETRYCOLLECTION(LINESTRING(1 1, 3 5),POLYGON((-1 -1, -1 -5, -5 -5, -5 -1, -1 -1)))')
      """.stripMargin).executeUpdate()
  }

  test("Basic test") {
    val df = spark.read.jdbc(jdbcUrl, "tbl", new Properties)
    val rows = df.collect()
    assert(rows.length == 2)
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types.length == 2)
    assert(types(0).equals("class java.lang.Integer"))
    assert(types(1).equals("class java.lang.String"))
  }

  test("Numeric types") {
    Seq(true, false).foreach { flag =>
      withSQLConf(SQLConf.LEGACY_MSSQLSERVER_NUMERIC_MAPPING_ENABLED.key -> s"$flag") {
        val df = spark.read.jdbc(jdbcUrl, "numbers", new Properties)
        val rows = df.collect()
        assert(rows.length == 1)
        val row = rows(0)
        val types = row.toSeq.map(x => x.getClass.toString)
        assert(types.length == 12)
        assert(types(0).equals("class java.lang.Boolean"))
        assert(types(1).equals("class java.lang.Integer"))
        if (flag) {
          assert(types(2).equals("class java.lang.Integer"))
        } else {
          assert(types(2).equals("class java.lang.Short"))
        }
        assert(types(3).equals("class java.lang.Integer"))
        assert(types(4).equals("class java.lang.Long"))
        assert(types(5).equals("class java.lang.Double"))
        if (flag) {
          assert(types(6).equals("class java.lang.Double"))
          assert(types(7).equals("class java.lang.Double"))
        } else {
          assert(types(6).equals("class java.lang.Float"))
          assert(types(7).equals("class java.lang.Float"))
        }
        assert(types(8).equals("class java.math.BigDecimal"))
        assert(types(9).equals("class java.math.BigDecimal"))
        assert(types(10).equals("class java.math.BigDecimal"))
        assert(types(11).equals("class java.math.BigDecimal"))
        assert(row.getBoolean(0) == false)
        assert(row.getInt(1) == 255)
        if (flag) {
          assert(row.getInt(2) == 32767)
        } else {
          assert(row.getShort(2) == 32767)
        }
        assert(row.getInt(3) == 2147483647)
        assert(row.getLong(4) == 9223372036854775807L)
        assert(row.getDouble(5) == 1.2345678901234512E14) // float(53) has 15-digits precision
        if (flag) {
          assert(row.getDouble(6) == 1.23456788103168E14) // float(24) has 7-digits precision
          assert(row.getDouble(7) == 1.23456788103168E14) // real = float(24)
        } else {
          assert(row.getFloat(6) == 1.23456788103168E14)  // float(24) has 7-digits precision
          assert(row.getFloat(7) == 1.23456788103168E14)  // real = float(24)
        }
        assert(row.getAs[BigDecimal](8).equals(new BigDecimal("123.00")))
        assert(row.getAs[BigDecimal](9).equals(new BigDecimal("12345.12000")))
        assert(row.getAs[BigDecimal](10).equals(new BigDecimal("922337203685477.5800")))
        assert(row.getAs[BigDecimal](11).equals(new BigDecimal("214748.3647")))
      }
    }
  }

  test("Date types") {
    withDefaultTimeZone(UTC) {
      val df = spark.read.jdbc(jdbcUrl, "dates", new Properties)
      val rows = df.collect()
      assert(rows.length == 1)
      val row = rows(0)
      val types = row.toSeq.map(x => x.getClass.toString)
      assert(types.length == 6)
      assert(types(0).equals("class java.sql.Date"))
      assert(types(1).equals("class java.sql.Timestamp"))
      assert(types(2).equals("class java.sql.Timestamp"))
      assert(types(3).equals("class java.lang.String"))
      assert(types(4).equals("class java.sql.Timestamp"))
      assert(types(5).equals("class java.sql.Timestamp"))
      assert(row.getAs[Date](0).equals(Date.valueOf("1991-11-09")))
      assert(row.getAs[Timestamp](1).equals(Timestamp.valueOf("1999-01-01 13:23:35.0")))
      assert(row.getAs[Timestamp](2).equals(Timestamp.valueOf("9999-12-31 23:59:59.0")))
      assert(row.getString(3).equals("1901-05-09 23:59:59.0000000 +14:00"))
      assert(row.getAs[Timestamp](4).equals(Timestamp.valueOf("1996-01-01 23:24:00.0")))
      assert(row.getAs[Timestamp](5).equals(Timestamp.valueOf("1970-01-01 13:31:24.0")))
    }
  }

  test("String types") {
    val df = spark.read.jdbc(jdbcUrl, "strings", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val row = rows(0)
    val types = row.toSeq.map(x => x.getClass.toString)
    assert(types.length == 9)
    assert(types(0).equals("class java.lang.String"))
    assert(types(1).equals("class java.lang.String"))
    assert(types(2).equals("class java.lang.String"))
    assert(types(3).equals("class java.lang.String"))
    assert(types(4).equals("class [B"))
    assert(types(5).equals("class [B"))
    assert(types(6).equals("class java.lang.String"))
    assert(types(7).equals("class java.lang.String"))
    assert(types(8).equals("class [B"))
    assert(row.getString(0).length == 10)
    assert(row.getString(0).trim.equals("the"))
    assert(row.getString(1).equals("quick"))
    assert(row.getString(2).length == 10)
    assert(row.getString(2).trim.equals("brown"))
    assert(row.getString(3).equals("fox"))
    assert(java.util.Arrays.equals(row.getAs[Array[Byte]](4), Array[Byte](0, 1, -30, 64)))
    assert(java.util.Arrays.equals(row.getAs[Array[Byte]](5), Array[Byte](0, 1, -30, 64)))
    assert(row.getString(6).equals("the"))
    assert(row.getString(7).equals("lazy"))
    assert(java.util.Arrays.equals(row.getAs[Array[Byte]](8), Array[Byte](100, 111, 103)))
  }

  test("Basic write test") {
    val df1 = spark.read.jdbc(jdbcUrl, "numbers", new Properties)
    val df2 = spark.read.jdbc(jdbcUrl, "dates", new Properties)
    val df3 = spark.read.jdbc(jdbcUrl, "strings", new Properties)
    df1.write.jdbc(jdbcUrl, "numberscopy", new Properties)
    df2.write.jdbc(jdbcUrl, "datescopy", new Properties)
    df3.write.jdbc(jdbcUrl, "stringscopy", new Properties)
  }

  test("SPARK-33813: MsSqlServerDialect should support spatial types") {
    val df = spark.read.jdbc(jdbcUrl, "spatials", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val row = rows(0)
    val types = row.toSeq.map(x => x.getClass.toString)
    assert(types.length == 10)
    assert(types(0) == "class [B")
    assert(row.getAs[Array[Byte]](0) ===
      Array(0, 0, 0, 0, 1, 15, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0,
        16, 64, 0, 0, 0, 0, 0, 0, 28, 64, 0, 0, 0, 0, 0, 0, 4, 64))
    assert(types(1) == "class [B")
    assert(row.getAs[Array[Byte]](1) ===
      Array[Byte](0, 0, 0, 0, 1, 4, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        -16, 63, 0, 0, 0, 0, 0, 0, -16, -65, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,
        0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 2))
    assert(types(2) == "class [B")
    assert(row.getAs[Array[Byte]](2) ===
      Array[Byte](0, 0, 0, 0, 2, 4, 5, 0, 0, 0, -12, -3, -44, 120, -23, -106,
        94, -64, -35, 36, 6, -127, -107, -45, 71, 64, -125, -64, -54, -95, 69,
        -106, 94, -64, 80, -115, -105, 110, 18, -45, 71, 64, -125, -64, -54,
        -95, 69, -106, 94, -64, 78, 98, 16, 88, 57, -44, 71, 64, -12, -3, -44,
        120, -23, -106, 94, -64, 78, 98, 16, 88, 57, -44, 71, 64, -12, -3, -44,
        120, -23, -106, 94, -64, -35, 36, 6, -127, -107, -45, 71, 64, 1, 0, 0,
        0, 2, 0, 0, 0, 0, 1, 0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 8))
    assert(types(3) == "class [B")
    assert(row.getAs[Array[Byte]](3) ===
      Array[Byte](-26, 16, 0, 0, 2, 4, 5, 0, 0, 0, -35, 36, 6, -127, -107, -45,
        71, 64, -12, -3, -44, 120, -23, -106, 94, -64, 80, -115, -105, 110, 18,
        -45, 71, 64, -125, -64, -54, -95, 69, -106, 94, -64, 78, 98, 16, 88, 57,
        -44, 71, 64, -125, -64, -54, -95, 69, -106, 94, -64, 78, 98, 16, 88, 57,
        -44, 71, 64, -12, -3, -44, 120, -23, -106, 94, -64, -35, 36, 6, -127, -107,
        -45, 71, 64, -12, -3, -44, 120, -23, -106, 94, -64, 1, 0, 0, 0, 3, 0, 0,
        0, 0, 1, 0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 9, 2, 0, 0, 0, 3, 1))
    assert(types(5) == "class [B")
    assert(row.getAs[Array[Byte]](4) ===
      Array[Byte](0, 0, 0, 0, 1, 4, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 52, -64, 0, 0,
        0, 0, 0, 0, 52, -64, 0, 0, 0, 0, 0, 0, 52, -64, 0, 0, 0, 0, 0, 0, 52, 64,
        0, 0, 0, 0, 0, 0, 52, 64, 0, 0, 0, 0, 0, 0, 52, 64, 0, 0, 0, 0, 0, 0, 52,
        64, 0, 0, 0, 0, 0, 0, 52, -64, 0, 0, 0, 0, 0, 0, 52, -64, 0, 0, 0, 0, 0,
        0, 52, -64, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 36, -64, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0,
        0, 2, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 3))
    assert(types(6) === "class [B")
    assert(row.getAs[Array[Byte]](5) ===
      Array[Byte](-26, 16, 0, 0, 2, 4, 5, 0, 0, 0, 0, 0, 0, 0, 0, -128, 71, 64, 51,
        51, 51, 51, 51, -109, 94, -64, 0, 0, 0, 0, 0, -128, 71, 64, 51, 51, 51, 51,
        51, -109, 94, 64, 0, 0, 0, 0, 0, -128, 72, 64, -51, -52, -52, -52, -52, 108,
        95, 64, 0, 0, 0, 0, 0, 0, 67, 64, 0, 0, 0, 0, 0, 64, 94, 64, 0, 0, 0, 0, 0,
        -128, 71, 64, 51, 51, 51, 51, 51, -109, 94, -64, 1, 0, 0, 0, 1, 0, 0, 0, 0,
        1, 0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 10))
    assert(types(6) === "class [B")
    assert(row.getAs[Array[Byte]](6) ===
      Array[Byte](0, 0, 0, 0, 1, 5, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0,
        0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 28, 64, 0, 0, 0, 0, 0, 0, 32, 64, 0, 0, 0, 0,
        0, 0, -8, -1, 0, 0, 0, 0, 0, 0, 35, 64, 2, 0, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0,
        0, 0, 3, 0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 1,
        0, 0, 0, 0, 1, 0, 0, 0, 1))
    assert(types(6) === "class [B")
    assert(row.getAs[Array[Byte]](7) ===
      Array[Byte](0, 0, 0, 0, 1, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 64, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0,
        0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0,
        0, 0, 0, 0, -16, 63, 2, 0, 0, 0, 1, 0, 0, 0, 0, 1, 2, 0, 0, 0, 3, 0, 0, 0,
        -1, -1, -1, -1, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 1, 0, 0, 0, 2))
    assert(types(6) === "class [B")
    assert(row.getAs[Array[Byte]](8) ===
      Array[Byte](0, 0, 0, 0, 1, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0,
        0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, -64, 0, 0, 0,
        0, 0, 0, 0, -64, 0, 0, 0, 0, 0, 0, 0, -64, 0, 0, 0, 0, 0, 0, 0, -64, 0, 0,
        0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0,
        0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, 8, 64, 0,
        0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0,
        0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, -16, 63,
        0, 0, 0, 0, 0, 0, -16, 63, 2, 0, 0, 0, 2, 0, 0, 0, 0, 2, 5, 0, 0, 0, 3, 0,
        0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 1, 0, 0, 0, 3))
    assert(types(6) === "class [B")
    assert(row.getAs[Array[Byte]](9) ===
      Array[Byte](0, 0, 0, 0, 1, 4, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0,
        0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0,
        0, 0, 0, 0, -16, -65, 0, 0, 0, 0, 0, 0, -16, -65, 0, 0, 0, 0, 0, 0, -16, -65,
        0, 0, 0, 0, 0, 0, 20, -64, 0, 0, 0, 0, 0, 0, 20, -64, 0, 0, 0, 0, 0, 0, 20,
        -64, 0, 0, 0, 0, 0, 0, 20, -64, 0, 0, 0, 0, 0, 0, -16, -65, 0, 0, 0, 0, 0, 0,
        -16, -65, 0, 0, 0, 0, 0, 0, -16, -65, 2, 0, 0, 0, 1, 0, 0, 0, 0, 2, 2, 0, 0,
        0, 3, 0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0,
        0, 0, 0, 1, 0, 0, 0, 3))
  }
}
