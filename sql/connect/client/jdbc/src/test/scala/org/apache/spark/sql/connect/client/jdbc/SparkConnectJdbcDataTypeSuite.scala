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

package org.apache.spark.sql.connect.client.jdbc

import java.sql.{ResultSet, SQLException, Types}

import scala.util.Using

import org.apache.spark.sql.connect.client.jdbc.test.JdbcHelper
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}

class SparkConnectJdbcDataTypeSuite extends ConnectFunSuite with RemoteSparkSession
    with JdbcHelper {

  override def jdbcUrl: String = s"jdbc:sc://localhost:$serverPort"

  private def timeToMillis(hour: Int, minute: Int, second: Int, millis: Int): Long = {
    hour * 3600000L + minute * 60000L + second * 1000L + millis
  }

  test("get null type") {
    withExecuteQuery("SELECT null") { rs =>
      assert(rs.next())
      assert(rs.getString(1) === null)
      assert(rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnName(1) === "NULL")
      assert(metaData.getColumnLabel(1) === "NULL")
      assert(metaData.getColumnType(1) === Types.NULL)
      assert(metaData.getColumnTypeName(1) === "VOID")
      assert(metaData.getColumnClassName(1) === "null")
      assert(metaData.isSigned(1) === false)
      assert(metaData.getPrecision(1) === 0)
      assert(metaData.getScale(1) === 0)
      assert(metaData.getColumnDisplaySize(1) === 4)
    }
  }

  test("get boolean type") {
    withExecuteQuery("SELECT true") { rs =>
      assert(rs.next())
      assert(rs.getBoolean(1) === true)
      assert(!rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnName(1) === "true")
      assert(metaData.getColumnLabel(1) === "true")
      assert(metaData.getColumnType(1) === Types.BOOLEAN)
      assert(metaData.getColumnTypeName(1) === "BOOLEAN")
      assert(metaData.getColumnClassName(1) === "java.lang.Boolean")
      assert(metaData.isSigned(1) === false)
      assert(metaData.getPrecision(1) === 1)
      assert(metaData.getScale(1) === 0)
      assert(metaData.getColumnDisplaySize(1) === 5)
    }
  }

  test("get byte type") {
    withExecuteQuery("SELECT cast(1 as byte)") { rs =>
      assert(rs.next())
      assert(rs.getByte(1) === 1.toByte)
      assert(!rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnName(1) === "CAST(1 AS TINYINT)")
      assert(metaData.getColumnLabel(1) === "CAST(1 AS TINYINT)")
      assert(metaData.getColumnType(1) === Types.TINYINT)
      assert(metaData.getColumnTypeName(1) === "TINYINT")
      assert(metaData.getColumnClassName(1) === "java.lang.Byte")
      assert(metaData.isSigned(1) === true)
      assert(metaData.getPrecision(1) === 3)
      assert(metaData.getScale(1) === 0)
      assert(metaData.getColumnDisplaySize(1) === 4)
    }
  }

  test("get short type") {
    withExecuteQuery("SELECT cast(1 as short)") { rs =>
      assert(rs.next())
      assert(rs.getShort(1) === 1.toShort)
      assert(!rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnName(1) === "CAST(1 AS SMALLINT)")
      assert(metaData.getColumnLabel(1) === "CAST(1 AS SMALLINT)")
      assert(metaData.getColumnType(1) === Types.SMALLINT)
      assert(metaData.getColumnTypeName(1) === "SMALLINT")
      assert(metaData.getColumnClassName(1) === "java.lang.Short")
      assert(metaData.isSigned(1) === true)
      assert(metaData.getPrecision(1) === 5)
      assert(metaData.getScale(1) === 0)
      assert(metaData.getColumnDisplaySize(1) === 6)
    }
  }

  test("get int type") {
    withExecuteQuery("SELECT 1") { rs =>
      assert(rs.next())
      assert(rs.getInt(1) === 1)
      assert(!rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnName(1) === "1")
      assert(metaData.getColumnLabel(1) === "1")
      assert(metaData.getColumnType(1) === Types.INTEGER)
      assert(metaData.getColumnTypeName(1) === "INT")
      assert(metaData.getColumnClassName(1) === "java.lang.Integer")
      assert(metaData.isSigned(1) === true)
      assert(metaData.getPrecision(1) === 10)
      assert(metaData.getScale(1) === 0)
      assert(metaData.getColumnDisplaySize(1) === 11)
    }
  }

  test("get bigint type") {
    withExecuteQuery("SELECT cast(1 as bigint)") { rs =>
      assert(rs.next())
      assert(rs.getLong(1) === 1L)
      assert(!rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnName(1) === "CAST(1 AS BIGINT)")
      assert(metaData.getColumnLabel(1) === "CAST(1 AS BIGINT)")
      assert(metaData.getColumnType(1) === Types.BIGINT)
      assert(metaData.getColumnTypeName(1) === "BIGINT")
      assert(metaData.getColumnClassName(1) === "java.lang.Long")
      assert(metaData.isSigned(1) === true)
      assert(metaData.getPrecision(1) === 19)
      assert(metaData.getScale(1) === 0)
      assert(metaData.getColumnDisplaySize(1) === 20)
    }
  }

  test("get float type") {
    withExecuteQuery("SELECT cast(1.2 as float)") { rs =>
      assert(rs.next())
      assert(rs.getFloat(1) === 1.2F)
      assert(!rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnName(1) === "CAST(1.2 AS FLOAT)")
      assert(metaData.getColumnLabel(1) === "CAST(1.2 AS FLOAT)")
      assert(metaData.getColumnType(1) === Types.FLOAT)
      assert(metaData.getColumnTypeName(1) === "FLOAT")
      assert(metaData.getColumnClassName(1) === "java.lang.Float")
      assert(metaData.isSigned(1) === true)
      assert(metaData.getPrecision(1) === 7)
      assert(metaData.getScale(1) === 7)
      assert(metaData.getColumnDisplaySize(1) === 14)
    }
  }

  test("get double type") {
    withExecuteQuery("SELECT cast(1.2 as double)") { rs =>
      assert(rs.next())
      assert(rs.getDouble(1) === 1.2D)
      assert(!rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnName(1) === "CAST(1.2 AS DOUBLE)")
      assert(metaData.getColumnLabel(1) === "CAST(1.2 AS DOUBLE)")
      assert(metaData.getColumnType(1) === Types.DOUBLE)
      assert(metaData.getColumnTypeName(1) === "DOUBLE")
      assert(metaData.getColumnClassName(1) === "java.lang.Double")
      assert(metaData.isSigned(1) === true)
      assert(metaData.getPrecision(1) === 15)
      assert(metaData.getScale(1) === 15)
      assert(metaData.getColumnDisplaySize(1) === 24)
    }
  }

  test("get string type") {
    withExecuteQuery("SELECT 'str'") { rs =>
      assert(rs.next())
      assert(rs.getString(1) === "str")
      assert(!rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnName(1) === "str")
      assert(metaData.getColumnLabel(1) === "str")
      assert(metaData.getColumnType(1) === Types.VARCHAR)
      assert(metaData.getColumnTypeName(1) === "STRING")
      assert(metaData.getColumnClassName(1) === "java.lang.String")
      assert(metaData.isSigned(1) === false)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getScale(1) === 0)
      assert(metaData.getColumnDisplaySize(1) === Int.MaxValue)
    }
  }

  test("get decimal type") {
    withStatement { stmt =>
      Seq(
        ("123.45", 37, 2, 39),
        ("-0.12345", 5, 5, 8),
        ("-0.12345", 6, 5, 8),
        ("-123.45", 5, 2, 7),
        ("12345", 5, 0, 6),
        ("-12345", 5, 0, 6)
      ).foreach {
        case (value, precision, scale, expectedColumnDisplaySize) =>
          val decimalType = s"DECIMAL($precision,$scale)"
          withExecuteQuery(stmt, s"SELECT cast('$value' as $decimalType)") { rs =>
            assert(rs.next())
            assert(rs.getBigDecimal(1) === new java.math.BigDecimal(value))
            assert(!rs.wasNull)
            assert(!rs.next())

            val metaData = rs.getMetaData
            assert(metaData.getColumnCount === 1)
            assert(metaData.getColumnName(1) === s"CAST($value AS $decimalType)")
            assert(metaData.getColumnLabel(1) === s"CAST($value AS $decimalType)")
            assert(metaData.getColumnType(1) === Types.DECIMAL)
            assert(metaData.getColumnTypeName(1) === decimalType)
            assert(metaData.getColumnClassName(1) === "java.math.BigDecimal")
            assert(metaData.isSigned(1) === true)
            assert(metaData.getPrecision(1) === precision)
            assert(metaData.getScale(1) === scale)
            assert(metaData.getColumnDisplaySize(1) === expectedColumnDisplaySize)
            assert(metaData.getColumnDisplaySize(1) >= value.size)
          }
      }
    }
  }

  test("getter functions column index out of bound") {
    withStatement { stmt =>
      Seq(
        ("'foo'", (rs: ResultSet) => rs.getString(999)),
        ("true", (rs: ResultSet) => rs.getBoolean(999)),
        ("cast(1 AS BYTE)", (rs: ResultSet) => rs.getByte(999)),
        ("cast(1 AS SHORT)", (rs: ResultSet) => rs.getShort(999)),
        ("cast(1 AS INT)", (rs: ResultSet) => rs.getInt(999)),
        ("cast(1 AS BIGINT)", (rs: ResultSet) => rs.getLong(999)),
        ("cast(1 AS FLOAT)", (rs: ResultSet) => rs.getFloat(999)),
        ("cast(1 AS DOUBLE)", (rs: ResultSet) => rs.getDouble(999)),
        ("cast(1 AS DECIMAL(10,5))", (rs: ResultSet) => rs.getBigDecimal(999)),
        ("CAST(X'0A0B0C' AS BINARY)", (rs: ResultSet) => rs.getBytes(999)),
        ("date '2025-11-15'", (rs: ResultSet) => rs.getBytes(999)),
        ("time '12:34:56.123456'", (rs: ResultSet) => rs.getBytes(999)),
        ("timestamp '2025-11-15 10:30:45.123456'", (rs: ResultSet) => rs.getTimestamp(999)),
        ("timestamp_ntz '2025-11-15 10:30:45.789012'", (rs: ResultSet) => rs.getTimestamp(999))
      ).foreach {
        case (query, getter) =>
          withExecuteQuery(stmt, s"SELECT $query") { rs =>
            assert(rs.next())
            val exception = intercept[SQLException] {
              getter(rs)
            }
            assert(exception.getMessage() ===
              "The column index is out of range: 999, number of columns: 1.")
          }
      }
    }
  }

  test("getter functions called after statement closed") {
    withStatement { stmt =>
      Seq(
        ("'foo'", (rs: ResultSet) => rs.getString(1), "foo"),
        ("true", (rs: ResultSet) => rs.getBoolean(1), true),
        ("cast(1 AS BYTE)", (rs: ResultSet) => rs.getByte(1), 1.toByte),
        ("cast(1 AS SHORT)", (rs: ResultSet) => rs.getShort(1), 1.toShort),
        ("cast(1 AS INT)", (rs: ResultSet) => rs.getInt(1), 1.toInt),
        ("cast(1 AS BIGINT)", (rs: ResultSet) => rs.getLong(1), 1.toLong),
        ("cast(1 AS FLOAT)", (rs: ResultSet) => rs.getFloat(1), 1.toFloat),
        ("cast(1 AS DOUBLE)", (rs: ResultSet) => rs.getDouble(1), 1.toDouble),
        ("cast(1 AS DECIMAL(10,5))", (rs: ResultSet) => rs.getBigDecimal(1),
          new java.math.BigDecimal("1.00000")),
        ("CAST(X'0A0B0C' AS BINARY)", (rs: ResultSet) => rs.getBytes(1),
          Array[Byte](0x0A, 0x0B, 0x0C)),
        ("date '2023-11-15'", (rs: ResultSet) => rs.getDate(1),
            java.sql.Date.valueOf("2023-11-15")),
        ("time '12:34:56.123456'", (rs: ResultSet) => rs.getTime(1), {
          val millis = timeToMillis(12, 34, 56, 123)
          new java.sql.Time(millis)
        })
      ).foreach {
        case (query, getter, expectedValue) =>
          var resultSet: Option[ResultSet] = None
          withExecuteQuery(stmt, s"SELECT $query") { rs =>
            assert(rs.next())
            expectedValue match {
              case arr: Array[Byte] =>
                assert(getter(rs).asInstanceOf[Array[Byte]].sameElements(arr))
              case other => assert(getter(rs) === other)
            }
            assert(!rs.wasNull)
            resultSet = Some(rs)
          }
          assert(resultSet.isDefined)
          val exception = intercept[SQLException] {
            getter(resultSet.get)
          }
          assert(exception.getMessage() === "JDBC Statement is closed.")
      }
    }
  }

  test("get date type") {
    withStatement { stmt =>
      // Test basic date type
      withExecuteQuery(stmt, "SELECT date '2023-11-15'") { rs =>
        assert(rs.next())
        assert(rs.getDate(1) === java.sql.Date.valueOf("2023-11-15"))
        assert(!rs.wasNull)
        assert(!rs.next())

        val metaData = rs.getMetaData
        assert(metaData.getColumnCount === 1)
        assert(metaData.getColumnName(1) === "DATE '2023-11-15'")
        assert(metaData.getColumnLabel(1) === "DATE '2023-11-15'")
        assert(metaData.getColumnType(1) === Types.DATE)
        assert(metaData.getColumnTypeName(1) === "DATE")
        assert(metaData.getColumnClassName(1) === "java.sql.Date")
        assert(metaData.isSigned(1) === false)
        assert(metaData.getPrecision(1) === 10)
        assert(metaData.getScale(1) === 0)
        assert(metaData.getColumnDisplaySize(1) === 10)
      }

      // Test date type with null
      withExecuteQuery(stmt, "SELECT cast(null as date)") { rs =>
        assert(rs.next())
        assert(rs.getDate(1) === null)
        assert(rs.wasNull)
        assert(!rs.next())

        val metaData = rs.getMetaData
        assert(metaData.getColumnCount === 1)
        assert(metaData.getColumnName(1) === "CAST(NULL AS DATE)")
        assert(metaData.getColumnLabel(1) === "CAST(NULL AS DATE)")
        assert(metaData.getColumnType(1) === Types.DATE)
        assert(metaData.getColumnTypeName(1) === "DATE")
        assert(metaData.getColumnClassName(1) === "java.sql.Date")
        assert(metaData.isSigned(1) === false)
        assert(metaData.getPrecision(1) === 10)
        assert(metaData.getScale(1) === 0)
        assert(metaData.getColumnDisplaySize(1) === 10)
      }

      // Test date type by column label
      withExecuteQuery(stmt, "SELECT date '2025-11-15' as test_date") { rs =>
        assert(rs.next())
        assert(rs.getDate("test_date") === java.sql.Date.valueOf("2025-11-15"))
        assert(!rs.wasNull)
        assert(!rs.next())
      }
    }
  }

  test("get binary type") {
    withStatement { stmt =>
      // Test basic binary type
      val testBytes = Array[Byte](0x01, 0x02, 0x03, 0x04, 0x05)
      val hexString = testBytes.map(b => "%02X".format(b)).mkString
      withExecuteQuery(stmt, s"SELECT CAST(X'$hexString' AS BINARY)") { rs =>
        assert(rs.next())
        val bytes = rs.getBytes(1)
        assert(bytes !== null)
        assert(bytes.length === testBytes.length)
        assert(bytes.sameElements(testBytes))
        assert(!rs.wasNull)

        val stringValue = rs.getString(1)
        val expectedString = new String(testBytes, java.nio.charset.StandardCharsets.UTF_8)
        assert(stringValue === expectedString)

        assert(!rs.next())

        val metaData = rs.getMetaData
        assert(metaData.getColumnCount === 1)
        assert(metaData.getColumnType(1) === Types.VARBINARY)
        assert(metaData.getColumnTypeName(1) === "BINARY")
        assert(metaData.getColumnClassName(1) === "[B")
        assert(metaData.isSigned(1) === false)
      }

      // Test binary type with UTF-8 text
      val textBytes = "\\xDeAdBeEf".getBytes(java.nio.charset.StandardCharsets.UTF_8)
      val hexString2 = textBytes.map(b => "%02X".format(b)).mkString
      withExecuteQuery(stmt, s"SELECT CAST(X'$hexString2' AS BINARY)") { rs =>
        assert(rs.next())
        val bytes = rs.getBytes(1)
        assert(bytes !== null)
        assert(bytes.sameElements(textBytes))

        val stringValue = rs.getString(1)
        assert(stringValue === "\\xDeAdBeEf")

        assert(!rs.next())
      }

      // Test binary type with null
      withExecuteQuery(stmt, "SELECT cast(null as binary)") { rs =>
        assert(rs.next())
        assert(rs.getBytes(1) === null)
        assert(rs.wasNull)
        assert(!rs.next())

        val metaData = rs.getMetaData
        assert(metaData.getColumnCount === 1)
        assert(metaData.getColumnType(1) === Types.VARBINARY)
        assert(metaData.getColumnTypeName(1) === "BINARY")
        assert(metaData.getColumnClassName(1) === "[B")
      }

      // Test binary type by column label
      val testBytes2 = Array[Byte](0x0A, 0x0B, 0x0C)
      val hexString3 = testBytes2.map(b => "%02X".format(b)).mkString
      withExecuteQuery(stmt, s"SELECT CAST(X'$hexString3' AS BINARY) as test_binary") { rs =>
        assert(rs.next())
        val bytes = rs.getBytes("test_binary")
        assert(bytes !== null)
        assert(bytes.length === testBytes2.length)
        assert(bytes.sameElements(testBytes2))
        assert(!rs.wasNull)

        val stringValue = rs.getString("test_binary")
        val expectedString = new String(testBytes2, java.nio.charset.StandardCharsets.UTF_8)
        assert(stringValue === expectedString)

        assert(!rs.next())

        val metaData = rs.getMetaData
        assert(metaData.getColumnCount === 1)
        assert(metaData.getColumnName(1) === "test_binary")
        assert(metaData.getColumnLabel(1) === "test_binary")
      }

      // Test empty binary
      withExecuteQuery(stmt, "SELECT CAST(X'' AS BINARY)") { rs =>
        assert(rs.next())
        val bytes = rs.getBytes(1)
        assert(bytes !== null)
        assert(bytes.length === 0)
        assert(!rs.wasNull)

        val stringValue = rs.getString(1)
        assert(stringValue === "")
        assert(!rs.next())
      }
    }
  }

  test("get time type") {
    withStatement { stmt =>
      // Test basic time type
      withExecuteQuery(stmt, "SELECT time '12:34:56.123456'") { rs =>
        assert(rs.next())
        val time = rs.getTime(1)
        // Verify milliseconds are preserved (123 from 123456 microseconds)
        val expectedMillis = timeToMillis(12, 34, 56, 123)
        assert(time.getTime === expectedMillis)
        assert(!rs.wasNull)
        assert(!rs.next())

        val metaData = rs.getMetaData
        assert(metaData.getColumnCount === 1)
        assert(metaData.getColumnName(1) === "TIME '12:34:56.123456'")
        assert(metaData.getColumnLabel(1) === "TIME '12:34:56.123456'")
        assert(metaData.getColumnType(1) === Types.TIME)
        assert(metaData.getColumnTypeName(1) === "TIME(6)")
        assert(metaData.getColumnClassName(1) === "java.sql.Time")
        assert(metaData.isSigned(1) === false)
        assert(metaData.getPrecision(1) === 6)
        assert(metaData.getScale(1) === 0)
        assert(metaData.getColumnDisplaySize(1) === 15)
      }

      // Test time type with null
      withExecuteQuery(stmt, "SELECT cast(null as time)") { rs =>
        assert(rs.next())
        assert(rs.getTime(1) === null)
        assert(rs.wasNull)
        assert(!rs.next())

        val metaData = rs.getMetaData
        assert(metaData.getColumnCount === 1)
        assert(metaData.getColumnName(1) === "CAST(NULL AS TIME(6))")
        assert(metaData.getColumnLabel(1) === "CAST(NULL AS TIME(6))")
        assert(metaData.getColumnType(1) === Types.TIME)
        assert(metaData.getColumnTypeName(1) === "TIME(6)")
        assert(metaData.getColumnClassName(1) === "java.sql.Time")
        assert(metaData.isSigned(1) === false)
        assert(metaData.getPrecision(1) === 6)
        assert(metaData.getScale(1) === 0)
        assert(metaData.getColumnDisplaySize(1) === 15)
      }

      // Test time type by column label
      withExecuteQuery(stmt, "SELECT time '09:15:30.456789' as test_time") { rs =>
        assert(rs.next())
        val time = rs.getTime("test_time")
        // Verify milliseconds are preserved (456 from 456789 microseconds)
        val expectedMillis = timeToMillis(9, 15, 30, 456)
        assert(time.getTime === expectedMillis)
        assert(!rs.wasNull)
        assert(!rs.next())
      }
    }
  }

  test("get time type with different precisions") {
    withStatement { stmt =>
      Seq(
        // (timeValue, precision, expectedDisplaySize, expectedMillis)
        // HH:MM:SS (no fractional)
        ("15:45:30.123456", 0, 8, timeToMillis(15, 45, 30, 0)),
        // HH:MM:SS.f (100ms from .1)
        ("10:20:30.123456", 1, 10, timeToMillis(10, 20, 30, 100)),
        // HH:MM:SS.fff (123ms)
        ("08:15:45.123456", 3, 12, timeToMillis(8, 15, 45, 123)),
        // HH:MM:SS.fff (999ms) . Spark TIME values can have microsecond precision,
        // but java.sql.Time can only store up to millisecond precision
        ("23:59:59.999999", 6, 15, timeToMillis(23, 59, 59, 999))
      ).foreach {
        case (timeValue, precision, expectedDisplaySize, expectedMillis) =>
          withExecuteQuery(stmt, s"SELECT cast(time '$timeValue' as time($precision))") { rs =>
            assert(rs.next(), s"Failed to get next row for precision $precision")
            val time = rs.getTime(1)
            assert(time.getTime === expectedMillis,
              s"Time millis mismatch for precision" +
                s" $precision: expected $expectedMillis, got ${time.getTime}")
            assert(!rs.wasNull, s"wasNull should be false for precision $precision")
            assert(!rs.next(), s"Should have no more rows for precision $precision")

            val metaData = rs.getMetaData
            assert(metaData.getColumnCount === 1)
            assert(metaData.getColumnType(1) === Types.TIME,
              s"Column type mismatch for precision $precision")
            assert(metaData.getColumnTypeName(1) === s"TIME($precision)",
              s"Column type name mismatch for precision $precision")
            assert(metaData.getColumnClassName(1) === "java.sql.Time",
              s"Column class name mismatch for precision $precision")
            assert(metaData.getPrecision(1) === precision,
              s"Precision mismatch for precision $precision")
            assert(metaData.getScale(1) === 0,
              s"Scale should be 0 for precision $precision")
            assert(metaData.getColumnDisplaySize(1) === expectedDisplaySize,
              s"Display size mismatch for precision $precision: " +
                s"expected $expectedDisplaySize, got ${metaData.getColumnDisplaySize(1)}")
          }
      }
    }
  }

  test("get date type with spark.sql.datetime.java8API.enabled") {
    withStatement { stmt =>
      Seq(true, false).foreach { java8APIEnabled =>
        stmt.execute(s"set spark.sql.datetime.java8API.enabled=$java8APIEnabled")
        Using.resource(stmt.executeQuery("SELECT date '2025-11-15'")) { rs =>
          assert(rs.next())
          assert(rs.getDate(1) === java.sql.Date.valueOf("2025-11-15"))
          assert(!rs.wasNull)
          assert(!rs.next())
        }
      }
    }
  }

  test("get time type with spark.sql.datetime.java8API.enabled") {
    withStatement { stmt =>
      Seq(true, false).foreach { java8APIEnabled =>
        stmt.execute(s"set spark.sql.datetime.java8API.enabled=$java8APIEnabled")
        Using.resource(stmt.executeQuery("SELECT time '12:34:56.123456'")) { rs =>
          assert(rs.next())
          val time = rs.getTime(1)
          val expectedMillis = timeToMillis(12, 34, 56, 123)
          assert(time.getTime === expectedMillis)
          assert(!rs.wasNull)
          assert(!rs.next())
        }
      }
    }
  }

  test("get timestamp type") {
    withStatement { stmt =>
      // Test basic timestamp type
      withExecuteQuery(stmt, "SELECT timestamp '2025-11-15 10:30:45.123456'") { rs =>
        assert(rs.next())
        val timestamp = rs.getTimestamp(1)
        assert(timestamp !== null)
        assert(timestamp === java.sql.Timestamp.valueOf("2025-11-15 10:30:45.123456"))
        assert(!rs.wasNull)
        assert(!rs.next())

        val metaData = rs.getMetaData
        assert(metaData.getColumnCount === 1)
        assert(metaData.getColumnName(1) === "TIMESTAMP '2025-11-15 10:30:45.123456'")
        assert(metaData.getColumnLabel(1) === "TIMESTAMP '2025-11-15 10:30:45.123456'")
        assert(metaData.getColumnType(1) === Types.TIMESTAMP)
        assert(metaData.getColumnTypeName(1) === "TIMESTAMP")
        assert(metaData.getColumnClassName(1) === "java.sql.Timestamp")
        assert(metaData.isSigned(1) === false)
        assert(metaData.getPrecision(1) === 29)
        assert(metaData.getScale(1) === 6)
        assert(metaData.getColumnDisplaySize(1) === 29)
      }

      // Test timestamp type with null
      withExecuteQuery(stmt, "SELECT cast(null as timestamp)") { rs =>
        assert(rs.next())
        assert(rs.getTimestamp(1) === null)
        assert(rs.wasNull)
        assert(!rs.next())

        val metaData = rs.getMetaData
        assert(metaData.getColumnCount === 1)
        assert(metaData.getColumnName(1) === "CAST(NULL AS TIMESTAMP)")
        assert(metaData.getColumnLabel(1) === "CAST(NULL AS TIMESTAMP)")
        assert(metaData.getColumnType(1) === Types.TIMESTAMP)
        assert(metaData.getColumnTypeName(1) === "TIMESTAMP")
        assert(metaData.getColumnClassName(1) === "java.sql.Timestamp")
        assert(metaData.isSigned(1) === false)
        assert(metaData.getPrecision(1) === 29)
        assert(metaData.getScale(1) === 6)
        assert(metaData.getColumnDisplaySize(1) === 29)
      }

      // Test timestamp type by column label and with calendar
      val tsString = "2025-11-15 10:30:45.987654"
      withExecuteQuery(stmt, s"SELECT timestamp '$tsString' as test_timestamp") { rs =>
        assert(rs.next())

        // Test by column label
        val timestamp = rs.getTimestamp("test_timestamp")
        assert(timestamp !== null)
        assert(timestamp === java.sql.Timestamp.valueOf(tsString))
        assert(!rs.wasNull)

        // Test with calendar - should return same value (Calendar is ignored)
        // Note: Spark Connect handles timezone at server, Calendar param is for API compliance
        val calUTC = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
        val timestampUTC = rs.getTimestamp(1, calUTC)
        assert(timestampUTC !== null)
        assert(timestampUTC.getTime === timestamp.getTime)

        val calPST = java.util.Calendar.getInstance(
          java.util.TimeZone.getTimeZone("America/Los_Angeles"))
        val timestampPST = rs.getTimestamp(1, calPST)
        assert(timestampPST !== null)
        // Same value regardless of calendar
        assert(timestampPST.getTime === timestamp.getTime)
        assert(timestampUTC.getTime === timestampPST.getTime)

        // Test with calendar by label
        val timestampLabel = rs.getTimestamp("test_timestamp", calUTC)
        assert(timestampLabel !== null)
        assert(timestampLabel.getTime === timestamp.getTime)

        // Test with null calendar - returns same value
        val timestampNullCal = rs.getTimestamp(1, null)
        assert(timestampNullCal !== null)
        assert(timestampNullCal.getTime === timestamp.getTime)

        assert(!rs.next())
      }

      // Test timestamp type with calendar for null value
      withExecuteQuery(stmt, "SELECT cast(null as timestamp)") { rs =>
        assert(rs.next())

        // Calendar parameter should not affect null handling
        val cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
        val timestamp = rs.getTimestamp(1, cal)
        assert(timestamp === null)
        assert(rs.wasNull)
        assert(!rs.next())
      }
    }
  }

  test("get timestamp_ntz type") {
    withStatement { stmt =>
      // Test basic timestamp_ntz type
      withExecuteQuery(stmt, "SELECT timestamp_ntz '2025-11-15 10:30:45.123456'") { rs =>
        assert(rs.next())
        val timestamp = rs.getTimestamp(1)
        assert(timestamp !== null)
        assert(timestamp === java.sql.Timestamp.valueOf("2025-11-15 10:30:45.123456"))
        assert(!rs.wasNull)
        assert(!rs.next())

        val metaData = rs.getMetaData
        assert(metaData.getColumnCount === 1)
        assert(metaData.getColumnName(1) === "TIMESTAMP_NTZ '2025-11-15 10:30:45.123456'")
        assert(metaData.getColumnLabel(1) === "TIMESTAMP_NTZ '2025-11-15 10:30:45.123456'")
        assert(metaData.getColumnType(1) === Types.TIMESTAMP)
        assert(metaData.getColumnTypeName(1) === "TIMESTAMP_NTZ")
        assert(metaData.getColumnClassName(1) === "java.sql.Timestamp")
        assert(metaData.isSigned(1) === false)
        assert(metaData.getPrecision(1) === 29)
        assert(metaData.getScale(1) === 6)
        assert(metaData.getColumnDisplaySize(1) === 29)
      }

      // Test timestamp_ntz by label, null, and with calendar - non-null value
      val tsString = "2025-11-15 14:22:33.789456"
      withExecuteQuery(stmt, s"SELECT timestamp_ntz '$tsString' as test_ts_ntz") { rs =>
        assert(rs.next())

        // Test by column label
        val timestamp = rs.getTimestamp("test_ts_ntz")
        assert(timestamp !== null)
        assert(timestamp === java.sql.Timestamp.valueOf(tsString))
        assert(!rs.wasNull)

        // Test with calendar - should return same value (Calendar is ignored)
        val calUTC = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
        val timestampCal = rs.getTimestamp(1, calUTC)
        assert(timestampCal !== null)
        assert(timestampCal.getTime === timestamp.getTime)

        assert(!rs.next())
      }

      // Test timestamp_ntz with null value
      withExecuteQuery(stmt, "SELECT cast(null as timestamp_ntz)") { rs =>
        assert(rs.next())
        assert(rs.getTimestamp(1) === null)
        assert(rs.wasNull)
        assert(!rs.next())
      }
    }
  }

  test("get timestamp types with spark.sql.datetime.java8API.enabled") {
    withStatement { stmt =>
      Seq(true, false).foreach { java8APIEnabled =>
        stmt.execute(s"set spark.sql.datetime.java8API.enabled=$java8APIEnabled")

        Using.resource(stmt.executeQuery(
          """SELECT
            |  timestamp '2025-11-15 10:30:45.123456' as ts,
            |  timestamp_ntz '2025-11-15 14:22:33.789012' as ts_ntz
            |""".stripMargin)) { rs =>
          assert(rs.next())

          // Test TIMESTAMP type
          val timestamp = rs.getTimestamp(1)
          assert(timestamp !== null)
          assert(timestamp === java.sql.Timestamp.valueOf("2025-11-15 10:30:45.123456"))
          assert(!rs.wasNull)

          // Test TIMESTAMP_NTZ type
          val timestampNtz = rs.getTimestamp(2)
          assert(timestampNtz !== null)
          assert(timestampNtz === java.sql.Timestamp.valueOf("2025-11-15 14:22:33.789012"))
          assert(!rs.wasNull)

          assert(!rs.next())
        }
      }
    }
  }
}
