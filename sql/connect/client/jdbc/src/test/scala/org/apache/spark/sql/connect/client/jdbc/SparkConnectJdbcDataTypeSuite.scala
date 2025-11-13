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

import org.apache.spark.sql.connect.client.jdbc.test.JdbcHelper
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}

class SparkConnectJdbcDataTypeSuite extends ConnectFunSuite with RemoteSparkSession
    with JdbcHelper {

  override def jdbcUrl: String = s"jdbc:sc://localhost:$serverPort"

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
      assert(metaData.getPrecision(1) === 255)
      assert(metaData.getScale(1) === 0)
      assert(metaData.getColumnDisplaySize(1) === 255)
    }
  }

  test("get decimal type") {
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
        withExecuteQuery(s"SELECT cast('$value' as $decimalType)") { rs =>
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

  test("getter functions column index out of bound") {
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
      ("CAST(X'0A0B0C' AS BINARY)", (rs: ResultSet) => rs.getBytes(999))
    ).foreach {
      case (query, getter) =>
        withExecuteQuery(s"SELECT $query") { rs =>
          assert(rs.next())
          val exception = intercept[SQLException] {
            getter(rs)
          }
          assert(exception.getMessage() ===
            "The column index is out of range: 999, number of columns: 1.")
        }
    }
  }

  test("getter functions called after statement closed") {
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
        Array[Byte](0x0A, 0x0B, 0x0C))
    ).foreach {
      case (query, getter, expectedValue) =>
        var resultSet: Option[ResultSet] = None
        withExecuteQuery(s"SELECT $query") { rs =>
          assert(rs.next())
          expectedValue match {
            case arr: Array[Byte] => assert(getter(rs).asInstanceOf[Array[Byte]].sameElements(arr))
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

  test("get date type") {
    withExecuteQuery("SELECT date '2023-11-15'") { rs =>
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
  }

  test("get date type with null") {
    withExecuteQuery("SELECT cast(null as date)") { rs =>
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
  }

  test("get date type by column label") {
    withExecuteQuery("SELECT date '2025-11-15' as test_date") { rs =>
      assert(rs.next())
      assert(rs.getDate("test_date") === java.sql.Date.valueOf("2025-11-15"))
      assert(!rs.wasNull)
      assert(!rs.next())
    }
  }

  test("get date type with calendar by column index") {
    withExecuteQuery("SELECT date '2025-11-15'") { rs =>
      assert(rs.next())

      val calUTC = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
      val dateUTC = rs.getDate(1, calUTC)
      assert(dateUTC !== null)
      assert(!rs.wasNull)

      val calPST = java.util.Calendar.getInstance(
        java.util.TimeZone.getTimeZone("America/Los_Angeles"))
      val datePST = rs.getDate(1, calPST)
      assert(datePST !== null)
      assert(!rs.wasNull)
      assert(!rs.next())
    }
  }

  test("get date type with calendar by column label") {
    withExecuteQuery("SELECT date '2025-11-15' as test_date") { rs =>
      assert(rs.next())

      val cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
      val date = rs.getDate("test_date", cal)
      assert(date !== null)
      assert(!rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnName(1) === "test_date")
      assert(metaData.getColumnLabel(1) === "test_date")
    }
  }

  test("get date type with calendar for null value") {
    withExecuteQuery("SELECT cast(null as date)") { rs =>
      assert(rs.next())

      val cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
      val date = rs.getDate(1, cal)
      assert(date === null)
      assert(rs.wasNull)
      assert(!rs.next())
    }
  }

  test("get date type with null calendar") {
    withExecuteQuery("SELECT date '2025-11-15'") { rs =>
      assert(rs.next())

      val date = rs.getDate(1, null)
      assert(date === java.sql.Date.valueOf("2025-11-15"))
      assert(!rs.wasNull)
      assert(!rs.next())
    }
  }

  test("get binary type") {
    val testBytes = Array[Byte](0x01, 0x02, 0x03, 0x04, 0x05)
    val hexString = testBytes.map(b => "%02X".format(b)).mkString
    withExecuteQuery(s"SELECT CAST(X'$hexString' AS BINARY)") { rs =>
      assert(rs.next())
      val bytes = rs.getBytes(1)
      assert(bytes !== null)
      assert(bytes.length === testBytes.length)
      assert(bytes.sameElements(testBytes))
      assert(!rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnType(1) === Types.BINARY)
      assert(metaData.getColumnTypeName(1) === "BINARY")
      assert(metaData.getColumnClassName(1) === "[B")
      assert(metaData.isSigned(1) === false)
    }
  }

  test("get binary type with null") {
    withExecuteQuery("SELECT cast(null as binary)") { rs =>
      assert(rs.next())
      assert(rs.getBytes(1) === null)
      assert(rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnType(1) === Types.BINARY)
      assert(metaData.getColumnTypeName(1) === "BINARY")
      assert(metaData.getColumnClassName(1) === "[B")
    }
  }

  test("get binary type by column label") {
    val testBytes = Array[Byte](0x0A, 0x0B, 0x0C)
    val hexString = testBytes.map(b => "%02X".format(b)).mkString
    withExecuteQuery(s"SELECT CAST(X'$hexString' AS BINARY) as test_binary") { rs =>
      assert(rs.next())
      val bytes = rs.getBytes("test_binary")
      assert(bytes !== null)
      assert(bytes.length === testBytes.length)
      assert(bytes.sameElements(testBytes))
      assert(!rs.wasNull)
      assert(!rs.next())

      val metaData = rs.getMetaData
      assert(metaData.getColumnCount === 1)
      assert(metaData.getColumnName(1) === "test_binary")
      assert(metaData.getColumnLabel(1) === "test_binary")
    }
  }

  test("get empty binary") {
    withExecuteQuery("SELECT CAST(X'' AS BINARY)") { rs =>
      assert(rs.next())
      val bytes = rs.getBytes(1)
      assert(bytes !== null)
      assert(bytes.length === 0)
      assert(!rs.wasNull)
      assert(!rs.next())
    }
  }
}
