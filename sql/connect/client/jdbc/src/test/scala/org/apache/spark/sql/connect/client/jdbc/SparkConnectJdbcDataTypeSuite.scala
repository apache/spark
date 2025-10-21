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

import java.sql.{Date, Timestamp, Types}

import org.apache.spark.sql.connect.client.jdbc.test.ConnectFunSuite
import org.apache.spark.sql.connect.client.jdbc.test.JdbcHelper
import org.apache.spark.sql.connect.client.jdbc.test.RemoteSparkSession
import org.apache.spark.sql.connect.client.jdbc.test.SQLHelper
import org.apache.spark.sql.internal.SqlApiConf

class SparkConnectJdbcDataTypeSuite extends ConnectFunSuite with RemoteSparkSession
    with SQLHelper with JdbcHelper {

  override def jdbcUrl: String = s"jdbc:sc://localhost:$serverPort"

  test("get null type") {
    withExecuteQuery("SELECT null") { rs =>
      assert(rs.next())
      assert(rs.getString(1) === null)
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "NULL")
      assert(metaData.getColumnTypeName(1) === "VOID")
      assert(metaData.getColumnType(1) === Types.NULL)
      // assert(metaData.getPrecision(1) === 0)
      // assert(metaData.getScale(1) === 0)
    }
  }

  test("get boolean type") {
    withExecuteQuery("SELECT true") { rs =>
      assert(rs.next())
      assert(rs.getBoolean(1) === true)
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "true")
      assert(metaData.getColumnTypeName(1) === "BOOLEAN")
      assert(metaData.getColumnType(1) === Types.BOOLEAN)
      // assert(metaData.getPrecision(1) === 1)
      // assert(metaData.getScale(1) === 0)
    }
  }

  test("get byte type") {
    withExecuteQuery("SELECT cast(1 as byte)") { rs =>
      assert(rs.next())
      assert(rs.getByte(1) === 1.toByte)
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "CAST(1 AS TINYINT)")
      assert(metaData.getColumnTypeName(1) === "TINYINT")
      assert(metaData.getColumnType(1) === Types.TINYINT)
      // assert(metaData.getPrecision(1) === 3)
      // assert(metaData.getScale(1) === 0)
    }
  }

  test("get short type") {
    withExecuteQuery("SELECT cast(1 as short)") { rs =>
      assert(rs.next())
      assert(rs.getShort(1) === 1.toShort)
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "CAST(1 AS SMALLINT)")
      assert(metaData.getColumnTypeName(1) === "SMALLINT")
      assert(metaData.getColumnType(1) === Types.SMALLINT)
      // assert(metaData.getPrecision(1) === 5)
      // assert(metaData.getScale(1) === 0)
    }
  }

  test("get int type") {
    withExecuteQuery("SELECT 1") { rs =>
      assert(rs.next())
      assert(rs.getInt(1) === 1)
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "1")
      assert(metaData.getColumnTypeName(1) === "INT")
      assert(metaData.getColumnType(1) === Types.INTEGER)
      // assert(metaData.getPrecision(1) === 10)
      // assert(metaData.getScale(1) === 0)
    }
  }

  test("get bigint type") {
    withExecuteQuery("SELECT cast(1 as bigint)") { rs =>
      assert(rs.next())
      assert(rs.getLong(1) === 1L)
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "CAST(1 AS BIGINT)")
      assert(metaData.getColumnTypeName(1) === "BIGINT")
      assert(metaData.getColumnType(1) === Types.BIGINT)
      // assert(metaData.getPrecision(1) === 19)
      // assert(metaData.getScale(1) === 0)
    }
  }

  test("get float type") {
    withExecuteQuery("SELECT cast(1.2 as float)") { rs =>
      assert(rs.next())
      assert(rs.getFloat(1) === 1.2F)
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "CAST(1.2 AS FLOAT)")
      assert(metaData.getColumnTypeName(1) === "FLOAT")
      assert(metaData.getColumnType(1) === Types.FLOAT)
      // assert(metaData.getPrecision(1) === 7)
      // assert(metaData.getScale(1) === 7)
    }
  }

  test("get double type") {
    withExecuteQuery("SELECT cast(1.2 as double)") { rs =>
      assert(rs.next())
      assert(rs.getDouble(1) === 1.2D)
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "CAST(1.2 AS DOUBLE)")
      assert(metaData.getColumnTypeName(1) === "DOUBLE")
      assert(metaData.getColumnType(1) === Types.DOUBLE)
      // assert(metaData.getPrecision(1) === 15)
      // assert(metaData.getScale(1) === 15)
    }
  }

  ignore("get date type") {
    withExecuteQuery("SELECT cast('2019-07-22' as date)") { rs =>
      assert(rs.next())
      assert(rs.getDate(1) === Date.valueOf("2019-07-22"))
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "CAST(2019-07-22 AS DATE)")
      assert(metaData.getColumnTypeName(1) === "date")
      assert(metaData.getColumnType(1) === Types.DATE)
      assert(metaData.getPrecision(1) === 10)
      assert(metaData.getScale(1) === 0)
    }
  }

  ignore("get timestamp type") {
    withExecuteQuery(
      "SELECT cast('2019-07-22 18:14:00' as timestamp)") { rs =>
      assert(rs.next())
      assert(rs.getTimestamp(1) === Timestamp.valueOf("2019-07-22 18:14:00"))
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "CAST(2019-07-22 18:14:00 AS TIMESTAMP)")
      assert(metaData.getColumnTypeName(1) === "timestamp")
      assert(metaData.getColumnType(1) === Types.TIMESTAMP)
      assert(metaData.getPrecision(1) === 29)
      assert(metaData.getScale(1) === 9)
    }
  }

  test("get string type") {
    withExecuteQuery("SELECT 'str'") { rs =>
      assert(rs.next())
      assert(rs.getString(1) === "str")
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) ==="str")
      assert(metaData.getColumnTypeName(1) === "STRING")
      assert(metaData.getColumnType(1) === Types.VARCHAR)
      // assert(metaData.getPrecision(1) === Int.MaxValue)
      // assert(metaData.getScale(1) === 0)
    }
  }

  ignore("get char type") {
    withExecuteQuery(
      "SELECT cast('char-str' as char(10))") { rs =>
      assert(rs.next())
      assert(rs.getString(1) === "char-str")
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) ==="CAST(char-str AS STRING)")
      assert(metaData.getColumnTypeName(1) === "STRING")
      assert(metaData.getColumnType(1) === Types.VARCHAR)
      // assert(metaData.getPrecision(1) === Int.MaxValue)
      // assert(metaData.getScale(1) === 0)
    }
  }

  ignore("get varchar type") {
    withExecuteQuery(
      "SELECT cast('varchar-str' as varchar(10))") { rs =>
      assert(rs.next())
      assert(rs.getString(1) === "varchar-str")
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) ==="CAST(varchar-str AS STRING)")
      assert(metaData.getColumnTypeName(1) === "STRING")
      assert(metaData.getColumnType(1) === Types.VARCHAR)
      // assert(metaData.getPrecision(1) === Int.MaxValue)
      // assert(metaData.getScale(1) === 0)
    }
  }

  ignore("get binary type") {
    withExecuteQuery("SELECT cast('ABC' as binary)") { rs =>
      assert(rs.next())
      assert(rs.getString(1) === "ABC")
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "CAST(ABC AS BINARY)")
      assert(metaData.getColumnTypeName(1) === "binary")
      assert(metaData.getColumnType(1) === Types.BINARY)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getScale(1) === 0)
    }
    if (!SqlApiConf.get.ansiEnabled) {
      withExecuteQuery("SELECT cast(49960 as binary)") { rs =>
        assert(rs.next())
        // assert(
        //   rs.getString(1) === UTF8String.fromBytes(NumberConverter.toBinary(49960)).toString)
      }
    }
    withExecuteQuery("SELECT cast(null as binary)") { rs =>
      assert(rs.next())
      assert(rs.getString(1) === null)
    }
  }

  ignore("get decimal type") {
    withExecuteQuery(
      "SELECT cast(1 as decimal(9, 1)) as col0, 1234.56BD as col1, 0.123 as col2") { rs =>
      assert(rs.next())
      assert(rs.getBigDecimal(1) === new java.math.BigDecimal("1.0"))
      assert(rs.getBigDecimal("col1") === new java.math.BigDecimal("1234.56"))
      assert(rs.getBigDecimal("col2") === new java.math.BigDecimal("0.123"))
      val metaData = rs.getMetaData
      (1 to 3) foreach { i =>
        assert(metaData.getColumnName(i) === s"col${i - 1}")
        assert(metaData.getColumnTypeName(i) === "decimal")
        assert(metaData.getColumnType(i) === Types.DECIMAL)
        assert(metaData.getPrecision(i) == 12 - i * 3)
        assert(metaData.getScale(i) == i)
      }
    }
    withExecuteQuery(
      "SELECT cast(null as decimal) ") { rs =>
      assert(rs.next())
      assert(rs.getBigDecimal(1) === null)
    }
  }

  ignore("get year-month interval type") {
    withExecuteQuery(
      "SELECT INTERVAL '1-1' YEAR TO MONTH AS ym") { rs =>
      assert(rs.next())
      // assert(rs.getObject(1) === new HiveIntervalYearMonth(1, 1))
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "ym")
      assert(metaData.getColumnTypeName(1) === "interval_year_month")
      assert(metaData.getColumnType(1) === Types.OTHER)
    }
  }

  ignore("get day-time interval type") {
    withExecuteQuery(
      "SELECT INTERVAL '1 10:11:12' DAY TO SECOND AS dt") { rs =>
      assert(rs.next())
      // assert(rs.getObject(1) === new HiveIntervalDayTime(1, 10, 11, 12, 0))
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "dt")
      assert(metaData.getColumnTypeName(1) === "interval_day_time")
      assert(metaData.getColumnType(1) === Types.OTHER)
    }
  }

  ignore("get interval type") {
    withExecuteQuery(
      "SELECT interval '1' year '2' month") { rs =>
      assert(rs.next())
      assert(rs.getString(1) === "1-2")
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "INTERVAL '1-2' YEAR TO MONTH")
      assert(metaData.getColumnTypeName(1) === "interval_year_month")
      assert(metaData.getColumnType(1) === Types.OTHER)
      assert(metaData.getPrecision(1) === 11)
      assert(metaData.getScale(1) === 0)
    }
    withExecuteQuery(
      "SELECT interval '1' day '2' hour '3' minute '4.005006' second") { rs =>
      assert(rs.next())
      assert(rs.getString(1) === "1 02:03:04.005006000")
      val metaData = rs.getMetaData
      assert(metaData.getColumnName(1) === "INTERVAL '1 02:03:04.005006' DAY TO SECOND")
      assert(metaData.getColumnTypeName(1) === "interval_day_time")
      assert(metaData.getColumnType(1) === Types.OTHER)
      assert(metaData.getPrecision(1) === 29)
      assert(metaData.getScale(1) === 0)
    }
  }

  ignore("get array type") {
    withExecuteQuery(
      "SELECT array() AS col1, array(1, 2) AS col2") { rs =>
      assert(rs.next())
      assert(rs.getString(2) === "[1,2]")
      assert(rs.getObject("col1") === "[]")
      assert(rs.getObject("col2") === "[1,2]")
      val metaData = rs.getMetaData
      (1 to 2) foreach { i =>
        assert(metaData.getColumnName(i) === s"col$i")
        assert(metaData.getColumnTypeName(i) === "array")
        assert(metaData.getColumnType(i) === Types.ARRAY)
        assert(metaData.getPrecision(i) === Int.MaxValue)
        assert(metaData.getScale(i) == 0)
      }
    }
  }

  ignore("get struct type") {
    withExecuteQuery(
      "SELECT struct('alpha' AS A, 'beta' AS B) as col0," +
        " struct('1', '2') AS col1, named_struct('a', 2, 'b', 4) AS col2") { rs =>
      assert(rs.next())
      assert(rs.getString(1) === """{"A":"alpha","B":"beta"}""")
      assert(rs.getObject("col1") === """{"col1":"1","col2":"2"}""")
      assert(rs.getObject("col2") === """{"a":2,"b":4}""")
      val metaData = rs.getMetaData
      (1 to 3) foreach { i =>
        assert(metaData.getColumnName(i) === s"col${i - 1}")
        assert(metaData.getColumnTypeName(1) === "struct")
        assert(metaData.getColumnType(i) === Types.STRUCT)
        assert(metaData.getPrecision(i) === Int.MaxValue)
        assert(metaData.getScale(i) == 0)
      }
    }
  }

  ignore("get map type") {
    withExecuteQuery(
      "SELECT map(), map(1, 2, 3, 4)") { rs =>
      assert(rs.next())
      assert(rs.getObject(1) === "{}")
      assert(rs.getObject(2) === "{1:2,3:4}")
      assert(rs.getString(2) === "{1:2,3:4}")
      val metaData = rs.getMetaData
      (1 to 2) foreach { i =>
        assert(metaData.getColumnName(i).startsWith("map("))
        assert(metaData.getColumnTypeName(1) === "map")
        assert(metaData.getColumnType(i) === Types.JAVA_OBJECT)
        assert(metaData.getPrecision(i) === Int.MaxValue)
        assert(metaData.getScale(i) == 0)
      }
    }
  }

}
