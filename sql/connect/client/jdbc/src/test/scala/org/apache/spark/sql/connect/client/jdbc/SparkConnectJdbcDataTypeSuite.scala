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

import java.sql.Types

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
}
