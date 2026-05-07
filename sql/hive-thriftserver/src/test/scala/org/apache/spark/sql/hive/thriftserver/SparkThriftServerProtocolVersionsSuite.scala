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

package org.apache.spark.sql.hive.thriftserver

import java.sql.{Date, Timestamp}
import java.util.{List => JList, Properties}

import org.apache.hadoop.hive.common.`type`.{HiveIntervalDayTime, HiveIntervalYearMonth}
import org.apache.hive.jdbc.{HiveConnection, HiveQueryResultSet}
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli.GetInfoType
import org.apache.hive.service.rpc.thrift.{TExecuteStatementReq, TGetInfoReq, TGetTablesReq, TOpenSessionReq, TProtocolVersion}
import org.apache.hive.service.rpc.thrift.TCLIService.Client
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import org.apache.spark.sql.catalyst.util.NumberConverter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String

class SparkThriftServerProtocolVersionsSuite extends HiveThriftServer2TestBase {

  override def mode: ServerMode.Value = ServerMode.binary

  def testExecuteStatementWithProtocolVersion(
      version: TProtocolVersion,
      sql: String)(f: HiveQueryResultSet => Unit): Unit = {
    val rawTransport = new TSocket(localhost, serverPort)
    val connection = new HiveConnection(s"jdbc:hive2://$localhost:$serverPort", new Properties)
    val user = System.getProperty("user.name")
    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
    val client = new Client(new TBinaryProtocol(transport))
    transport.open()
    var rs: HiveQueryResultSet = null
    try {
      val clientProtocol = new TOpenSessionReq(version)
      val openResp = client.OpenSession(clientProtocol)
      val sessHandle = openResp.getSessionHandle
      val execReq = new TExecuteStatementReq(sessHandle, sql)
      val execResp = client.ExecuteStatement(execReq)
      val stmtHandle = execResp.getOperationHandle

      // Set the HiveConnection protocol to our testing protocol version.
      // RowSetFactory uses this protocol version to construct different RowSet.
      val protocol = connection.getClass.getDeclaredField("protocol")
      protocol.setAccessible(true)
      protocol.set(connection, version)
      assert(connection.getProtocol === version)

      rs = new HiveQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(stmtHandle).setMaxRows(Int.MaxValue).setFetchSize(Int.MaxValue)
        .build()
      f(rs)
    } finally {
      rs.close()
      connection.close()
      transport.close()
      rawTransport.close()
    }
  }

  def testGetInfoWithProtocolVersion(version: TProtocolVersion): Unit = {
    val rawTransport = new TSocket(localhost, serverPort)
    val connection = new HiveConnection(s"jdbc:hive2://$localhost:$serverPort", new Properties)
    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
    val client = new Client(new TBinaryProtocol(transport))
    transport.open()
    try {
      val clientProtocol = new TOpenSessionReq(version)
      val openResp = client.OpenSession(clientProtocol)
      val sessHandle = openResp.getSessionHandle

      val dbVersionReq = new TGetInfoReq(sessHandle, GetInfoType.CLI_DBMS_VER.toTGetInfoType)
      val dbVersion = client.GetInfo(dbVersionReq).getInfoValue.getStringValue

      val dbNameReq = new TGetInfoReq(sessHandle, GetInfoType.CLI_DBMS_NAME.toTGetInfoType)
      val dbName = client.GetInfo(dbNameReq).getInfoValue.getStringValue

      assert(dbVersion === org.apache.spark.SPARK_VERSION)
      assert(dbName === "Spark SQL")
    } finally {
      connection.close()
      transport.close()
      rawTransport.close()
    }
  }

  def testGetTablesWithProtocolVersion(
      version: TProtocolVersion,
      schema: String,
      tableNamePattern: String,
      tableTypes: JList[String])(f: HiveQueryResultSet => Unit): Unit = {
    val rawTransport = new TSocket(localhost, serverPort)
    val connection = new HiveConnection(s"jdbc:hive2://$localhost:$serverPort", new Properties)
    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
    val client = new Client(new TBinaryProtocol(transport))
    transport.open()
    var rs: HiveQueryResultSet = null
    try {
      val clientProtocol = new TOpenSessionReq(version)
      val openResp = client.OpenSession(clientProtocol)
      val sessHandle = openResp.getSessionHandle
      val getTableReq = new TGetTablesReq(sessHandle)
      getTableReq.setSchemaName(schema)
      getTableReq.setTableName(tableNamePattern)
      getTableReq.setTableTypes(tableTypes)

      val getTableResp = client.GetTables(getTableReq)

      // Set the HiveConnection protocol to our testing protocol version.
      // RowSetFactory uses this protocol version to construct different RowSet.
      val protocol = connection.getClass.getDeclaredField("protocol")
      protocol.setAccessible(true)
      protocol.set(connection, version)
      assert(connection.getProtocol === version)

      rs = new HiveQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(getTableResp.getOperationHandle)
        .build()
      f(rs)
    } finally {
      rs.close()
      connection.close()
      transport.close()
      rawTransport.close()
    }
  }

  TProtocolVersion.values().foreach { version =>
    test(s"$version get byte type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(1 as byte)") { rs =>
        assert(rs.next())
        assert(rs.getByte(1) === 1.toByte)
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "CAST(1 AS TINYINT)")
        assert(metaData.getColumnTypeName(1) === "tinyint")
        assert(metaData.getColumnType(1) === java.sql.Types.TINYINT)
        assert(metaData.getPrecision(1) === 3)
        assert(metaData.getScale(1) === 0)
      }
    }

    test(s"$version get short type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(1 as short)") { rs =>
        assert(rs.next())
        assert(rs.getShort(1) === 1.toShort)
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "CAST(1 AS SMALLINT)")
        assert(metaData.getColumnTypeName(1) === "smallint")
        assert(metaData.getColumnType(1) === java.sql.Types.SMALLINT)
        assert(metaData.getPrecision(1) === 5)
        assert(metaData.getScale(1) === 0)
      }
    }

    test(s"$version get int type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT 1") { rs =>
        assert(rs.next())
        assert(rs.getInt(1) === 1)
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "1")
        assert(metaData.getColumnTypeName(1) === "int")
        assert(metaData.getColumnType(1) === java.sql.Types.INTEGER)
        assert(metaData.getPrecision(1) === 10)
        assert(metaData.getScale(1) === 0)
      }
    }

    test(s"$version get bigint type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(1 as bigint)") { rs =>
        assert(rs.next())
        assert(rs.getLong(1) === 1L)
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "CAST(1 AS BIGINT)")
        assert(metaData.getColumnTypeName(1) === "bigint")
        assert(metaData.getColumnType(1) === java.sql.Types.BIGINT)
        assert(metaData.getPrecision(1) === 19)
        assert(metaData.getScale(1) === 0)
      }
    }

    test(s"$version get float type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(1.2 as float)") { rs =>
        assert(rs.next())
        assert(rs.getFloat(1) === 1.2F)
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "CAST(1.2 AS FLOAT)")
        assert(metaData.getColumnTypeName(1) === "float")
        assert(metaData.getColumnType(1) === java.sql.Types.FLOAT)
        assert(metaData.getPrecision(1) === 7)
        assert(metaData.getScale(1) === 7)
      }
    }

    test(s"$version get double type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(1.2 as double)") { rs =>
        assert(rs.next())
        assert(rs.getDouble(1) === 1.2D)
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "CAST(1.2 AS DOUBLE)")
        assert(metaData.getColumnTypeName(1) === "double")
        assert(metaData.getColumnType(1) === java.sql.Types.DOUBLE)
        assert(metaData.getPrecision(1) === 15)
        assert(metaData.getScale(1) === 15)
      }
    }

    test(s"$version get decimal type") {
      testExecuteStatementWithProtocolVersion(version,
        "SELECT cast(1 as decimal(9, 1)) as col0, 1234.56BD as col1, 0.123 as col2") { rs =>
        assert(rs.next())
        assert(rs.getBigDecimal(1) === new java.math.BigDecimal("1.0"))
        assert(rs.getBigDecimal("col1") === new java.math.BigDecimal("1234.56"))
        assert(rs.getBigDecimal("col2") === new java.math.BigDecimal("0.123"))
        val metaData = rs.getMetaData
        (1 to 3) foreach { i =>
          assert(metaData.getColumnName(i) === s"col${i - 1}")
          assert(metaData.getColumnTypeName(i) === "decimal")
          assert(metaData.getColumnType(i) === java.sql.Types.DECIMAL)
          assert(metaData.getPrecision(i) == 12 - i * 3)
          assert(metaData.getScale(i) == i)
        }
      }
      testExecuteStatementWithProtocolVersion(version,
        "SELECT cast(null as decimal) ") { rs =>
        assert(rs.next())
        assert(rs.getBigDecimal(1) === null)
      }
    }

    test(s"$version get string type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT 'str'") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "str")
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) ==="str")
        assert(metaData.getColumnTypeName(1) === "string")
        assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
        assert(metaData.getPrecision(1) === Int.MaxValue)
        assert(metaData.getScale(1) === 0)
      }
    }

    test(s"$version get char type") {
      testExecuteStatementWithProtocolVersion(version,
        "SELECT cast('char-str' as char(10))") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "char-str")
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) ==="CAST(char-str AS STRING)")
        assert(metaData.getColumnTypeName(1) === "string")
        assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
        assert(metaData.getPrecision(1) === Int.MaxValue)
        assert(metaData.getScale(1) === 0)
      }
    }

    test(s"$version get varchar type") {
      testExecuteStatementWithProtocolVersion(version,
        "SELECT cast('varchar-str' as varchar(10))") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "varchar-str")
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) ==="CAST(varchar-str AS STRING)")
        assert(metaData.getColumnTypeName(1) === "string")
        assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
        assert(metaData.getPrecision(1) === Int.MaxValue)
        assert(metaData.getScale(1) === 0)
      }
    }

    test(s"$version get binary type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast('ABC' as binary)") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "ABC")
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "CAST(ABC AS BINARY)")
        assert(metaData.getColumnTypeName(1) === "binary")
        assert(metaData.getColumnType(1) === java.sql.Types.BINARY)
        assert(metaData.getPrecision(1) === Int.MaxValue)
        assert(metaData.getScale(1) === 0)
      }
      if (!SQLConf.get.ansiEnabled) {
        testExecuteStatementWithProtocolVersion(version, "SELECT cast(49960 as binary)") { rs =>
          assert(rs.next())
          assert(rs.getString(1) === UTF8String.fromBytes(NumberConverter.toBinary(49960)).toString)
        }
      }
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(null as binary)") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === null)
      }
    }

    test(s"$version get boolean type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT true") { rs =>
        assert(rs.next())
        assert(rs.getBoolean(1) === true)
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "true")
        assert(metaData.getColumnTypeName(1) === "boolean")
        assert(metaData.getColumnType(1) === java.sql.Types.BOOLEAN)
        assert(metaData.getPrecision(1) === 1)
        assert(metaData.getScale(1) === 0)
      }
    }

    test(s"$version get date type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast('2019-07-22' as date)") { rs =>
        assert(rs.next())
        assert(rs.getDate(1) === Date.valueOf("2019-07-22"))
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "CAST(2019-07-22 AS DATE)")
        assert(metaData.getColumnTypeName(1) === "date")
        assert(metaData.getColumnType(1) === java.sql.Types.DATE)
        assert(metaData.getPrecision(1) === 10)
        assert(metaData.getScale(1) === 0)
      }
    }

    test(s"$version get timestamp type") {
      testExecuteStatementWithProtocolVersion(version,
        "SELECT cast('2019-07-22 18:14:00' as timestamp)") { rs =>
        assert(rs.next())
        assert(rs.getTimestamp(1) === Timestamp.valueOf("2019-07-22 18:14:00"))
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "CAST(2019-07-22 18:14:00 AS TIMESTAMP)")
        assert(metaData.getColumnTypeName(1) === "timestamp")
        assert(metaData.getColumnType(1) === java.sql.Types.TIMESTAMP)
        assert(metaData.getPrecision(1) === 29)
        assert(metaData.getScale(1) === 9)
      }
    }

    test(s"$version get void") {
      testExecuteStatementWithProtocolVersion(version, "SELECT null") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === null)
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "NULL")
        assert(metaData.getColumnTypeName(1) === "void")
        assert(metaData.getColumnType(1) === java.sql.Types.NULL)
        assert(metaData.getPrecision(1) === 0)
        assert(metaData.getScale(1) === 0)
      }
    }

    test(s"$version get interval type") {
      testExecuteStatementWithProtocolVersion(version,
        "SELECT interval '1' year '2' month") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "1-2")
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "INTERVAL '1-2' YEAR TO MONTH")
        assert(metaData.getColumnTypeName(1) === "interval_year_month")
        assert(metaData.getColumnType(1) === java.sql.Types.OTHER)
        assert(metaData.getPrecision(1) === 11)
        assert(metaData.getScale(1) === 0)
      }
      testExecuteStatementWithProtocolVersion(version,
        "SELECT interval '1' day '2' hour '3' minute '4.005006' second") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "1 02:03:04.005006000")
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "INTERVAL '1 02:03:04.005006' DAY TO SECOND")
        assert(metaData.getColumnTypeName(1) === "interval_day_time")
        assert(metaData.getColumnType(1) === java.sql.Types.OTHER)
        assert(metaData.getPrecision(1) === 29)
        assert(metaData.getScale(1) === 0)
      }
    }

    test(s"$version get array type") {
      testExecuteStatementWithProtocolVersion(
        version, "SELECT array() AS col1, array(1, 2) AS col2") { rs =>
        assert(rs.next())
        assert(rs.getString(2) === "[1,2]")
        assert(rs.getObject("col1") === "[]")
        assert(rs.getObject("col2") === "[1,2]")
        val metaData = rs.getMetaData
        (1 to 2) foreach { i =>
          assert(metaData.getColumnName(i) === s"col$i")
          assert(metaData.getColumnTypeName(i) === "array")
          assert(metaData.getColumnType(i) === java.sql.Types.ARRAY)
          assert(metaData.getPrecision(i) === Int.MaxValue)
          assert(metaData.getScale(i) == 0)
        }
      }
    }

    test(s"$version get map type") {
      testExecuteStatementWithProtocolVersion(version,
        "SELECT map(), map(1, 2, 3, 4)") { rs =>
        assert(rs.next())
        assert(rs.getObject(1) === "{}")
        assert(rs.getObject(2) === "{1:2,3:4}")
        assert(rs.getString(2) === "{1:2,3:4}")
        val metaData = rs.getMetaData
        (1 to 2) foreach { i =>
          assert(metaData.getColumnName(i).startsWith("map("))
          assert(metaData.getColumnTypeName(1) === "map")
          assert(metaData.getColumnType(i) === java.sql.Types.JAVA_OBJECT)
          assert(metaData.getPrecision(i) === Int.MaxValue)
          assert(metaData.getScale(i) == 0)
        }
      }
    }

    test(s"$version get struct type") {
      testExecuteStatementWithProtocolVersion(version,
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
          assert(metaData.getColumnType(i) === java.sql.Types.STRUCT)
          assert(metaData.getPrecision(i) === Int.MaxValue)
          assert(metaData.getScale(i) == 0)
        }
      }
    }

    test(s"$version get info") {
      testGetInfoWithProtocolVersion(version)
    }

    test(s"$version get tables") {
      def checkResult(tableNames: Seq[String], rs: HiveQueryResultSet): Unit = {
        if (tableNames.nonEmpty) {
          for (i <- tableNames.indices) {
            assert(rs.next())
            assert(rs.getString("TABLE_NAME") === tableNames(i))
          }
        } else {
          assert(!rs.next())
        }
      }

      withJdbcStatement("table1", "table2") { statement =>
        Seq(
          "CREATE TABLE table1(key INT, val STRING)",
          "CREATE TABLE table2(key INT, val STRING)").foreach(statement.execute)

        testGetTablesWithProtocolVersion(version, "%", "%", null) { rs =>
          checkResult(Seq("table1", "table2"), rs)
        }

        testGetTablesWithProtocolVersion(version, "%", "table1", null) { rs =>
          checkResult(Seq("table1"), rs)
        }
      }
    }

    test(s"SPARK-35017: $version get day-time interval type") {
      testExecuteStatementWithProtocolVersion(
        version, "SELECT INTERVAL '1 10:11:12' DAY TO SECOND AS dt") { rs =>
        assert(rs.next())
        assert(rs.getObject(1) === new HiveIntervalDayTime(1, 10, 11, 12, 0))
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "dt")
        assert(metaData.getColumnTypeName(1) === "interval_day_time")
        assert(metaData.getColumnType(1) === java.sql.Types.OTHER)
      }
    }

    test(s"SPARK-35018: $version get year-month interval type") {
      testExecuteStatementWithProtocolVersion(
        version, "SELECT INTERVAL '1-1' YEAR TO MONTH AS ym") { rs =>
        assert(rs.next())
        assert(rs.getObject(1) === new HiveIntervalYearMonth(1, 1))
        val metaData = rs.getMetaData
        assert(metaData.getColumnName(1) === "ym")
        assert(metaData.getColumnTypeName(1) === "interval_year_month")
        assert(metaData.getColumnType(1) === java.sql.Types.OTHER)
      }
    }
  }
}
