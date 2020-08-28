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

import org.apache.hive.jdbc.{HiveConnection, HiveQueryResultSet}
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli.GetInfoType
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import org.apache.spark.sql.catalyst.util.NumberConverter
import org.apache.spark.unsafe.types.UTF8String

class SparkThriftServerProtocolVersionsSuite extends HiveThriftJdbcTest {

  override def mode: ServerMode.Value = ServerMode.binary

  def testExecuteStatementWithProtocolVersion(
      version: ThriftserverShimUtils.TProtocolVersion,
      sql: String)(f: HiveQueryResultSet => Unit): Unit = {
    val rawTransport = new TSocket("localhost", serverPort)
    val connection = new HiveConnection(s"jdbc:hive2://localhost:$serverPort", new Properties)
    val user = System.getProperty("user.name")
    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
    val client = new ThriftserverShimUtils.Client(new TBinaryProtocol(transport))
    transport.open()
    var rs: HiveQueryResultSet = null
    try {
      val clientProtocol = new ThriftserverShimUtils.TOpenSessionReq(version)
      val openResp = client.OpenSession(clientProtocol)
      val sessHandle = openResp.getSessionHandle
      val execReq = new ThriftserverShimUtils.TExecuteStatementReq(sessHandle, sql)
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

  def testGetInfoWithProtocolVersion(version: ThriftserverShimUtils.TProtocolVersion): Unit = {
    val rawTransport = new TSocket("localhost", serverPort)
    val connection = new HiveConnection(s"jdbc:hive2://localhost:$serverPort", new Properties)
    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
    val client = new ThriftserverShimUtils.Client(new TBinaryProtocol(transport))
    transport.open()
    try {
      val clientProtocol = new ThriftserverShimUtils.TOpenSessionReq(version)
      val openResp = client.OpenSession(clientProtocol)
      val sessHandle = openResp.getSessionHandle

      val dbVersionReq =
        new ThriftserverShimUtils.TGetInfoReq(sessHandle, GetInfoType.CLI_DBMS_VER.toTGetInfoType)
      val dbVersion = client.GetInfo(dbVersionReq).getInfoValue.getStringValue

      val dbNameReq =
        new ThriftserverShimUtils.TGetInfoReq(sessHandle, GetInfoType.CLI_DBMS_NAME.toTGetInfoType)
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
      version: ThriftserverShimUtils.TProtocolVersion,
      schema: String,
      tableNamePattern: String,
      tableTypes: JList[String])(f: HiveQueryResultSet => Unit): Unit = {
    val rawTransport = new TSocket("localhost", serverPort)
    val connection = new HiveConnection(s"jdbc:hive2://localhost:$serverPort", new Properties)
    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
    val client = new ThriftserverShimUtils.Client(new TBinaryProtocol(transport))
    transport.open()
    var rs: HiveQueryResultSet = null
    try {
      val clientProtocol = new ThriftserverShimUtils.TOpenSessionReq(version)
      val openResp = client.OpenSession(clientProtocol)
      val sessHandle = openResp.getSessionHandle
      val getTableReq = new ThriftserverShimUtils.TGetTablesReq(sessHandle)
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

  ThriftserverShimUtils.testedProtocolVersions.foreach { version =>
    test(s"$version get byte type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(1 as byte)") { rs =>
        assert(rs.next())
        assert(rs.getByte(1) === 1.toByte)
      }
    }

    test(s"$version get short type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(1 as short)") { rs =>
        assert(rs.next())
        assert(rs.getShort(1) === 1.toShort)
      }
    }

    test(s"$version get int type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT 1") { rs =>
        assert(rs.next())
        assert(rs.getInt(1) === 1)
      }
    }

    test(s"$version get bigint type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(1 as bigint)") { rs =>
        assert(rs.next())
        assert(rs.getLong(1) === 1L)
      }
    }

    test(s"$version get float type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(1.2 as float)") { rs =>
        assert(rs.next())
        assert(rs.getFloat(1) === 1.2F)
      }
    }

    test(s"$version get double type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(1.2 as double)") { rs =>
        assert(rs.next())
        assert(rs.getDouble(1) === 1.2D)
      }
    }

    test(s"$version get decimal type") {
      testExecuteStatementWithProtocolVersion(version,
        "SELECT cast(1 as decimal(18, 2)) as c") { rs =>
        assert(rs.next())
        assert(rs.getBigDecimal(1) === new java.math.BigDecimal("1.00"))
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
      }
    }

    test(s"$version get char type") {
      testExecuteStatementWithProtocolVersion(version,
        "SELECT cast('char-str' as char(10))") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "char-str")
      }
    }

    test(s"$version get varchar type") {
      testExecuteStatementWithProtocolVersion(version,
        "SELECT cast('varchar-str' as varchar(10))") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "varchar-str")
      }
    }

    test(s"$version get binary type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast('ABC' as binary)") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "ABC")
      }
      testExecuteStatementWithProtocolVersion(version, "SELECT cast(49960 as binary)") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === UTF8String.fromBytes(NumberConverter.toBinary(49960)).toString)
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
      }
    }

    test(s"$version get date type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT cast('2019-07-22' as date)") { rs =>
        assert(rs.next())
        assert(rs.getDate(1) === Date.valueOf("2019-07-22"))
      }
    }

    test(s"$version get timestamp type") {
      testExecuteStatementWithProtocolVersion(version,
        "SELECT cast('2019-07-22 18:14:00' as timestamp)") { rs =>
        assert(rs.next())
        assert(rs.getTimestamp(1) === Timestamp.valueOf("2019-07-22 18:14:00"))
      }
    }

    test(s"$version get void") {
      testExecuteStatementWithProtocolVersion(version, "SELECT null") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === null)
      }
    }

    test(s"$version get interval type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT interval '1' year '2' day") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "1 years 2 days")
      }
    }

    test(s"$version get array type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT array(1, 2)") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "[1,2]")
      }
    }

    test(s"$version get map type") {
      testExecuteStatementWithProtocolVersion(version, "SELECT map(1, 2)") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "{1:2}")
      }
    }

    test(s"$version get struct type") {
      testExecuteStatementWithProtocolVersion(version,
        "SELECT struct('alpha' AS A, 'beta' AS B)") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === """{"A":"alpha","B":"beta"}""")
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
  }
}
