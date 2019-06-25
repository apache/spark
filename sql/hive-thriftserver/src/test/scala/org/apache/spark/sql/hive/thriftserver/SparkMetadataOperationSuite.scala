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

import java.util.{Arrays => JArrays, List => JList, Properties}

import org.apache.hive.jdbc.{HiveConnection, HiveQueryResultSet}
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

class SparkMetadataOperationSuite extends HiveThriftJdbcTest {

  override def mode: ServerMode.Value = ServerMode.binary

  test("Spark's own GetSchemasOperation(SparkGetSchemasOperation)") {
    def testGetSchemasOperation(
        catalog: String,
        schemaPattern: String)(f: HiveQueryResultSet => Unit): Unit = {
      val rawTransport = new TSocket("localhost", serverPort)
      val connection = new HiveConnection(s"jdbc:hive2://localhost:$serverPort", new Properties)
      val user = System.getProperty("user.name")
      val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
      val client = new ThriftserverShimUtils.Client(new TBinaryProtocol(transport))
      transport.open()
      var rs: HiveQueryResultSet = null
      try {
        val openResp = client.OpenSession(new ThriftserverShimUtils.TOpenSessionReq)
        val sessHandle = openResp.getSessionHandle
        val schemaReq = new ThriftserverShimUtils.TGetSchemasReq(sessHandle)

        if (catalog != null) {
          schemaReq.setCatalogName(catalog)
        }

        if (schemaPattern == null) {
          schemaReq.setSchemaName("%")
        } else {
          schemaReq.setSchemaName(schemaPattern)
        }

        rs = new HiveQueryResultSet.Builder(connection)
          .setClient(client)
          .setSessionHandle(sessHandle)
          .setStmtHandle(client.GetSchemas(schemaReq).getOperationHandle)
          .build()
        f(rs)
      } finally {
        rs.close()
        connection.close()
        transport.close()
        rawTransport.close()
      }
    }

    def checkResult(dbNames: Seq[String], rs: HiveQueryResultSet): Unit = {
      if (dbNames.nonEmpty) {
        for (i <- dbNames.indices) {
          assert(rs.next())
          assert(rs.getString("TABLE_SCHEM") === dbNames(i))
        }
      } else {
        assert(!rs.next())
      }
    }

    withDatabase("db1", "db2") { statement =>
      Seq("CREATE DATABASE db1", "CREATE DATABASE db2").foreach(statement.execute)

      testGetSchemasOperation(null, "%") { rs =>
        checkResult(Seq("db1", "db2"), rs)
      }
      testGetSchemasOperation(null, "db1") { rs =>
        checkResult(Seq("db1"), rs)
      }
      testGetSchemasOperation(null, "db_not_exist") { rs =>
        checkResult(Seq.empty, rs)
      }
      testGetSchemasOperation(null, "db*") { rs =>
        checkResult(Seq("db1", "db2"), rs)
      }
    }
  }

  test("Spark's own GetTablesOperation(SparkGetTablesOperation)") {
    def testGetTablesOperation(
        schema: String,
        tableNamePattern: String,
        tableTypes: JList[String])(f: HiveQueryResultSet => Unit): Unit = {
      val rawTransport = new TSocket("localhost", serverPort)
      val connection = new HiveConnection(s"jdbc:hive2://localhost:$serverPort", new Properties)
      val user = System.getProperty("user.name")
      val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
      val client = new ThriftserverShimUtils.Client(new TBinaryProtocol(transport))
      transport.open()

      var rs: HiveQueryResultSet = null

      try {
        val openResp = client.OpenSession(new ThriftserverShimUtils.TOpenSessionReq)
        val sessHandle = openResp.getSessionHandle

        val getTableReq = new ThriftserverShimUtils.TGetTablesReq(sessHandle)
        getTableReq.setSchemaName(schema)
        getTableReq.setTableName(tableNamePattern)
        getTableReq.setTableTypes(tableTypes)

        rs = new HiveQueryResultSet.Builder(connection)
          .setClient(client)
          .setSessionHandle(sessHandle)
          .setStmtHandle(client.GetTables(getTableReq).getOperationHandle)
          .build()

        f(rs)
      } finally {
        rs.close()
        connection.close()
        transport.close()
        rawTransport.close()
      }
    }

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

    withJdbcStatement("table1", "table2", "view1") { statement =>
      Seq(
        "CREATE TABLE table1(key INT, val STRING)",
        "CREATE TABLE table2(key INT, val STRING)",
        "CREATE VIEW view1 AS SELECT * FROM table2").foreach(statement.execute)

      testGetTablesOperation("%", "%", null) { rs =>
        checkResult(Seq("table1", "table2", "view1"), rs)
      }

      testGetTablesOperation("%", "table1", null) { rs =>
        checkResult(Seq("table1"), rs)
      }

      testGetTablesOperation("%", "table_not_exist", null) { rs =>
        checkResult(Seq.empty, rs)
      }

      testGetTablesOperation("%", "%", JArrays.asList("TABLE")) { rs =>
        checkResult(Seq("table1", "table2"), rs)
      }

      testGetTablesOperation("%", "%", JArrays.asList("VIEW")) { rs =>
        checkResult(Seq("view1"), rs)
      }

      testGetTablesOperation("%", "%", JArrays.asList("TABLE", "VIEW")) { rs =>
        checkResult(Seq("table1", "table2", "view1"), rs)
      }
    }
  }

  test("Spark's own GetColumnsOperation(SparkGetColumnsOperation)") {
    def testGetColumnsOperation(
        schema: String,
        tableNamePattern: String,
        columnNamePattern: String)(f: HiveQueryResultSet => Unit): Unit = {
      val rawTransport = new TSocket("localhost", serverPort)
      val connection = new HiveConnection(s"jdbc:hive2://localhost:$serverPort", new Properties)
      val user = System.getProperty("user.name")
      val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
      val client = new ThriftserverShimUtils.Client(new TBinaryProtocol(transport))
      transport.open()

      var rs: HiveQueryResultSet = null

      try {
        val openResp = client.OpenSession(new ThriftserverShimUtils.TOpenSessionReq)
        val sessHandle = openResp.getSessionHandle

        val getColumnsReq = new ThriftserverShimUtils.TGetColumnsReq(sessHandle)
        getColumnsReq.setSchemaName(schema)
        getColumnsReq.setTableName(tableNamePattern)
        getColumnsReq.setColumnName(columnNamePattern)

        rs = new HiveQueryResultSet.Builder(connection)
          .setClient(client)
          .setSessionHandle(sessHandle)
          .setStmtHandle(client.GetColumns(getColumnsReq).getOperationHandle)
          .build()

        f(rs)
      } finally {
        rs.close()
        connection.close()
        transport.close()
        rawTransport.close()
      }
    }

    def checkResult(
        columns: Seq[(String, String, String, String, String)],
        rs: HiveQueryResultSet) : Unit = {
      if (columns.nonEmpty) {
        for (i <- columns.indices) {
          assert(rs.next())
          val col = columns(i)
          assert(rs.getString("TABLE_NAME") === col._1)
          assert(rs.getString("COLUMN_NAME") === col._2)
          assert(rs.getString("DATA_TYPE") === col._3)
          assert(rs.getString("TYPE_NAME") === col._4)
          assert(rs.getString("REMARKS") === col._5)
        }
      } else {
        assert(!rs.next())
      }
    }

    withJdbcStatement("table1", "table2", "view1") { statement =>
      Seq(
        "CREATE TABLE table1(key INT comment 'Int column', val STRING comment 'String column')",
        "CREATE TABLE table2(key INT, val DECIMAL comment 'Decimal column')",
        "CREATE VIEW view1 AS SELECT key FROM table1"
      ).foreach(statement.execute)

      testGetColumnsOperation("%", "%", null) { rs =>
        checkResult(
          Seq(
            ("table1", "key", "4", "INT", "Int column"),
            ("table1", "val", "12", "STRING", "String column"),
            ("table2", "key", "4", "INT", ""),
            ("table2", "val", "3", "DECIMAL(10,0)", "Decimal column"),
            ("view1", "key", "4", "INT", "Int column")), rs)
      }

      testGetColumnsOperation("%", "table1", null) { rs =>
        checkResult(
          Seq(
            ("table1", "key", "4", "INT", "Int column"),
            ("table1", "val", "12", "STRING", "String column")), rs)
      }

      testGetColumnsOperation("%", "table1", "key") { rs =>
        checkResult(Seq(("table1", "key", "4", "INT", "Int column")), rs)
      }

      testGetColumnsOperation("%", "table_not_exist", null) { rs =>
        checkResult(Seq.empty, rs)
      }
    }
  }
}
