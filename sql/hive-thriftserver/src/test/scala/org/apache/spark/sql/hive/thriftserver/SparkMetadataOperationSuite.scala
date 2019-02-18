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

import org.apache.hive.jdbc.{HiveConnection, HiveQueryResultSet, Utils => JdbcUtils}
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli.thrift._
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
      val client = new TCLIService.Client(new TBinaryProtocol(transport))
      transport.open()
      var rs: HiveQueryResultSet = null
      try {
        val openResp = client.OpenSession(new TOpenSessionReq)
        val sessHandle = openResp.getSessionHandle
        val schemaReq = new TGetSchemasReq(sessHandle)

        if (catalog != null) {
          schemaReq.setCatalogName(catalog)
        }

        if (schemaPattern == null) {
          schemaReq.setSchemaName("%")
        } else {
          schemaReq.setSchemaName(schemaPattern)
        }

        val schemaResp = client.GetSchemas(schemaReq)
        JdbcUtils.verifySuccess(schemaResp.getStatus)

        rs = new HiveQueryResultSet.Builder(connection)
          .setClient(client)
          .setSessionHandle(sessHandle)
          .setStmtHandle(schemaResp.getOperationHandle)
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
      val client = new TCLIService.Client(new TBinaryProtocol(transport))
      transport.open()

      var rs: HiveQueryResultSet = null

      try {
        val openResp = client.OpenSession(new TOpenSessionReq)
        val sessHandle = openResp.getSessionHandle

        val getTableReq = new TGetTablesReq(sessHandle)
        getTableReq.setSchemaName(schema)
        getTableReq.setTableName(tableNamePattern)
        getTableReq.setTableTypes(tableTypes)

        val getTableResp = client.GetTables(getTableReq)

        JdbcUtils.verifySuccess(getTableResp.getStatus)

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
}
