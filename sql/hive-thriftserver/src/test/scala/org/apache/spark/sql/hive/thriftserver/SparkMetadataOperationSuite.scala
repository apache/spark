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

  private def withHiveQueryResultSet(
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

  test("Spark's own GetTablesOperation(SparkGetTablesOperation)") {

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
      Seq("CREATE TABLE table1(key INT, val STRING)",
        "CREATE TABLE table2(key INT, val STRING)",
        "CREATE VIEW view1 AS SELECT * FROM table2").foreach(statement.execute)

      withHiveQueryResultSet("%", "%", null) { rs =>
        checkResult(Seq("table1", "table2", "view1"), rs)
      }

      withHiveQueryResultSet("%", "table1", null) { rs =>
        checkResult(Seq("table1"), rs)
      }

      withHiveQueryResultSet("%", "table_not_exist", null) { rs =>
        checkResult(Seq.empty, rs)
      }

      withHiveQueryResultSet("%", "%", JArrays.asList("TABLE")) { rs =>
        checkResult(Seq("table1", "table2"), rs)
      }

      withHiveQueryResultSet("%", "%", JArrays.asList("VIEW")) { rs =>
        checkResult(Seq("view1"), rs)
      }

      withHiveQueryResultSet("%", "%", JArrays.asList("TABLE", "VIEW")) { rs =>
        checkResult(Seq("table1", "table2", "view1"), rs)
      }
    }
  }

}
