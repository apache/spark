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

import java.sql.Connection

import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkException
import org.apache.spark.sql.Row

abstract class SharedJDBCIntegrationSuite extends DockerJDBCIntegrationSuite {

  override def beforeAll(): Unit = runIfTestsEnabled(s"Prepare for ${this.getClass.getName}") {
    super.beforeAll()
    var conn: Connection = null
    eventually(connectionTimeout, interval(1.second)) {
      conn = getConnection()
    }
    try {
      createSharedTable(conn)
    } finally {
      conn.close()
    }
  }

  /**
   * Create a table with the same name that can be used to test common functionality
   * in
   * @param conn
   */
  def createSharedTable(conn: Connection): Unit = {
    val batchStmt = conn.createStatement()

    batchStmt.addBatch("CREATE TABLE tbl_shared (x INTEGER)")
    batchStmt.addBatch("INSERT INTO tbl_shared VALUES(1)")

    batchStmt.executeBatch()
    batchStmt.close()
  }

  test("SPARK-52184: Wrap external engine syntax error") {
    val ex = intercept[SparkException] {
      spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("query", "THIS IS NOT VALID SQL").load()
    }

    // Exception should be detected in analysis phase first when we resolve a schema from
    // through JDBC by sending SELECT * FROM (<subquery>) [LIMIT 1][WHERE 1=0] query.
    checkErrorMatchPVals(
      ex,
      condition = "JDBC_EXTERNAL_ENGINE_SYNTAX_ERROR.DURING_OUTPUT_SCHEMA_RESOLUTION",
      parameters = Map(
        "jdbcQuery" -> "SELECT \\* FROM \\(.*",
        "externalEngineError" -> "[\\s\\S]*"
      )
    )
  }

  test("SPARK-53386: Parameter `query` should work when ending with semicolons") {
    val dfSingle = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("query", "SELECT x FROM tbl_shared; ")
      .load()
    checkAnswer(dfSingle, Seq(Row(1)))

    val dfMultiple = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("query", "SELECT x FROM tbl_shared;;;")
      .load()
    checkAnswer(dfMultiple, Seq(Row(1)))
  }
}
