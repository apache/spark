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

import java.sql.{Connection, SQLException}

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

  /**
   * Name of a table that does not exist in the database under test.
   */
  protected val nonExistentTableName = "table_that_does_not_exist"

  /**
   * SQLSTATE the driver reports when [[nonExistentTableName]] is read. None when the suite does
   * not pin the dialect's SQLSTATE down.
   */
  protected def nonExistentTableSQLState: Option[String] = None

  /**
   * A user that can open a session but has no read privilege on [[RestrictedUser.table]], together
   * with the JDBC URL needed to reach it. The container is fresh for every suite run, so the
   * suite only has to create the user. None when the suite cannot create such a user.
   */
  protected case class RestrictedUser(
      url: String,
      table: String,
      user: String,
      password: String,
      expectedSQLState: Option[String] = None,
      expectedMessage: Option[String] = None)

  protected def createRestrictedUser(): Option[RestrictedUser] = None

  test("SPARK-59369: a non-existent table is not classified as a syntax error") {
    // The failure has to surface the driver's own SQLException. If the dialect classifies it as a
    // syntax error, resolveTable wraps it in a SparkException and intercept[SQLException] fails.
    val e = intercept[SQLException] {
      spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", nonExistentTableName)
        .load()
    }
    nonExistentTableSQLState.foreach { sqlState =>
      assertResult(sqlState)(e.getSQLState)
    }
  }

  test("SPARK-59369: a missing privilege is not classified as a syntax error") {
    val restricted = createRestrictedUser()
    // Safety net for a suite that neither creates a user nor excludes this test through
    // `excluded`; the six JDBC suites all do one or the other.
    assume(restricted.isDefined, "this dialect cannot create a restricted user")

    val RestrictedUser(url, table, user, password, expectedSQLState, expectedMessage) =
      restricted.get
    // As above, the driver's own SQLException has to win over JDBC_EXTERNAL_ENGINE_SYNTAX_ERROR.
    val e = intercept[SQLException] {
      spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .load()
    }
    expectedSQLState.foreach { sqlState =>
      assertResult(sqlState)(e.getSQLState)
    }
    expectedMessage.foreach { message =>
      assert(e.getMessage.contains(message), s"Unexpected error message: ${e.getMessage}")
    }
  }

  test("SPARK-52184: Wrap external engine syntax error") {
    val ex = intercept[SparkException] {
      spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("query", "THIS IS NOT VALID SQL").load()
    }

    // Exception should be detected in analysis phase first when we resolve a schema from
    // through JDBC by sending SELECT * FROM (<subquery>) [LIMIT 1][WHERE 1=0] query.
    checkError(
      exception = ex,
      condition = "JDBC_EXTERNAL_ENGINE_SYNTAX_ERROR.DURING_OUTPUT_SCHEMA_RESOLUTION",
      sqlState = Some("42000"),
      parameters = Map(
        "jdbcQuery" -> "SELECT \\* FROM \\(.*",
        "externalEngineError" -> "[\\s\\S]+",
        "externalEngineSqlState" -> ".+"
      ),
      matchPVals = true
    )
    ex.getCause match {
      case cause: SQLException =>
        val expectedSqlState =
          Option(cause.getSQLState).filter(_.nonEmpty).getOrElse("unknown")
        assertResult(expectedSqlState) {
          ex.getMessageParameters.get("externalEngineSqlState")
        }
      case other =>
        fail(s"Expected SQLException cause, but got: $other")
    }
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
