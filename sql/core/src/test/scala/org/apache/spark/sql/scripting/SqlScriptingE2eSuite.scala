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

package org.apache.spark.sql.scripting

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.CompoundBody
import org.apache.spark.sql.catalyst.util.QuotingUtils.toSQLConf
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for SQL Scripting.
 * This suite is not intended to heavily test the SQL scripting (parser & interpreter) logic.
 * It is rather focused on testing the sql() API - whether it can handle SQL scripts correctly,
 *  results are returned in expected manner, config flags are applied properly, etc.
 * For full functionality tests, see SqlScriptingParserSuite and SqlScriptingInterpreterSuite.
 */
class SqlScriptingE2eSuite extends QueryTest with SharedSparkSession {
  // Helpers
  private var originalSqlScriptingConfVal: String = null

  private def verifySqlScriptResult(sqlText: String, expected: Seq[Row]): Unit = {
    val df = spark.sql(sqlText)
    checkAnswer(df, expected)
  }

  // Tests setup
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    originalSqlScriptingConfVal = spark.conf.get(SQLConf.SQL_SCRIPTING_ENABLED.key)
    spark.conf.set(SQLConf.SQL_SCRIPTING_ENABLED.key, "true")
  }

  protected override def afterAll(): Unit = {
    spark.conf.set(SQLConf.SQL_SCRIPTING_ENABLED.key, originalSqlScriptingConfVal)
    super.afterAll()
  }

  // Tests
  test("SQL Scripting not enabled") {
    withSQLConf(SQLConf.SQL_SCRIPTING_ENABLED.key -> "false") {
      val sqlScriptText =
        """
          |BEGIN
          |  SELECT 1;
          |END""".stripMargin
      checkError(
        exception = intercept[SqlScriptingException] {
          spark.sql(sqlScriptText).asInstanceOf[CompoundBody]
        },
        errorClass = "UNSUPPORTED_FEATURE.SQL_SCRIPTING",
        parameters = Map("sqlScriptingEnabled" -> toSQLConf(SQLConf.SQL_SCRIPTING_ENABLED.key)))
    }
  }

  test("single select") {
    val sqlText = "SELECT 1;"
    verifySqlScriptResult(sqlText, Seq(Row(1)))
  }

  test("multiple selects") {
    val sqlText =
      """
        |BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |END""".stripMargin
    verifySqlScriptResult(sqlText, Seq(Row(2)))
  }

  test("multi statement - simple") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          |  CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |  SELECT a FROM t;
          |END
          |""".stripMargin
      verifySqlScriptResult(sqlScript, Seq(Row(1)))
    }
  }

  test("last statement without result") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE x INT;
        |  SET x = 1;
        |  DROP TEMPORARY VARIABLE x;
        |END
        |""".stripMargin
    verifySqlScriptResult(sqlScript, Seq.empty)
  }
}
