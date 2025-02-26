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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.CompoundBody
import org.apache.spark.sql.catalyst.util.QuotingUtils.toSQLConf
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


/**
 * End-to-end tests for SQL Scripting.
 * This suite is not intended to heavily test the SQL scripting (parser & interpreter) logic.
 * It is rather focused on testing the sql() API - whether it can handle SQL scripts correctly,
 *  results are returned in expected manner, config flags are applied properly, etc.
 * For full functionality tests, see SqlScriptingParserSuite and SqlScriptingInterpreterSuite.
 */
class SqlScriptingE2eSuite extends QueryTest with SharedSparkSession {
  // Helpers
  private def verifySqlScriptResult(
      sqlText: String,
      expected: Seq[Row],
      expectedSchema: Option[StructType] = None): Unit = {
    val df = spark.sql(sqlText)
    checkAnswer(df, expected)

    assert(expectedSchema.forall(_ === df.schema))
  }

  private def verifySqlScriptResultWithNamedParams(
      sqlText: String,
      expected: Seq[Row],
      args: Map[String, Any]): Unit = {
    val df = spark.sql(sqlText, args)
    checkAnswer(df, expected)
  }

  // Tests setup
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.ANSI_ENABLED.key, "true")
      .set(SQLConf.SQL_SCRIPTING_ENABLED.key, "true")
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
        condition = "UNSUPPORTED_FEATURE.SQL_SCRIPTING",
        parameters = Map("sqlScriptingEnabled" -> toSQLConf(SQLConf.SQL_SCRIPTING_ENABLED.key)))
    }
  }

  test("Scripting with exception handlers") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
        |  BEGIN
        |    SELECT flag;
        |    SET VAR flag = 1;
        |  END;
        |  BEGIN
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 2;
        |    END;
        |    SELECT 5;
        |    SELECT 1/0;
        |    SELECT 6;
        |  END;
        |  SELECT 7;
        |  SELECT flag;
        |END
        |""".stripMargin
    verifySqlScriptResult(sqlScript, Seq(Row(2)))
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

  test("script without result statement") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE x INT;
        |  SET x = 1;
        |END
        |""".stripMargin
    verifySqlScriptResult(sqlScript, Seq.empty)
  }

  test("SPARK-51284: script with empty result") {
    withTable("scripting_test_table") {
      val sqlScript =
        """
          |BEGIN
          |  CREATE TABLE scripting_test_table (id INT);
          |  SELECT * FROM scripting_test_table;
          |  DROP TABLE scripting_test_table;
          |END
          |""".stripMargin
      verifySqlScriptResult(
        sqlScript,
        Seq.empty,
        Some(StructType(Seq(StructField("id", IntegerType)).toArray))
      )
    }
  }

  test("empty script") {
    val sqlScript =
      """
        |BEGIN
        |END
        |""".stripMargin
    verifySqlScriptResult(sqlScript, Seq.empty)
  }

  test("named params") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;
        |  IF :param_1 > 10 THEN
        |    SELECT :param_2;
        |  ELSE
        |    SELECT :param_3;
        |  END IF;
        |END""".stripMargin
    // Define a map with SQL parameters
    val args: Map[String, Any] = Map(
      "param_1" -> 5,
      "param_2" -> "greater",
      "param_3" -> "smaller"
    )
    verifySqlScriptResultWithNamedParams(sqlScriptText, Seq(Row("smaller")), args)
  }

  test("positional params") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;
        |  IF ? > 10 THEN
        |    SELECT ?;
        |  ELSE
        |    SELECT ?;
        |  END IF;
        |END""".stripMargin
    // Define an array with SQL parameters in the correct order.
    val args: Array[Any] = Array(5, "greater", "smaller")
    checkError(
      exception = intercept[SqlScriptingException] {
        spark.sql(sqlScriptText, args).asInstanceOf[CompoundBody]
      },
      condition = "UNSUPPORTED_FEATURE.SQL_SCRIPTING_WITH_POSITIONAL_PARAMETERS",
      parameters = Map.empty)
  }

  test("named params with positional params - should fail") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT ?;
        |  IF :param > 10 THEN
        |    SELECT 1;
        |  ELSE
        |    SELECT 2;
        |  END IF;
        |END""".stripMargin
    // Define a map with SQL parameters.
    val args: Map[String, Any] = Map("param" -> 5)
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(sqlScriptText, args).asInstanceOf[CompoundBody]
      },
      condition = "UNBOUND_SQL_PARAMETER",
      parameters = Map("name" -> "_16"),
      context = ExpectedContext(
        fragment = "?",
        start = 16,
        stop = 16))
  }
}
