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

import org.apache.spark.sql.{Dataset, QueryTest, Row}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.test.SharedSparkSession

class SqlScriptingIntegrationSuite extends QueryTest with SharedSparkSession {
  // Helpers
  private def verifySqlScriptResult(
      sqlText: String, expected: Seq[Seq[Row]], printResult: Boolean = false): Unit = {
    val interpreter = SqlScriptingInterpreter()
    val compoundBody = spark.sessionState.sqlParser.parseScript(sqlText)
    val executionPlan = interpreter.buildExecutionPlan(compoundBody)
    val result = executionPlan.flatMap {
      case statement: SingleStatementExec =>
        if (printResult) {
          // scalastyle:off println
          println("Executing: " + statement.getText(sqlText))
          // scalastyle:on println
        }

        if (statement.consumed) {
          None
        } else {
          Some(Dataset.ofRows(spark, statement.parsedPlan, new QueryPlanningTracker))
        }
      case _ => None
    }.toArray

    assert(result.length == expected.length)
    result.zip(expected).foreach{ case (df, expectedAnswer) => checkAnswer(df, expectedAnswer)}
  }

  // Tests
  test("select 1") {
    verifySqlScriptResult("SELECT 1;", Seq(Seq(Row(1))))
  }

  test("multi statement - simple") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |SELECT a, b FROM t WHERE a = 12;
          |SELECT a FROM t;
          |END
          |""".stripMargin
      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // select with filter
        Seq(Row(1))
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("multi statement - count") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |SELECT
          | CASE WHEN COUNT(*) > 10 THEN true
          | ELSE false
          | END AS MoreThanTen
          |FROM t;
          |END
          |""".stripMargin
      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert #1
        Seq.empty[Row], // insert #2
        Seq(Row(false))
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("session vars - set and read") {
    val sqlScript =
      """
        |BEGIN
        |DECLARE var = 1;
        |SET VAR var = var + 1;
        |SELECT var;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq.empty[Row], // declare var
      Seq.empty[Row], // set var
      Seq(Row(2)), // select
      Seq.empty[Row] // drop var
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("session vars - set and read scoped") {
    val sqlScript =
      """
        |BEGIN
        | BEGIN
        |   DECLARE var = 1;
        |   SELECT var;
        | END;
        | BEGIN
        |   DECLARE var = 2;
        |   SELECT var;
        | END;
        | BEGIN
        |   DECLARE var = 3;
        |   SET VAR var = var + 1;
        |   SELECT var;
        | END;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq.empty[Row], // declare var
      Seq(Row(1)), // select
      Seq.empty[Row], // drop var
      Seq.empty[Row], // declare var
      Seq(Row(2)), // select
      Seq.empty[Row], // drop var
      Seq.empty[Row], // declare var
      Seq.empty[Row], // set var
      Seq(Row(4)), // select
      Seq.empty[Row], // drop var
    )
    verifySqlScriptResult(sqlScript, expected)
  }
}
