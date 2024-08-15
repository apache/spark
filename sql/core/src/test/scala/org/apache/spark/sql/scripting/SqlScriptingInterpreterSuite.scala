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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.plans.logical.CompoundBody
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SQL Scripting interpreter tests.
 * Output from the parser is provided to the interpreter.
 * Output from the interpreter (iterator over executable statements) is then checked - statements
 *   are executed and output DataFrames are compared with expected outputs.
 */
class SqlScriptingInterpreterSuite extends SparkFunSuite with SharedSparkSession {
  // Helpers
  private def runSqlScript(sqlText: String): Seq[Array[Row]] = {
    val interpreter = SqlScriptingInterpreter(spark)
    val compoundBody = spark.sessionState.sqlParser.parsePlan(sqlText).asInstanceOf[CompoundBody]
    interpreter.executeInternal(compoundBody).toSeq
  }

  private def verifySqlScriptResult(sqlText: String, expected: Seq[Array[Row]]): Unit = {
    val result = runSqlScript(sqlText)
    assert(result.length == expected.length)
    result.zip(expected).foreach {
      case (actualAnswer, expectedAnswer) =>
        assert(actualAnswer.sameElements(expectedAnswer))
    }
  }

  // Tests setup
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.SQL_SCRIPTING_ENABLED.key, "true")
  }

  protected override def afterAll(): Unit = {
    spark.conf.set(SQLConf.SQL_SCRIPTING_ENABLED.key, "false")
    super.afterAll()
  }

  // Tests
  test("select 1; select 2;") {
    val sqlScript =
      """
        |BEGIN
        |SELECT 1;
        |SELECT 2;
        |END
        |""".stripMargin
    val expected = Seq(
      Array(Row(1)),
      Array(Row(2))
    )
    verifySqlScriptResult(sqlScript, expected)
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
        Array.empty[Row], // create table
        Array.empty[Row], // insert
        Array.empty[Row], // select with filter
        Array(Row(1)) // select
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
        Array.empty[Row], // create table
        Array.empty[Row], // insert #1
        Array.empty[Row], // insert #2
        Array(Row(false)) // select
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("session vars - set and read (SET VAR)") {
    val sqlScript =
      """
        |BEGIN
        |DECLARE var = 1;
        |SET VAR var = var + 1;
        |SELECT var;
        |END
        |""".stripMargin
    val expected = Seq(
      Array.empty[Row], // declare var
      Array.empty[Row], // set var
      Array(Row(2)) // select
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("session vars - set and read (SET)") {
    val sqlScript =
      """
        |BEGIN
        |DECLARE var = 1;
        |SET var = var + 1;
        |SELECT var;
        |END
        |""".stripMargin
    val expected = Seq(
      Array.empty[Row], // declare var
      Array.empty[Row], // set var
      Array(Row(2)) // select
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
      Array.empty[Row], // declare var
      Array(Row(1)), // select
      Array.empty[Row], // declare var
      Array(Row(2)), // select
      Array.empty[Row], // declare var
      Array.empty[Row], // set var
      Array(Row(4)) // select
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("session vars - var out of scope") {
    val varName: String = "testVarName"
    val sqlScript =
      s"""
        |BEGIN
        | BEGIN
        |   DECLARE $varName = 1;
        |   SELECT $varName;
        | END;
        | SELECT $varName;
        |END
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException] {
        runSqlScript(sqlScript)
      },
      errorClass = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> s"`$varName`"),
      context = ExpectedContext(
        fragment = s"$varName",
        start = 79,
        stop = 89)
    )
  }

  test("session vars - drop var statement") {
    val sqlScript =
      """
        |BEGIN
        |DECLARE var = 1;
        |SET VAR var = var + 1;
        |SELECT var;
        |DROP TEMPORARY VARIABLE var;
        |END
        |""".stripMargin
    val expected = Seq(
      Array.empty[Row], // declare var
      Array.empty[Row], // set var
      Array(Row(2)), // select
      Array.empty[Row] // drop var - explicit
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("if") {
    val commands =
      """
        |BEGIN
        | IF 1=1 THEN
        |   SELECT 42;
        | END IF;
        |END
        |""".stripMargin
    val expected = Seq(Array(Row(42)))
    verifySqlScriptResult(commands, expected)
  }

  test("if nested") {
    val commands =
      """
        |BEGIN
        | IF 1=1 THEN
        |   IF 2=1 THEN
        |     SELECT 41;
        |   ELSE
        |     SELECT 42;
        |   END IF;
        | END IF;
        |END
        |""".stripMargin
    val expected = Seq(Array(Row(42)))
    verifySqlScriptResult(commands, expected)
  }

  test("if else going in if") {
    val commands =
      """
        |BEGIN
        | IF 1=1
        | THEN
        |   SELECT 42;
        | ELSE
        |   SELECT 43;
        | END IF;
        |END
        |""".stripMargin

    val expected = Seq(Array(Row(42)))
    verifySqlScriptResult(commands, expected)
  }

  test("if else if going in else if") {
    val commands =
      """
        |BEGIN
        |  IF 1=2
        |  THEN
        |    SELECT 42;
        |  ELSE IF 1=1
        |  THEN
        |    SELECT 43;
        |  ELSE
        |    SELECT 44;
        |  END IF;
        |END
        |""".stripMargin

    val expected = Seq(Array(Row(43)))
    verifySqlScriptResult(commands, expected)
  }

  test("if else going in else") {
    val commands =
      """
        |BEGIN
        | IF 1=2
        | THEN
        |   SELECT 42;
        | ELSE
        |   SELECT 43;
        | END IF;
        |END
        |""".stripMargin

    val expected = Seq(Array(Row(43)))
    verifySqlScriptResult(commands, expected)
  }

  test("if else if going in else") {
    val commands =
      """
        |BEGIN
        |  IF 1=2
        |  THEN
        |    SELECT 42;
        |  ELSE IF 1=3
        |  THEN
        |    SELECT 43;
        |  ELSE
        |    SELECT 44;
        |  END IF;
        |END
        |""".stripMargin

    val expected = Seq(Array(Row(44)))
    verifySqlScriptResult(commands, expected)
  }

  test("if with count") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |IF (SELECT COUNT(*) > 2 FROM t) THEN
          |   SELECT 42;
          | ELSE
          |   SELECT 43;
          | END IF;
          |END
          |""".stripMargin

      val expected = Seq(
        Array.empty[Row], // create
        Array.empty[Row], // insert
        Array.empty[Row], // insert
        Array(Row(43))) // select
      verifySqlScriptResult(commands, expected)
    }
  }

  test("if else if with count") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |  CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |  IF (SELECT COUNT(*) > 2 FROM t) THEN
          |    SELECT 42;
          |  ELSE IF (SELECT COUNT(*) > 1 FROM t) THEN
          |    SELECT 43;
          |  ELSE
          |    SELECT 44;
          |  END IF;
          |END
          |""".stripMargin

      val expected = Seq(
        Array.empty[Row], // create
        Array.empty[Row], // insert
        Array.empty[Row], // insert
        Array(Row(43))) // select
      verifySqlScriptResult(commands, expected)
    }
  }

  test("if's condition must be a boolean statement") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |  IF 1 THEN
          |    SELECT 45;
          |  END IF;
          |END
          |""".stripMargin
      checkError(
        exception = intercept[SqlScriptingException] (
          runSqlScript(commands)
        ),
        errorClass = "INVALID_BOOLEAN_STATEMENT",
        parameters = Map("invalidStatement" -> "1")
      )
    }
  }

  test("if's condition must return a single row data") {
    withTable("t1", "t2") {
      // empty row
      val commands1 =
        """
          |BEGIN
          |  CREATE TABLE t1 (a BOOLEAN) USING parquet;
          |  IF (SELECT * FROM t1) THEN
          |    SELECT 46;
          |  END IF;
          |END
          |""".stripMargin
      checkError(
        exception = intercept[SqlScriptingException] (
          runSqlScript(commands1)
        ),
        errorClass = "BOOLEAN_STATEMENT_WITH_EMPTY_ROW",
        parameters = Map("invalidStatement" -> "(SELECT * FROM T1)")
      )

      // too many rows ( > 1 )
      val commands2 =
        """
          |BEGIN
          |  CREATE TABLE t2 (a BOOLEAN) USING parquet;
          |  INSERT INTO t2 VALUES (true);
          |  INSERT INTO t2 VALUES (true);
          |  IF (SELECT * FROM t2) THEN
          |    SELECT 46;
          |  END IF;
          |END
          |""".stripMargin
      checkError(
        exception = intercept[SparkException] (
          runSqlScript(commands2)
        ),
        errorClass = "SCALAR_SUBQUERY_TOO_MANY_ROWS",
        parameters = Map.empty,
        context = ExpectedContext(fragment = "(SELECT * FROM t2)", start = 121, stop = 138)
      )
    }
  }

  test("while") {
    val commands =
      """
        |BEGIN
        | DECLARE i = 0;
        | WHILE i < 3 DO
        |   SELECT i;
        |   SET VAR i = i + 1;
        | END WHILE;
        |END
        |""".stripMargin

    val expected = Seq(
      Array.empty[Row], // declare i
      Array(Row(0)), // select i
      Array.empty[Row], // set i
      Array(Row(1)), // select i
      Array.empty[Row], // set i
      Array(Row(2)), // select i
      Array.empty[Row] // set i
    )
    verifySqlScriptResult(commands, expected)
  }

  test("while: not entering body") {
    val commands =
      """
        |BEGIN
        | DECLARE i = 3;
        | WHILE i < 3 DO
        |   SELECT i;
        |   SET VAR i = i + 1;
        | END WHILE;
        |END
        |""".stripMargin

    val expected = Seq(
      Array.empty[Row] // declare i
    )
    verifySqlScriptResult(commands, expected)
  }

  test("nested while") {
    val commands =
      """
        |BEGIN
        | DECLARE i = 0;
        | DECLARE j = 0;
        | WHILE i < 2 DO
        |   SET VAR j = 0;
        |   WHILE j < 2 DO
        |     SELECT i, j;
        |     SET VAR j = j + 1;
        |   END WHILE;
        |   SET VAR i = i + 1;
        | END WHILE;
        |END
        |""".stripMargin

    val expected = Seq(
      Array.empty[Row], // declare i
      Array.empty[Row], // declare j
      Array.empty[Row], // set j to 0
      Array(Row(0, 0)), // select i, j
      Array.empty[Row], // increase j
      Array(Row(0, 1)), // select i, j
      Array.empty[Row], // increase j
      Array.empty[Row], // increase i
      Array.empty[Row], // set j to 0
      Array(Row(1, 0)), // select i, j
      Array.empty[Row], // increase j
      Array(Row(1, 1)), // select i, j
      Array.empty[Row], // increase j
      Array.empty[Row] // increase i
    )
    verifySqlScriptResult(commands, expected)
  }

  test("while with count") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |WHILE (SELECT COUNT(*) < 2 FROM t) DO
          |  SELECT 42;
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |END WHILE;
          |END
          |""".stripMargin

      val expected = Seq(
        Array.empty[Row], // create table
        Array(Row(42)), // select
        Array.empty[Row], // insert
        Array(Row(42)), // select
        Array.empty[Row] // insert
      )
      verifySqlScriptResult(commands, expected)
    }
  }
}
