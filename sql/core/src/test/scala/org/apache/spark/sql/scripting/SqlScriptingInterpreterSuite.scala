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

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, QueryTest, Row}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SQL Scripting interpreter tests.
 * Output from the parser is provided to the interpreter.
 * Output from the interpreter (iterator over executable statements) is then checked - statements
 *   are executed and output DataFrames are compared with expected outputs.
 */
class SqlScriptingInterpreterSuite extends QueryTest with SharedSparkSession {
  // Helpers
  private def runSqlScript(sqlText: String): Array[DataFrame] = {
    val interpreter = SqlScriptingInterpreter()
    val compoundBody = spark.sessionState.sqlParser.parseScript(sqlText)
    val executionPlan = interpreter.buildExecutionPlan(compoundBody, spark)
    executionPlan.flatMap {
      case statement: SingleStatementExec =>
        if (statement.isExecuted) {
          None
        } else {
          Some(Dataset.ofRows(spark, statement.parsedPlan, new QueryPlanningTracker))
        }
      case _ => None
    }.toArray
  }

  private def verifySqlScriptResult(sqlText: String, expected: Seq[Seq[Row]]): Unit = {
    val result = runSqlScript(sqlText)
    assert(result.length == expected.length)
    result.zip(expected).foreach { case (df, expectedAnswer) => checkAnswer(df, expectedAnswer) }
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
        Seq(Row(1)) // select
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
        Seq(Row(false)) // select
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
      Seq.empty[Row], // declare var
      Seq.empty[Row], // set var
      Seq(Row(2)), // select
      Seq.empty[Row] // drop var
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
      Seq.empty[Row] // drop var
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("session vars - var out of scope") {
    val varName: String = "testVarName"
    val e = intercept[AnalysisException] {
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
      verifySqlScriptResult(sqlScript, Seq.empty)
    }
    checkError(
      exception = e,
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
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
      Seq.empty[Row], // declare var
      Seq.empty[Row], // set var
      Seq(Row(2)), // select
      Seq.empty[Row], // drop var - explicit
      Seq.empty[Row] // drop var - implicit
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
    val expected = Seq(Seq(Row(42)))
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
    val expected = Seq(Seq(Row(42)))
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

    val expected = Seq(Seq(Row(42)))
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

    val expected = Seq(Seq(Row(43)))
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

    val expected = Seq(Seq(Row(43)))
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

    val expected = Seq(Seq(Row(44)))
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

      val expected = Seq(Seq.empty[Row], Seq.empty[Row], Seq.empty[Row], Seq(Row(43)))
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

      val expected = Seq(Seq.empty[Row], Seq.empty[Row], Seq.empty[Row], Seq(Row(43)))
      verifySqlScriptResult(commands, expected)
    }
  }

  test("cif") {
    val commands =
      """
        |BEGIN
        | CASE
        |   WHEN 1 = 1 THEN
        |     SELECT 42;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq(Seq(Row(42)))
    verifySqlScriptResult(commands, expected)
  }

  test("cif nested") {
    val commands =
      """
        |BEGIN
        | CASE
        |   WHEN 1 = 2 THEN
        |     SELECT 42;
        |   WHEN 1 IN (1,2,3) THEN
        |     SELECT 43;
        |   WHEN (SELECT * FROM t) THEN
        |     SELECT * FROM b;
        |   ELSE
        |     SELECT 3;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq(Seq(Row(43)))
    verifySqlScriptResult(commands, expected)
  }

  test("cif else going in if") {
    val commands =
      """
        |BEGIN
        | CASE (SELECT 2)
        |   WHEN 1 THEN
        |     SELECT 1;
        |   WHEN 2 THEN
        |     SELECT 42;
        |   WHEN (SELECT * FROM t) THEN
        |     SELECT * FROM b;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq(Seq(Row(42)))
    verifySqlScriptResult(commands, expected)
  }

  test("cif else if going in else if") {
    val commands =
      """
        |BEGIN
        | CASE 1
        |   WHEN 2 THEN
        |     SELECT 1;
        |   WHEN 3 THEN
        |     SELECT 2;
        |   ELSE
        |     SELECT 43;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq(Seq(Row(43)))
    verifySqlScriptResult(commands, expected)
  }

  test("cif with count") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |CASE
          | WHEN (SELECT COUNT(*) > 2 FROM t) THEN
          |   SELECT 42;
          | ELSE
          |   SELECT 43;
          | END CASE;
          |END
          |""".stripMargin

      val expected = Seq(Seq.empty[Row], Seq.empty[Row], Seq.empty[Row], Seq(Row(43)))
      verifySqlScriptResult(commands, expected)
    }
  }

  test("cif else if with count") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |  CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |  CASE
          |  WHEN (SELECT COUNT(*) > 2 FROM t) THEN
          |   SELECT 42;
          |  WHEN (SELECT COUNT(*) > 1 FROM t) THEN
          |   SELECT 43;
          |  ELSE
          |    SELECT 44;
          |  END CASE;
          |END
          |""".stripMargin

      val expected = Seq(Seq.empty[Row], Seq.empty[Row], Seq.empty[Row], Seq(Row(43)))
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
        condition = "INVALID_BOOLEAN_STATEMENT",
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
        condition = "BOOLEAN_STATEMENT_WITH_EMPTY_ROW",
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
        condition = "SCALAR_SUBQUERY_TOO_MANY_ROWS",
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
      Seq.empty[Row], // declare i
      Seq(Row(0)), // select i
      Seq.empty[Row], // set i
      Seq(Row(1)), // select i
      Seq.empty[Row], // set i
      Seq(Row(2)), // select i
      Seq.empty[Row], // set i
      Seq.empty[Row] // drop var
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
      Seq.empty[Row], // declare i
      Seq.empty[Row] // drop i
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
      Seq.empty[Row], // declare i
      Seq.empty[Row], // declare j
      Seq.empty[Row], // set j to 0
      Seq(Row(0, 0)), // select i, j
      Seq.empty[Row], // increase j
      Seq(Row(0, 1)), // select i, j
      Seq.empty[Row], // increase j
      Seq.empty[Row], // increase i
      Seq.empty[Row], // set j to 0
      Seq(Row(1, 0)), // select i, j
      Seq.empty[Row], // increase j
      Seq(Row(1, 1)), // select i, j
      Seq.empty[Row], // increase j
      Seq.empty[Row], // increase i
      Seq.empty[Row], // drop j
      Seq.empty[Row] // drop i
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
        Seq.empty[Row], // create table
        Seq(Row(42)), // select
        Seq.empty[Row], // insert
        Seq(Row(42)), // select
        Seq.empty[Row] // insert
      )
      verifySqlScriptResult(commands, expected)
    }
  }

  test("repeat") {
    val commands =
      """
        |BEGIN
        | DECLARE i = 0;
        | REPEAT
        |   SELECT i;
        |   SET VAR i = i + 1;
        | UNTIL
        |   i = 3
        | END REPEAT;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq.empty[Row], // declare i
      Seq(Row(0)), // select i
      Seq.empty[Row], // set i
      Seq(Row(1)), // select i
      Seq.empty[Row], // set i
      Seq(Row(2)), // select i
      Seq.empty[Row], // set i
      Seq.empty[Row] // drop var
    )
    verifySqlScriptResult(commands, expected)
  }

  test("repeat: enters body only once") {
    val commands =
      """
        |BEGIN
        | DECLARE i = 3;
        | REPEAT
        |   SELECT i;
        |   SET VAR i = i + 1;
        | UNTIL
        |   1 = 1
        | END REPEAT;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq.empty[Row], // declare i
      Seq(Row(3)), // select i
      Seq.empty[Row], // set i
      Seq.empty[Row] // drop i
    )
    verifySqlScriptResult(commands, expected)
  }

  test("nested repeat") {
    val commands =
      """
        |BEGIN
        | DECLARE i = 0;
        | DECLARE j = 0;
        | REPEAT
        |   SET VAR j = 0;
        |   REPEAT
        |     SELECT i, j;
        |     SET VAR j = j + 1;
        |   UNTIL j >= 2
        |   END REPEAT;
        |   SET VAR i = i + 1;
        | UNTIL i >= 2
        | END REPEAT;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq.empty[Row], // declare i
      Seq.empty[Row], // declare j
      Seq.empty[Row], // set j to 0
      Seq(Row(0, 0)), // select i, j
      Seq.empty[Row], // increase j
      Seq(Row(0, 1)), // select i, j
      Seq.empty[Row], // increase j
      Seq.empty[Row], // increase i
      Seq.empty[Row], // set j to 0
      Seq(Row(1, 0)), // select i, j
      Seq.empty[Row], // increase j
      Seq(Row(1, 1)), // select i, j
      Seq.empty[Row], // increase j
      Seq.empty[Row], // increase i
      Seq.empty[Row], // drop j
      Seq.empty[Row] // drop i
    )
    verifySqlScriptResult(commands, expected)
  }

  test("repeat with count") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |REPEAT
          |  SELECT 42;
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |UNTIL (SELECT COUNT(*) >= 2 FROM t)
          |END REPEAT;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq(Row(42)), // select
        Seq.empty[Row], // insert
        Seq(Row(42)), // select
        Seq.empty[Row] // insert
      )
      verifySqlScriptResult(commands, expected)
    }
  }

  test("repeat with non boolean condition - constant") {
    val commands =
      """
        |BEGIN
        | DECLARE i = 0;
        | REPEAT
        |   SELECT i;
        |   SET VAR i = i + 1;
        | UNTIL
        |   1
        | END REPEAT;
        |END
        |""".stripMargin

    checkError(
      exception = intercept[SqlScriptingException] (
        runSqlScript(commands)
      ),
      condition = "INVALID_BOOLEAN_STATEMENT",
      parameters = Map("invalidStatement" -> "1")
    )
  }

  test("repeat with empty subquery condition") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          | CREATE TABLE t (a BOOLEAN) USING parquet;
          | REPEAT
          |   SELECT 1;
          | UNTIL
          |   (SELECT * FROM t)
          | END REPEAT;
          |END
          |""".stripMargin

      checkError(
        exception = intercept[SqlScriptingException] (
          runSqlScript(commands)
        ),
        condition = "BOOLEAN_STATEMENT_WITH_EMPTY_ROW",
        parameters = Map("invalidStatement" -> "(SELECT * FROM T)")
      )
    }
  }

  test("repeat with too many rows in subquery condition") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          | CREATE TABLE t (a BOOLEAN) USING parquet;
          | INSERT INTO t VALUES (true);
          | INSERT INTO t VALUES (true);
          | REPEAT
          |   SELECT 1;
          | UNTIL
          |   (SELECT * FROM t)
          | END REPEAT;
          |END
          |""".stripMargin

      checkError(
        exception = intercept[SparkException] (
          runSqlScript(commands)
        ),
        condition = "SCALAR_SUBQUERY_TOO_MANY_ROWS",
        parameters = Map.empty,
        context = ExpectedContext(fragment = "(SELECT * FROM t)", start = 141, stop = 157)
      )
    }
  }

  test("leave compound block") {
    val sqlScriptText =
      """
        |lbl: BEGIN
        |  SELECT 1;
        |  LEAVE lbl;
        |END""".stripMargin
    val expected = Seq(
      Seq(Row(1)) // select
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("leave while loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: WHILE 1 = 1 DO
        |    SELECT 1;
        |    LEAVE lbl;
        |  END WHILE;
        |END""".stripMargin
    val expected = Seq(
      Seq(Row(1)) // select
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("leave repeat loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: REPEAT
        |    SELECT 1;
        |    LEAVE lbl;
        |  UNTIL 1 = 2
        |  END REPEAT;
        |END""".stripMargin
    val expected = Seq(
      Seq(Row(1)) // select 1
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("iterate compound block - should fail") {
    val sqlScriptText =
      """
        |lbl: BEGIN
        |  SELECT 1;
        |  ITERATE lbl;
        |END""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        runSqlScript(sqlScriptText)
      },
      condition = "INVALID_LABEL_USAGE.ITERATE_IN_COMPOUND",
      parameters = Map("labelName" -> "LBL"))
  }

  test("iterate while loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE x INT;
        |  SET x = 0;
        |  lbl: WHILE x < 2 DO
        |    SET x = x + 1;
        |    ITERATE lbl;
        |    SET x = x + 2;
        |  END WHILE;
        |  SELECT x;
        |END""".stripMargin
    val expected = Seq(
      Seq.empty[Row], // declare
      Seq.empty[Row], // set x = 0
      Seq.empty[Row], // set x = 1
      Seq.empty[Row], // set x = 2
      Seq(Row(2)), // select
      Seq.empty[Row] // drop
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("iterate repeat loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE x INT;
        |  SET x = 0;
        |  lbl: REPEAT
        |    SET x = x + 1;
        |    ITERATE lbl;
        |    SET x = x + 2;
        |  UNTIL x > 1
        |  END REPEAT;
        |  SELECT x;
        |END""".stripMargin
    val expected = Seq(
      Seq.empty[Row], // declare
      Seq.empty[Row], // set x = 0
      Seq.empty[Row], // set x = 1
      Seq.empty[Row], // set x = 2
      Seq(Row(2)), // select x
      Seq.empty[Row] // drop
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("leave with wrong label - should fail") {
    val sqlScriptText =
      """
        |lbl: BEGIN
        |  SELECT 1;
        |  LEAVE randomlbl;
        |END""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        runSqlScript(sqlScriptText)
      },
      condition = "INVALID_LABEL_USAGE.DOES_NOT_EXIST",
      parameters = Map("labelName" -> "RANDOMLBL", "statementType" -> "LEAVE"))
  }

  test("iterate with wrong label - should fail") {
    val sqlScriptText =
      """
        |lbl: BEGIN
        |  SELECT 1;
        |  ITERATE randomlbl;
        |END""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        runSqlScript(sqlScriptText)
      },
      condition = "INVALID_LABEL_USAGE.DOES_NOT_EXIST",
      parameters = Map("labelName" -> "RANDOMLBL", "statementType" -> "ITERATE"))
  }

  test("leave outer loop from nested repeat loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: REPEAT
        |    lbl2: REPEAT
        |      SELECT 1;
        |      LEAVE lbl;
        |    UNTIL 1 = 2
        |    END REPEAT;
        |  UNTIL 1 = 2
        |  END REPEAT;
        |END""".stripMargin
    val expected = Seq(
      Seq(Row(1)) // select 1
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("leave outer loop from nested while loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: WHILE 1 = 1 DO
        |    lbl2: WHILE 2 = 2 DO
        |      SELECT 1;
        |      LEAVE lbl;
        |    END WHILE;
        |  END WHILE;
        |END""".stripMargin
    val expected = Seq(
      Seq(Row(1)) // select
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("iterate outer loop from nested while loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE x INT;
        |  SET x = 0;
        |  lbl: WHILE x < 2 DO
        |    SET x = x + 1;
        |    lbl2: WHILE 2 = 2 DO
        |      SELECT 1;
        |      ITERATE lbl;
        |    END WHILE;
        |  END WHILE;
        |  SELECT x;
        |END""".stripMargin
    val expected = Seq(
      Seq.empty[Row], // declare
      Seq.empty[Row], // set x = 0
      Seq.empty[Row], // set x = 1
      Seq(Row(1)), // select 1
      Seq.empty[Row], // set x = 2
      Seq(Row(1)), // select 1
      Seq(Row(2)), // select x
      Seq.empty[Row] // drop
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("nested compounds in loop - leave in inner compound") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE x INT;
        |  SET x = 0;
        |  lbl: WHILE x < 2 DO
        |    SET x = x + 1;
        |    BEGIN
        |      SELECT 1;
        |      lbl2: BEGIN
        |        SELECT 2;
        |        LEAVE lbl2;
        |        SELECT 3;
        |      END;
        |    END;
        |  END WHILE;
        |  SELECT x;
        |END""".stripMargin
    val expected = Seq(
      Seq.empty[Row], // declare
      Seq.empty[Row], // set x = 0
      Seq.empty[Row], // set x = 1
      Seq(Row(1)), // select 1
      Seq(Row(2)), // select 2
      Seq.empty[Row], // set x = 2
      Seq(Row(1)), // select 1
      Seq(Row(2)), // select 2
      Seq(Row(2)), // select x
      Seq.empty[Row] // drop
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("iterate outer loop from nested repeat loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE x INT;
        |  SET x = 0;
        |  lbl: REPEAT
        |    SET x = x + 1;
        |    lbl2: REPEAT
        |      SELECT 1;
        |      ITERATE lbl;
        |    UNTIL 1 = 2
        |    END REPEAT;
        |  UNTIL x > 1
        |  END REPEAT;
        |  SELECT x;
        |END""".stripMargin
    val expected = Seq(
      Seq.empty[Row], // declare
      Seq.empty[Row], // set x = 0
      Seq.empty[Row], // set x = 1
      Seq(Row(1)), // select 1
      Seq.empty[Row], // set x = 2
      Seq(Row(1)), // select 1
      Seq(Row(2)), // select x
      Seq.empty[Row] // drop
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }
}
