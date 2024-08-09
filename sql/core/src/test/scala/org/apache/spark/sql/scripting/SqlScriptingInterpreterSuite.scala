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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{AnalysisException, Row}
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
  private def verifySqlScriptResult(sqlText: String, expected: Seq[Array[Row]]): Unit = {
    val interpreter = SqlScriptingInterpreter(spark)
    val compoundBody = spark.sessionState.sqlParser.parseScript(sqlText)
    val result = interpreter.execute(compoundBody).toSeq
    assert(result.length == expected.length)
    result.zip(expected).foreach {
      case (actualAnswer, expectedAnswer) =>
        assert(actualAnswer.sameElements(expectedAnswer))
    }
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.SQL_SCRIPTING_ENABLED.key, "true")
  }

  protected override def afterAll(): Unit = {
    spark.conf.set(SQLConf.SQL_SCRIPTING_ENABLED.key, "false")
    super.afterAll()
  }

  // Tests
  test("select 1") {
    verifySqlScriptResult("SELECT 1;", Seq(Array(Row(1))))
  }

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
      Array(Row(2)), // select
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
      Array(Row(2)), // select
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
      Array(Row(4)), // select
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
      Array.empty[Row], // drop var - explicit
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("duplicate handler") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE flag INT = -1;
        |  DECLARE zero_division CONDITION FOR '22012';
        |  DECLARE CONTINUE HANDLER FOR zero_division
        |  BEGIN
        |    SET VAR flag = 1;
        |  END;
        |  DECLARE CONTINUE HANDLER FOR '22012'
        |  BEGIN
        |    SET VAR flag = 2;
        |  END;
        |  SELECT 1/0;
        |  SELECT flag;
        |END
        |""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        verifySqlScriptResult(sqlScript, Seq.empty)
      },
      errorClass = "DUPLICATE_HANDLER_FOR_SAME_SQL_STATE",
      parameters = Map("sqlState" -> "22012"))
  }

  test("handler - continue resolve in the same block") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE flag INT = -1;
        |  DECLARE zero_division CONDITION FOR '22012';
        |  DECLARE CONTINUE HANDLER FOR zero_division
        |  BEGIN
        |    SELECT flag;
        |    SET VAR flag = 1;
        |  END;
        |  SELECT 2;
        |  SELECT 3;
        |  SELECT 1/0;
        |  SELECT 4;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Array.empty[Row], // declare var
      Array(Row(2)),    // select
      Array(Row(3)),    // select
      Array(Row(-1)),   // select flag
      Array.empty[Row], // set flag
      Array(Row(4)),    // select
      Array(Row(1)),    // select
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("handler - continue resolve in outer block") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE flag INT = -1;
        |  DECLARE zero_division CONDITION FOR '22012';
        |  DECLARE CONTINUE HANDLER FOR zero_division
        |  BEGIN
        |    SELECT flag;
        |    SET VAR flag = 1;
        |  END;
        |  SELECT 2;
        |  BEGIN
        |    SELECT 3;
        |    BEGIN
        |      SELECT 4;
        |      SELECT 1/0;
        |      SELECT 5;
        |    END;
        |    SELECT 6;
        |  END;
        |  SELECT 7;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Array.empty[Row], // declare var
      Array(Row(2)),    // select
      Array(Row(3)),    // select
      Array(Row(4)),    // select
      Array(Row(-1)),   // select flag
      Array.empty[Row], // set flag
      Array(Row(5)),    // select
      Array(Row(6)),    // select
      Array(Row(7)),    // select
      Array(Row(1)),    // select
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("handler - continue resolve in the same block nested") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE flag INT = -1;
        |  SELECT 2;
        |  BEGIN
        |    SELECT 3;
        |    BEGIN
        |      DECLARE zero_division CONDITION FOR '22012';
        |      DECLARE CONTINUE HANDLER FOR zero_division
        |      BEGIN
        |        SELECT flag;
        |        SET VAR flag = 1;
        |      END;
        |      SELECT 4;
        |      SELECT 1/0;
        |      SELECT 5;
        |    END;
        |    SELECT 6;
        |  END;
        |  SELECT 7;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Array.empty[Row], // declare var
      Array(Row(2)),    // select
      Array(Row(3)),    // select
      Array(Row(4)),    // select
      Array(Row(-1)),   // select flag
      Array.empty[Row], // set flag
      Array(Row(5)),    // select
      Array(Row(6)),    // select
      Array(Row(7)),    // select
      Array(Row(1)),    // select
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("handler - exit resolve in the same block") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE flag INT = -1;
        |  BEGIN
        |    DECLARE zero_division CONDITION FOR '22012';
        |    DECLARE EXIT HANDLER FOR zero_division
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 1;
        |    END;
        |    SELECT 2;
        |    SELECT 3;
        |    SELECT 1/0;
        |    SELECT 4;
        |  END;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Array.empty[Row], // declare var
      Array(Row(2)),    // select
      Array(Row(3)),    // select
      Array(Row(-1)),   // select flag
      Array.empty[Row], // set flag
      Array(Row(1)),    // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("handler - exit resolve in outer block") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE flag INT = -1;
        |  BEGIN
        |    DECLARE zero_division CONDITION FOR '22012';
        |    DECLARE EXIT HANDLER FOR zero_division
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 1;
        |    END;
        |    SELECT 2;
        |    SELECT 3;
        |    BEGIN
        |      SELECT 4;
        |      SELECT 1/0;
        |      SELECT 5;
        |    END;
        |    SELECT 6;
        |  END;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Array.empty[Row], // declare var
      Array(Row(2)),    // select
      Array(Row(3)),    // select
      Array(Row(4)),    // select
      Array(Row(-1)),   // select flag
      Array.empty[Row], // set flag
                        // skip select 5
                        // skip select 6
      Array(Row(1)),    // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("handler - continue resolve by the CATCH ALL handler") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE flag INT = -1;
        |  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        |  BEGIN
        |    SELECT flag;
        |    SET VAR flag = 1;
        |  END;
        |  SELECT 2;
        |  SELECT 1/0;
        |  SELECT 3;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Array.empty[Row], // declare var
      Array(Row(2)),    // select
      Array(Row(-1)),   // select flag
      Array.empty[Row], // set flag
      Array(Row(3)),    // select
      Array(Row(1)),    // select
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("chained begin end blocks") {
    val sqlScript =
      """
        |BEGIN
        | BEGIN
        |   SELECT 1;
        |   SELECT 2;
        | END;
        | BEGIN
        |   SELECT 3;
        |   SELECT 4;
        | END;
        | BEGIN
        |   SELECT 5;
        |   SELECT 6;
        | END;
        |END
        |""".stripMargin
    val expected = Seq(
      Array(Row(1)), // select
      Array(Row(2)), // select
      Array(Row(3)), // select
      Array(Row(4)), // select
      Array(Row(5)), // select
      Array(Row(6))  // select
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

      val expected = Seq(Array.empty[Row], Array.empty[Row], Array.empty[Row], Array(Row(43)))
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

      val expected = Seq(Array.empty[Row], Array.empty[Row], Array.empty[Row], Array(Row(43)))
      verifySqlScriptResult(commands, expected)
    }
  }
}
