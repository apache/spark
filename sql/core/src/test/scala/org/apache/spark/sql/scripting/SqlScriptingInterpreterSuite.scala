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

import org.apache.spark.{SparkConf, SparkException, SparkNumberFormatException}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, QueryTest, Row}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.expressions.Expression
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
class SqlScriptingInterpreterSuite extends QueryTest with SharedSparkSession {

  // Tests setup
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.SQL_SCRIPTING_ENABLED.key, "true")
  }

  // Helpers
  private def runSqlScript(
      sqlText: String,
      args: Map[String, Expression] = Map.empty): Array[DataFrame] = {
    val interpreter = SqlScriptingInterpreter(spark)
    val compoundBody = spark.sessionState.sqlParser.parsePlan(sqlText).asInstanceOf[CompoundBody]

    // Initialize context so scopes can be entered correctly.
    val context = new SqlScriptingExecutionContext()
    val executionPlan = interpreter.buildExecutionPlan(compoundBody, args, context)
    context.frames.append(new SqlScriptingExecutionFrame(executionPlan.getTreeIterator))
    executionPlan.enterScope()

    executionPlan.getTreeIterator.flatMap {
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

  test("empty begin end block") {
    val sqlScript =
      """
        |BEGIN
        |END
        |""".stripMargin
    val expected = Seq.empty[Seq[Row]]
    verifySqlScriptResult(sqlScript, expected)
  }

  test("empty begin end blocks") {
    val sqlScript =
      """
        |BEGIN
        | BEGIN
        | END;
        | BEGIN
        | END;
        |END
        |""".stripMargin
    val expected = Seq.empty[Seq[Row]]
    verifySqlScriptResult(sqlScript, expected)
  }

  test("empty begin end blocks with single statement") {
    val sqlScript =
      """
        |BEGIN
        | BEGIN
        | END;
        | SELECT 1;
        | BEGIN
        | END;
        |END
        |""".stripMargin
    val expected = Seq(Seq(Row(1)))
    verifySqlScriptResult(sqlScript, expected)
  }

  test("empty begin end blocks - nested") {
    val sqlScript =
      """
        |BEGIN
        | BEGIN
        |   BEGIN
        |   END;
        |   BEGIN
        |   END;
        | END;
        |END
        |""".stripMargin
    val expected = Seq.empty[Seq[Row]]
    verifySqlScriptResult(sqlScript, expected)
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

  test("if - empty body") {
    val commands =
      """
        |BEGIN
        | IF 1=1 THEN
        |   BEGIN
        |   END;
        | END IF;
        |END
        |""".stripMargin
    val expected = Seq.empty[Seq[Row]]
    verifySqlScriptResult(commands, expected)
  }

  test("if - nested empty body") {
    val commands =
      """
        |BEGIN
        | IF 1=1 THEN
        |   BEGIN
        |     BEGIN
        |     END;
        |   END;
        |   BEGIN
        |     BEGIN
        |     END;
        |   END;
        | END IF;
        |END
        |""".stripMargin
    val expected = Seq.empty[Seq[Row]]
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

  test("searched case") {
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

  test("searched case - empty body") {
    val commands =
      """
        |BEGIN
        | CASE
        |   WHEN 1 = 1 THEN
        |     BEGIN
        |     END;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq.empty[Seq[Row]]
    verifySqlScriptResult(commands, expected)
  }

  test("searched case - nested empty body") {
    val commands =
      """
        |BEGIN
        | CASE
        |   WHEN 1 = 1 THEN
        |     BEGIN
        |       BEGIN
        |       END;
        |     END;
        |     BEGIN
        |       BEGIN
        |       END;
        |     END;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq.empty[Seq[Row]]
    verifySqlScriptResult(commands, expected)
  }

  test("searched case nested") {
    val commands =
      """
        |BEGIN
        | CASE
        |   WHEN 1=1 THEN
        |   CASE
        |    WHEN 2=1 THEN
        |     SELECT 41;
        |   ELSE
        |     SELECT 42;
        |   END CASE;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq(Seq(Row(42)))
    verifySqlScriptResult(commands, expected)
  }

  test("searched case second case") {
    val commands =
      """
        |BEGIN
        | CASE
        |   WHEN 1 = (SELECT 2) THEN
        |     SELECT 1;
        |   WHEN 2 = 2 THEN
        |     SELECT 42;
        |   WHEN (SELECT * FROM t) THEN
        |     SELECT * FROM b;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq(Seq(Row(42)))
    verifySqlScriptResult(commands, expected)
  }

  test("searched case going in else") {
    val commands =
      """
        |BEGIN
        | CASE
        |   WHEN 2 = 1 THEN
        |     SELECT 1;
        |   WHEN 3 IN (1,2) THEN
        |     SELECT 2;
        |   ELSE
        |     SELECT 43;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq(Seq(Row(43)))
    verifySqlScriptResult(commands, expected)
  }

  test("searched case with count") {
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

  test("searched case else with count") {
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

  test("searched case no cases matched no else") {
    val commands =
      """
        |BEGIN
        | CASE
        |   WHEN 1 = 2 THEN
        |     SELECT 42;
        |   WHEN 1 = 3 THEN
        |     SELECT 43;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq()
    verifySqlScriptResult(commands, expected)
  }

  test("searched case when evaluates to null") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |  CREATE TABLE t (a BOOLEAN) USING parquet;
          |  CASE
          |  WHEN (SELECT * FROM t) THEN
          |   SELECT 42;
          |  END CASE;
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

  test("searched case with non boolean condition - constant") {
    val commands =
      """
        |BEGIN
        |  CASE
        |  WHEN 1 THEN
        |   SELECT 42;
        |  END CASE;
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

  test("searched case with too many rows in subquery condition") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          | CREATE TABLE t (a BOOLEAN) USING parquet;
          | INSERT INTO t VALUES (true);
          | INSERT INTO t VALUES (true);
          | CASE
          |   WHEN (SELECT * FROM t) THEN
          |     SELECT 1;
          | END CASE;
          |END
          |""".stripMargin

      checkError(
        exception = intercept[SparkException] (
          runSqlScript(commands)
        ),
        condition = "SCALAR_SUBQUERY_TOO_MANY_ROWS",
        parameters = Map.empty,
        context = ExpectedContext(fragment = "(SELECT * FROM t)", start = 124, stop = 140)
      )
    }
  }

  test("simple case") {
    val commands =
      """
        |BEGIN
        | CASE 1
        |   WHEN 1 THEN
        |     SELECT 42;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq(Seq(Row(42)))
    verifySqlScriptResult(commands, expected)
  }

  test("simple case - empty body") {
    val commands =
      """
        |BEGIN
        | CASE 1
        |   WHEN 1 THEN
        |     BEGIN
        |     END;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq.empty[Seq[Row]]
    verifySqlScriptResult(commands, expected)
  }

  test("simple case - nested empty body") {
    val commands =
      """
        |BEGIN
        | CASE 1
        |   WHEN 1 THEN
        |     BEGIN
        |       BEGIN
        |       END;
        |     END;
        |     BEGIN
        |       BEGIN
        |       END;
        |     END;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq.empty[Seq[Row]]
    verifySqlScriptResult(commands, expected)
  }

  test("simple case nested") {
    val commands =
      """
        |BEGIN
        | CASE 1
        |   WHEN 1 THEN
        |   CASE 2
        |    WHEN (SELECT 3) THEN
        |     SELECT 41;
        |   ELSE
        |     SELECT 42;
        |   END CASE;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq(Seq(Row(42)))
    verifySqlScriptResult(commands, expected)
  }

  test("simple case second case") {
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

  test("simple case going in else") {
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

  test("simple case with count") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |INSERT INTO t VALUES (1, 'a', 1.0);
          |CASE (SELECT COUNT(*) FROM t)
          | WHEN 1 THEN
          |   SELECT 41;
          | WHEN 2 THEN
          |   SELECT 42;
          | ELSE
          |   SELECT 43;
          | END CASE;
          |END
          |""".stripMargin

      val expected = Seq(Seq.empty[Row], Seq.empty[Row], Seq.empty[Row], Seq(Row(42)))
      verifySqlScriptResult(commands, expected)
    }
  }

  test("simple case else with count") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |  CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |  INSERT INTO t VALUES (2, 'b', 2.0);
          |  CASE (SELECT COUNT(*) FROM t)
          |   WHEN 1 THEN
          |     SELECT 42;
          |   WHEN 3 THEN
          |     SELECT 43;
          |   ELSE
          |     SELECT 44;
          |  END CASE;
          |END
          |""".stripMargin

      val expected = Seq(Seq.empty[Row], Seq.empty[Row], Seq.empty[Row], Seq(Row(44)))
      verifySqlScriptResult(commands, expected)
    }
  }

  test("simple case no cases matched no else") {
    val commands =
      """
        |BEGIN
        | CASE 1
        |   WHEN 2 THEN
        |     SELECT 42;
        |   WHEN 3 THEN
        |     SELECT 43;
        | END CASE;
        |END
        |""".stripMargin
    val expected = Seq()
    verifySqlScriptResult(commands, expected)
  }

  test("simple case mismatched types") {
    val commands =
      """
        |BEGIN
        | CASE 1
        |   WHEN "one" THEN
        |     SELECT 42;
        | END CASE;
        |END
        |""".stripMargin
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkError(
        exception = intercept[SparkNumberFormatException](
          runSqlScript(commands)
        ),
        condition = "CAST_INVALID_INPUT",
        parameters = Map(
          "expression" -> "'one'",
          "sourceType" -> "\"STRING\"",
          "targetType" -> "\"BIGINT\""),
        context = ExpectedContext(fragment = "\"one\"", start = 23, stop = 27))
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkError(
        exception = intercept[SqlScriptingException](
          runSqlScript(commands)
        ),
        condition = "BOOLEAN_STATEMENT_WITH_EMPTY_ROW",
        parameters = Map("invalidStatement" -> "\"ONE\""))
    }
  }

  test("simple case compare with null") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |  CREATE TABLE t (a INT) USING parquet;
          |  CASE (SELECT COUNT(*) FROM t)
          |   WHEN 1 THEN
          |     SELECT 42;
          |   ELSE
          |     SELECT 43;
          |  END CASE;
          |END
          |""".stripMargin

      val expected = Seq(Seq.empty[Row], Seq(Row(43)))
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
      val exception = intercept[SqlScriptingException] {
        runSqlScript(commands)
      }
      checkError(
        exception = exception,
        condition = "INVALID_BOOLEAN_STATEMENT",
        parameters = Map("invalidStatement" -> "1")
      )
      assert(exception.origin.line.isDefined)
      assert(exception.origin.line.get == 3)
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
      val exception = intercept[SqlScriptingException] {
        runSqlScript(commands1)
      }
      checkError(
        exception = exception,
        condition = "BOOLEAN_STATEMENT_WITH_EMPTY_ROW",
        parameters = Map("invalidStatement" -> "(SELECT * FROM T1)")
      )
      assert(exception.origin.line.isDefined)
      assert(exception.origin.line.get == 4)

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

  test("repeat - empty body") {
    val commands =
      """
        |BEGIN
        | REPEAT
        |   BEGIN
        |   END;
        | UNTIL 1 = 1
        | END REPEAT;
        |END
        |""".stripMargin

    val expected = Seq.empty[Seq[Row]]
    verifySqlScriptResult(commands, expected)
  }

  test("repeat - nested empty body") {
    val commands =
      """
        |BEGIN
        | REPEAT
        |   BEGIN
        |     BEGIN
        |     END;
        |   END;
        |   BEGIN
        |   END;
        | UNTIL 1 = 1
        | END REPEAT;
        |END
        |""".stripMargin

    val expected = Seq.empty[Seq[Row]]
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
        |BEGIN
        |  lbl: BEGIN
        |    SELECT 1;
        |    LEAVE lbl;
        |    SELECT 2;
        |  END;
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
        |BEGIN
        |  lbl: BEGIN
        |    SELECT 1;
        |    ITERATE lbl;
        |  END;
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
        |BEGIN
        |  lbl: BEGIN
        |    SELECT 1;
        |    LEAVE randomlbl;
        |  END;
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
        |BEGIN
        |  lbl: BEGIN
        |    SELECT 1;
        |    ITERATE randomlbl;
        |  END;
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

  test("loop statement with leave") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE x INT;
        |  SET x = 0;
        |  lbl: LOOP
        |    SET x = x + 1;
        |    SELECT x;
        |    IF x > 2
        |    THEN
        |     LEAVE lbl;
        |    END IF;
        |  END LOOP;
        |  SELECT x;
        |END""".stripMargin
    val expected = Seq(
      Seq.empty[Row], // declare
      Seq.empty[Row], // set x = 0
      Seq.empty[Row], // set x = 1
      Seq(Row(1)), // select x
      Seq.empty[Row], // set x = 2
      Seq(Row(2)), // select x
      Seq.empty[Row], // set x = 3
      Seq(Row(3)), // select x
      Seq(Row(3)), // select x
      Seq.empty[Row] // drop
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("nested loop statement with leave") {
    val commands =
      """
        |BEGIN
        | DECLARE x = 0;
        | DECLARE y = 0;
        | lbl1: LOOP
        |   SET VAR y = 0;
        |   lbl2: LOOP
        |     SELECT x, y;
        |     SET VAR y = y + 1;
        |     IF y >= 2 THEN
        |       LEAVE lbl2;
        |     END IF;
        |   END LOOP;
        |   SET VAR x = x + 1;
        |   IF x >= 2 THEN
        |     LEAVE lbl1;
        |   END IF;
        | END LOOP;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq.empty[Row], // declare x
      Seq.empty[Row], // declare y
      Seq.empty[Row], // set y to 0
      Seq(Row(0, 0)), // select x, y
      Seq.empty[Row], // increase y
      Seq(Row(0, 1)), // select x, y
      Seq.empty[Row], // increase y
      Seq.empty[Row], // increase x
      Seq.empty[Row], // set y to 0
      Seq(Row(1, 0)), // select x, y
      Seq.empty[Row], // increase y
      Seq(Row(1, 1)), // select x, y
      Seq.empty[Row], // increase y
      Seq.empty[Row], // increase x
      Seq.empty[Row], // drop y
      Seq.empty[Row] // drop x
    )
    verifySqlScriptResult(commands, expected)
  }

  test("iterate loop statement") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE x INT;
        |  SET x = 0;
        |  lbl: LOOP
        |    SET x = x + 1;
        |    IF x > 1 THEN
        |     LEAVE lbl;
        |    END IF;
        |    ITERATE lbl;
        |    SET x = x + 2;
        |  END LOOP;
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

  test("leave outer loop from nested loop statement") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: LOOP
        |    lbl2: LOOP
        |      SELECT 1;
        |      LEAVE lbl;
        |    END LOOP;
        |  END LOOP;
        |END""".stripMargin
    val expected = Seq(
      Seq(Row(1)) // select 1
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("iterate outer loop from nested loop statement") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE x INT;
        |  SET x = 0;
        |  lbl: LOOP
        |    SET x = x + 1;
        |    IF x > 2 THEN
        |     LEAVE lbl;
        |    END IF;
        |    lbl2: LOOP
        |      SELECT 1;
        |      ITERATE lbl;
        |      SET x = 10;
        |    END LOOP;
        |  END LOOP;
        |  SELECT x;
        |END""".stripMargin
    val expected = Seq(
      Seq.empty[Row], // declare
      Seq.empty[Row], // set x = 0
      Seq.empty[Row], // set x = 1
      Seq(Row(1)), // select 1
      Seq.empty[Row], // set x = 2
      Seq(Row(1)), // select 1
      Seq.empty[Row], // set x = 3
      Seq(Row(3)), // select x
      Seq.empty[Row] // drop
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }

  test("for statement - enters body once") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT, stringCol STRING, doubleCol DOUBLE) using parquet;
          | INSERT INTO t VALUES (1, 'first', 1.0);
          | FOR row AS SELECT * FROM t DO
          |   SELECT row.intCol;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row(1)), // select row.intCol
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - enters body with multiple statements multiple times") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT, stringCol STRING, doubleCol DOUBLE) using parquet;
          | INSERT INTO t VALUES (1, 'first', 1.0);
          | INSERT INTO t VALUES (2, 'second', 2.0);
          | FOR row AS SELECT * FROM t ORDER BY intCol DO
          |   SELECT row.intCol;
          |   SELECT intCol;
          |   SELECT row.stringCol;
          |   SELECT stringCol;
          |   SELECT row.doubleCol;
          |   SELECT doubleCol;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // insert
        Seq(Row(1)), // select row.intCol
        Seq(Row(1)), // select intCol
        Seq(Row("first")), // select row.stringCol
        Seq(Row("first")), // select stringCol
        Seq(Row(1.0)), // select row.doubleCol
        Seq(Row(1.0)), // select doubleCol
        Seq(Row(2)), // select row.intCol
        Seq(Row(2)), // select intCol
        Seq(Row("second")), // select row.stringCol
        Seq(Row("second")), // select stringCol
        Seq(Row(2.0)), // select row.doubleCol
        Seq(Row(2.0)), // select doubleCol
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - sum of column from table") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | DECLARE sumOfCols = 0;
          | CREATE TABLE t (intCol INT) using parquet;
          | INSERT INTO t VALUES (1), (2), (3), (4);
          | FOR row AS SELECT * FROM t DO
          |   SET sumOfCols = sumOfCols + row.intCol;
          | END FOR;
          | SELECT sumOfCols;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // declare sumOfCols
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // set sumOfCols
        Seq.empty[Row], // set sumOfCols
        Seq.empty[Row], // set sumOfCols
        Seq.empty[Row], // set sumOfCols
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq(Row(10)), // select sumOfCols
        Seq.empty[Row] // drop sumOfCols
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - map, struct, array") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (int_column INT, map_column MAP<STRING, INT>,
          | struct_column STRUCT<name: STRING, age: INT>, array_column ARRAY<STRING>);
          | INSERT INTO t VALUES
          |  (1, MAP('a', 1), STRUCT('John', 25), ARRAY('apricot', 'quince')),
          |  (2, MAP('b', 2), STRUCT('Jane', 30), ARRAY('plum', 'pear'));
          | FOR row AS SELECT * FROM t ORDER BY int_column DO
          |   SELECT row.map_column;
          |   SELECT map_column;
          |   SELECT row.struct_column;
          |   SELECT struct_column;
          |   SELECT row.array_column;
          |   SELECT array_column;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row(Map("a" -> 1))), // select row.map_column
        Seq(Row(Map("a" -> 1))), // select map_column
        Seq(Row(Row("John", 25))), // select row.struct_column
        Seq(Row(Row("John", 25))), // select struct_column
        Seq(Row(Array("apricot", "quince"))), // select row.array_column
        Seq(Row(Array("apricot", "quince"))), // select array_column
        Seq(Row(Map("b" -> 2))), // select row.map_column
        Seq(Row(Map("b" -> 2))), // select map_column
        Seq(Row(Row("Jane", 30))), // select row.struct_column
        Seq(Row(Row("Jane", 30))), // select struct_column
        Seq(Row(Array("plum", "pear"))), // select row.array_column
        Seq(Row(Array("plum", "pear"))), // select array_column
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - nested struct") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t
          | (int_column INT,
          | struct_column STRUCT<num: INT, struct2: STRUCT<struct3: STRUCT<name: STRING>>>);
          | INSERT INTO t VALUES
          |  (1, STRUCT(1, STRUCT(STRUCT("one")))),
          |  (2, STRUCT(2, STRUCT(STRUCT("two"))));
          | FOR row AS SELECT * FROM t ORDER BY int_column DO
          |   SELECT row.struct_column;
          |   SELECT struct_column;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row(Row(1, Row(Row("one"))))), // select row.struct_column
        Seq(Row(Row(1, Row(Row("one"))))), // select struct_column
        Seq(Row(Row(2, Row(Row("two"))))), // select row.struct_column
        Seq(Row(Row(2, Row(Row("two"))))), // select struct_column
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - nested map") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (int_column INT, map_column MAP<STRING, MAP<INT, MAP<BOOLEAN, INT>>>);
          | INSERT INTO t VALUES
          |  (1, MAP('a', MAP(1, MAP(false, 10)))),
          |  (2, MAP('b', MAP(2, MAP(true, 20))));
          | FOR row AS SELECT * FROM t ORDER BY int_column DO
          |   SELECT row.map_column;
          |   SELECT map_column;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row(Map("a" -> Map(1 -> Map(false -> 10))))), // select row.map_column
        Seq(Row(Map("a" -> Map(1 -> Map(false -> 10))))), // select map_column
        Seq(Row(Map("b" -> Map(2 -> Map(true -> 20))))), // select row.map_column
        Seq(Row(Map("b" -> Map(2 -> Map(true -> 20))))), // select map_column
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - nested array") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t
          | (int_column INT, array_column ARRAY<ARRAY<ARRAY<INT>>>);
          | INSERT INTO t VALUES
          |  (1, ARRAY(ARRAY(ARRAY(1, 2), ARRAY(3, 4)), ARRAY(ARRAY(5, 6)))),
          |  (2, ARRAY(ARRAY(ARRAY(7, 8), ARRAY(9, 10)), ARRAY(ARRAY(11, 12))));
          | FOR row AS SELECT * FROM t ORDER BY int_column DO
          |   SELECT row.array_column;
          |   SELECT array_column;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row(Seq(Seq(Seq(1, 2), Seq(3, 4)), Seq(Seq(5, 6))))), // row.array_column
        Seq(Row(Seq(Seq(Seq(1, 2), Seq(3, 4)), Seq(Seq(5, 6))))), // array_column
        Seq(Row(Array(Seq(Seq(7, 8), Seq(9, 10)), Seq(Seq(11, 12))))), // row.array_column
        Seq(Row(Array(Seq(Seq(7, 8), Seq(9, 10)), Seq(Seq(11, 12))))), // array_column
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - empty result") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | FOR row AS SELECT * FROM t ORDER BY intCol DO
          |   SELECT row.intCol;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row] // create table
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - empty body") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT, stringCol STRING, doubleCol DOUBLE) using parquet;
          | INSERT INTO t VALUES (1, 'first', 1.0);
          | FOR row AS SELECT * FROM t DO
          |   BEGIN
          |   END;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - nested empty body") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT, stringCol STRING, doubleCol DOUBLE) using parquet;
          | INSERT INTO t VALUES (1, 'first', 1.0);
          | FOR row AS SELECT * FROM t DO
          |   BEGIN
          |     BEGIN
          |     END;
          |   END;
          |   BEGIN
          |     BEGIN
          |     END;
          |   END;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement iterate") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT, stringCol STRING) using parquet;
          | INSERT INTO t VALUES (1, 'first'), (2, 'second'), (3, 'third'), (4, 'fourth');
          |
          | lbl: FOR x AS SELECT * FROM t ORDER BY intCol DO
          |   IF x.intCol = 2 THEN
          |     ITERATE lbl;
          |   END IF;
          |   SELECT stringCol;
          |   SELECT x.stringCol;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row("first")), // select stringCol
        Seq(Row("first")), // select x.stringCol
        Seq(Row("third")), // select stringCol
        Seq(Row("third")), // select x.stringCol
        Seq(Row("fourth")), // select stringCol
        Seq(Row("fourth")), // select x.stringCol
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement leave") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT, stringCol STRING) using parquet;
          | INSERT INTO t VALUES (1, 'first'), (2, 'second'), (3, 'third'), (4, 'fourth');
          |
          | lbl: FOR x AS SELECT * FROM t ORDER BY intCol DO
          |   IF x.intCol = 3 THEN
          |     LEAVE lbl;
          |   END IF;
          |   SELECT stringCol;
          |   SELECT x.stringCol;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row("first")), // select stringCol
        Seq(Row("first")), // select x.stringCol
        Seq(Row("second")), // select stringCol
        Seq(Row("second")) // select x.stringCol
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - nested - in while") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | DECLARE cnt = 0;
          | CREATE TABLE t (intCol INT) using parquet;
          | INSERT INTO t VALUES (0);
          | WHILE cnt < 2 DO
          |   SET cnt = cnt + 1;
          |   FOR x AS SELECT * FROM t ORDER BY intCol DO
          |     SELECT x.intCol;
          |   END FOR;
          |   INSERT INTO t VALUES (cnt);
          | END WHILE;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // declare cnt
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // set cnt
        Seq(Row(0)), // select intCol
        Seq.empty[Row], // insert
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // set cnt
        Seq(Row(0)), // select intCol
        Seq(Row(1)), // select intCol
        Seq.empty[Row], // insert
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop cnt
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - nested - in other for") {
    withTable("t", "t2") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | CREATE TABLE t2 (intCol2 INT) using parquet;
          | INSERT INTO t VALUES (0), (1);
          | INSERT INTO t2 VALUES (2), (3);
          | FOR x as SELECT * FROM t ORDER BY intCol DO
          |   FOR y AS SELECT * FROM t2 ORDER BY intCol2 DESC DO
          |     SELECT x.intCol;
          |     SELECT intCol;
          |     SELECT y.intCol2;
          |     SELECT intCol2;
          |   END FOR;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // insert
        Seq(Row(0)), // select x.intCol
        Seq(Row(0)), // select intCol
        Seq(Row(3)), // select y.intCol2
        Seq(Row(3)), // select intCol2
        Seq(Row(0)), // select x.intCol
        Seq(Row(0)), // select intCol
        Seq(Row(2)), // select y.intCol2
        Seq(Row(2)), // select intCol2
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq(Row(1)), // select x.intCol
        Seq(Row(1)), // select intCol
        Seq(Row(3)), // select y.intCol2
        Seq(Row(3)), // select intCol2
        Seq(Row(1)), // select x.intCol
        Seq(Row(1)), // select intCol
        Seq(Row(2)), // select y.intCol2
        Seq(Row(2)), // select intCol2
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop outer var
        Seq.empty[Row] // drop outer var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  // ignored until loops are fixed to support empty bodies
  ignore("for statement - nested - empty result set") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | REPEAT
          |   FOR x AS SELECT * FROM t ORDER BY intCol DO
          |     SELECT x.intCol;
          |   END FOR;
          | UNTIL 1 = 1
          | END REPEAT;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // declare cnt
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // set cnt
        Seq(Row(0)), // select intCol
        Seq.empty[Row], // insert
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // set cnt
        Seq(Row(0)), // select intCol
        Seq(Row(1)), // select intCol
        Seq.empty[Row], // insert
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop cnt
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - nested - iterate outer loop") {
    withTable("t", "t2") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | CREATE TABLE t2 (intCol2 INT) using parquet;
          | INSERT INTO t VALUES (0), (1);
          | INSERT INTO t2 VALUES (2), (3);
          | lbl1: FOR x as SELECT * FROM t ORDER BY intCol DO
          |   lbl2: FOR y AS SELECT * FROM t2 ORDER BY intCol2 DESC DO
          |     SELECT y.intCol2;
          |     SELECT intCol2;
          |     ITERATE lbl1;
          |     SELECT 1;
          |   END FOR;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // insert
        Seq(Row(3)), // select y.intCol2
        Seq(Row(3)), // select intCol2
        Seq(Row(3)), // select y.intCol2
        Seq(Row(3)), // select intCol2
        Seq.empty[Row], // drop outer var
        Seq.empty[Row] // drop outer var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - nested - leave outer loop") {
    withTable("t", "t2") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | CREATE TABLE t2 (intCol2 INT) using parquet;
          | INSERT INTO t VALUES (0), (1);
          | INSERT INTO t2 VALUES (2), (3);
          | lbl1: FOR x as SELECT * FROM t ORDER BY intCol DO
          |   lbl2: FOR y AS SELECT * FROM t2 ORDER BY intCol2 DESC DO
          |     SELECT y.intCol2;
          |     SELECT intCol2;
          |     LEAVE lbl1;
          |     SELECT 1;
          |   END FOR;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // insert
        Seq(Row(3)), // select y.intCol2
        Seq(Row(3)) // select intCol2
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - nested - leave inner loop") {
    withTable("t", "t2") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | CREATE TABLE t2 (intCol2 INT) using parquet;
          | INSERT INTO t VALUES (0), (1);
          | INSERT INTO t2 VALUES (2), (3);
          | lbl1: FOR x as SELECT * FROM t ORDER BY intCol DO
          |   lbl2: FOR y AS SELECT * FROM t2 ORDER BY intCol2 DESC DO
          |     SELECT y.intCol2;
          |     SELECT intCol2;
          |     LEAVE lbl2;
          |     SELECT 1;
          |   END FOR;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // insert
        Seq(Row(3)), // select y.intCol2
        Seq(Row(3)), // select intCol2
        Seq(Row(3)), // select y.intCol2
        Seq(Row(3)), // select intCol2
        Seq.empty[Row], // drop outer var
        Seq.empty[Row] // drop outer var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - enters body once") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT, stringCol STRING, doubleCol DOUBLE) using parquet;
          | INSERT INTO t VALUES (1, 'first', 1.0);
          | FOR SELECT * FROM t DO
          |   SELECT intCol;
          |   SELECT stringCol;
          |   SELECT doubleCol;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row(1)), // select intCol
        Seq(Row("first")), // select stringCol
        Seq(Row(1.0)), // select doubleCol
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - enters body with multiple statements multiple times") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT, stringCol STRING, doubleCol DOUBLE) using parquet;
          | INSERT INTO t VALUES (1, 'first', 1.0);
          | INSERT INTO t VALUES (2, 'second', 2.0);
          | FOR SELECT * FROM t ORDER BY intCol DO
          |   SELECT intCol;
          |   SELECT stringCol;
          |   SELECT doubleCol;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // insert
        Seq(Row(1)), // select intCol
        Seq(Row("first")), // select stringCol
        Seq(Row(1.0)), // select doubleCol
        Seq(Row(2)), // select intCol
        Seq(Row("second")), // select stringCol
        Seq(Row(2.0)), // select doubleCol
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - sum of column from table") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | DECLARE sumOfCols = 0;
          | CREATE TABLE t (intCol INT) using parquet;
          | INSERT INTO t VALUES (1), (2), (3), (4);
          | FOR SELECT * FROM t DO
          |   SET sumOfCols = sumOfCols + intCol;
          | END FOR;
          | SELECT sumOfCols;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // declare sumOfCols
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // set sumOfCols
        Seq.empty[Row], // set sumOfCols
        Seq.empty[Row], // set sumOfCols
        Seq.empty[Row], // set sumOfCols
        Seq.empty[Row], // drop local var
        Seq(Row(10)), // select sumOfCols
        Seq.empty[Row] // drop sumOfCols
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - map, struct, array") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (int_column INT, map_column MAP<STRING, INT>,
          | struct_column STRUCT<name: STRING, age: INT>, array_column ARRAY<STRING>);
          | INSERT INTO t VALUES
          |  (1, MAP('a', 1), STRUCT('John', 25), ARRAY('apricot', 'quince')),
          |  (2, MAP('b', 2), STRUCT('Jane', 30), ARRAY('plum', 'pear'));
          | FOR SELECT * FROM t ORDER BY int_column DO
          |   SELECT map_column;
          |   SELECT struct_column;
          |   SELECT array_column;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row(Map("a" -> 1))), // select map_column
        Seq(Row(Row("John", 25))), // select struct_column
        Seq(Row(Array("apricot", "quince"))), // select array_column
        Seq(Row(Map("b" -> 2))), // select map_column
        Seq(Row(Row("Jane", 30))), // select struct_column
        Seq(Row(Array("plum", "pear"))), // select array_column
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - nested struct") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (int_column INT,
          | struct_column STRUCT<num: INT, struct2: STRUCT<struct3: STRUCT<name: STRING>>>);
          | INSERT INTO t VALUES
          |  (1, STRUCT(1, STRUCT(STRUCT("one")))),
          |  (2, STRUCT(2, STRUCT(STRUCT("two"))));
          | FOR SELECT * FROM t ORDER BY int_column DO
          |   SELECT struct_column;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row(Row(1, Row(Row("one"))))), // select struct_column
        Seq(Row(Row(2, Row(Row("two"))))), // select struct_column
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - nested map") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (int_column INT, map_column MAP<STRING, MAP<INT, MAP<BOOLEAN, INT>>>);
          | INSERT INTO t VALUES
          |  (1, MAP('a', MAP(1, MAP(false, 10)))),
          |  (2, MAP('b', MAP(2, MAP(true, 20))));
          | FOR SELECT * FROM t ORDER BY int_column DO
          |   SELECT map_column;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row(Map("a" -> Map(1 -> Map(false -> 10))))), // select map_column
        Seq(Row(Map("b" -> Map(2 -> Map(true -> 20))))), // select map_column
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - nested array") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t
          | (int_column INT, array_column ARRAY<ARRAY<ARRAY<INT>>>);
          | INSERT INTO t VALUES
          |  (1, ARRAY(ARRAY(ARRAY(1, 2), ARRAY(3, 4)), ARRAY(ARRAY(5, 6)))),
          |  (2, ARRAY(ARRAY(ARRAY(7, 8), ARRAY(9, 10)), ARRAY(ARRAY(11, 12))));
          | FOR SELECT * FROM t ORDER BY int_column DO
          |   SELECT array_column;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row(Seq(Seq(Seq(1, 2), Seq(3, 4)), Seq(Seq(5, 6))))), // array_column
        Seq(Row(Array(Seq(Seq(7, 8), Seq(9, 10)), Seq(Seq(11, 12))))), // array_column
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - empty result") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | FOR SELECT * FROM t ORDER BY intCol DO
          |   SELECT intCol;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row] // create table
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - iterate") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT, stringCol STRING) using parquet;
          | INSERT INTO t VALUES (1, 'first'), (2, 'second'), (3, 'third'), (4, 'fourth');
          |
          | lbl: FOR SELECT * FROM t ORDER BY intCol DO
          |   IF intCol = 2 THEN
          |     ITERATE lbl;
          |   END IF;
          |   SELECT stringCol;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row("first")), // select stringCol
        Seq(Row("third")), // select stringCol
        Seq(Row("fourth")), // select stringCol
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop local var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - leave") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT, stringCol STRING) using parquet;
          | INSERT INTO t VALUES (1, 'first'), (2, 'second'), (3, 'third'), (4, 'fourth');
          |
          | lbl: FOR SELECT * FROM t ORDER BY intCol DO
          |   IF intCol = 3 THEN
          |     LEAVE lbl;
          |   END IF;
          |   SELECT stringCol;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq(Row("first")), // select stringCol
        Seq(Row("second")) // select stringCol
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - nested - in while") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | DECLARE cnt = 0;
          | CREATE TABLE t (intCol INT) using parquet;
          | INSERT INTO t VALUES (0);
          | WHILE cnt < 2 DO
          |   SET cnt = cnt + 1;
          |   FOR SELECT * FROM t ORDER BY intCol DO
          |     SELECT intCol;
          |   END FOR;
          |   INSERT INTO t VALUES (cnt);
          | END WHILE;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // declare cnt
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // set cnt
        Seq(Row(0)), // select intCol
        Seq.empty[Row], // insert
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // set cnt
        Seq(Row(0)), // select intCol
        Seq(Row(1)), // select intCol
        Seq.empty[Row], // insert
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop cnt
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - nested - in other for") {
    withTable("t", "t2") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | CREATE TABLE t2 (intCol2 INT) using parquet;
          | INSERT INTO t VALUES (0), (1);
          | INSERT INTO t2 VALUES (2), (3);
          | FOR SELECT * FROM t ORDER BY intCol DO
          |   FOR SELECT * FROM t2 ORDER BY intCol2 DESC DO
          |     SELECT intCol;
          |     SELECT intCol2;
          |   END FOR;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // insert
        Seq(Row(0)), // select intCol
        Seq(Row(3)), // select intCol2
        Seq(Row(0)), // select intCol
        Seq(Row(2)), // select intCol2
        Seq.empty[Row], // drop local var
        Seq(Row(1)), // select intCol
        Seq(Row(3)), // select intCol2
        Seq(Row(1)), // select intCol
        Seq(Row(2)), // select intCol2
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop outer var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  // ignored until loops are fixed to support empty bodies
  ignore("for statement - no variable - nested - empty result set") {
    withTable("t") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | REPEAT
          |   FOR SELECT * FROM t ORDER BY intCol DO
          |     SELECT intCol;
          |   END FOR;
          | UNTIL 1 = 1
          | END REPEAT;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // declare cnt
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // set cnt
        Seq(Row(0)), // select intCol
        Seq.empty[Row], // insert
        Seq.empty[Row], // drop local var
        Seq.empty[Row], // set cnt
        Seq(Row(0)), // select intCol
        Seq(Row(1)), // select intCol
        Seq.empty[Row], // insert
        Seq.empty[Row], // drop local var
        Seq.empty[Row] // drop cnt
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - nested - iterate outer loop") {
    withTable("t", "t2") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | CREATE TABLE t2 (intCol2 INT) using parquet;
          | INSERT INTO t VALUES (0), (1);
          | INSERT INTO t2 VALUES (2), (3);
          | lbl1: FOR SELECT * FROM t ORDER BY intCol DO
          |   lbl2: FOR SELECT * FROM t2 ORDER BY intCol2 DESC DO
          |     SELECT intCol2;
          |     ITERATE lbl1;
          |     SELECT 1;
          |   END FOR;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // insert
        Seq(Row(3)), // select intCol2
        Seq(Row(3)), // select intCol2
        Seq.empty[Row] // drop outer var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - nested - leave outer loop") {
    withTable("t", "t2") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | CREATE TABLE t2 (intCol2 INT) using parquet;
          | INSERT INTO t VALUES (0), (1);
          | INSERT INTO t2 VALUES (2), (3);
          | lbl1: FOR SELECT * FROM t ORDER BY intCol DO
          |   lbl2: FOR SELECT * FROM t2 ORDER BY intCol2 DESC DO
          |     SELECT intCol2;
          |     LEAVE lbl1;
          |     SELECT 1;
          |   END FOR;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // insert
        Seq(Row(3)) // select intCol2
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("for statement - no variable - nested - leave inner loop") {
    withTable("t", "t2") {
      val sqlScript =
        """
          |BEGIN
          | CREATE TABLE t (intCol INT) using parquet;
          | CREATE TABLE t2 (intCol2 INT) using parquet;
          | INSERT INTO t VALUES (0), (1);
          | INSERT INTO t2 VALUES (2), (3);
          | lbl1: FOR SELECT * FROM t ORDER BY intCol DO
          |   lbl2: FOR SELECT * FROM t2 ORDER BY intCol2 DESC DO
          |     SELECT intCol2;
          |     LEAVE lbl2;
          |     SELECT 1;
          |   END FOR;
          | END FOR;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // create table
        Seq.empty[Row], // create table
        Seq.empty[Row], // insert
        Seq.empty[Row], // insert
        Seq(Row(3)), // select intCol2
        Seq(Row(3)), // select intCol2
        Seq.empty[Row] // drop outer var
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }
}
