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

import scala.collection.mutable.ListBuffer

import org.apache.spark.{SparkArithmeticException, SparkConf}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.CompoundBody
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SQL Scripting interpreter tests.
 * Output from the parser is provided to the interpreter.
 * Output from the interpreter (iterator over executable statements) is then checked - statements
 *   are executed and output DataFrames are compared with expected outputs.
 */
class SqlScriptingExecutionSuite extends QueryTest with SharedSparkSession {

  // Tests setup
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.SQL_SCRIPTING_ENABLED.key, "true")
  }

  // Helpers
  private def runSqlScript(
      sqlText: String,
      args: Map[String, Expression] = Map.empty): Seq[Array[Row]] = {
    val compoundBody = spark.sessionState.sqlParser.parsePlan(sqlText).asInstanceOf[CompoundBody]
    val sse = new SqlScriptingExecution(compoundBody, spark, args)
    val result: ListBuffer[Array[Row]] = ListBuffer.empty

    var df = sse.getNextResult
    while (df.isDefined) {
      // Collect results from the current DataFrame.
      sse.withErrorHandling {
        result.append(df.get.collect())
      }
      df = sse.getNextResult
    }
    result.toSeq
  }

  private def verifySqlScriptResult(sqlText: String, expected: Seq[Seq[Row]]): Unit = {
    val result = runSqlScript(sqlText)
    assert(result.length == expected.length)
    result.zip(expected).foreach {
      case (actualAnswer, expectedAnswer) =>
        assert(actualAnswer.sameElements(expectedAnswer))
    }
  }

  // Handler tests
  test("duplicate handler for the same condition") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE duplicate_condition CONDITION FOR SQLSTATE '12345';
        |  DECLARE OR REPLACE flag INT = -1;
        |  DECLARE EXIT HANDLER FOR duplicate_condition
        |  BEGIN
        |    SET VAR flag = 1;
        |  END;
        |  DECLARE EXIT HANDLER FOR duplicate_condition
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
      condition = "DUPLICATE_EXCEPTION_HANDLER.CONDITION",
      parameters = Map("condition" -> "DUPLICATE_CONDITION"))
  }

  test("duplicate handler for the same sqlState") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  DECLARE EXIT HANDLER FOR SQLSTATE '12345'
        |  BEGIN
        |    SET VAR flag = 1;
        |  END;
        |  DECLARE EXIT HANDLER FOR SQLSTATE '12345'
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
      condition = "DUPLICATE_EXCEPTION_HANDLER.SQLSTATE",
      parameters = Map("sqlState" -> "12345"))
  }

  test("Specific condition takes precedence over sqlState") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  BEGIN
        |    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 1;
        |    END;
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 2;
        |    END;
        |    SELECT 1/0;
        |  END;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(-1)),   // select flag
      Seq(Row(1))     // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }

  test("Innermost handler takes precedence over other handlers") {
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
        |    SELECT 1/0;
        |  END;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(-1)),   // select flag
      Seq(Row(2))     // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }

  test("handler - exit resolve in the same block") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE VARIABLE flag INT = -1;
        |  scope_to_exit: BEGIN
        |    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 1;
        |    END;
        |    SELECT 2;
        |    SELECT 3;
        |    SELECT 1/0;
        |    SELECT 4;
        |    SELECT 5;
        |  END;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(2)),    // select
      Seq(Row(3)),    // select
      Seq(Row(-1)),   // select flag
      Seq(Row(1))     // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }

  test("handler - exit resolve in the same block when if condition fails") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE VARIABLE flag INT = -1;
        |  scope_to_exit: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 1;
        |    END;
        |    SELECT 2;
        |    SELECT 3;
        |    IF 1 > 1/0 THEN
        |      SELECT 10;
        |    END IF;
        |    SELECT 4;
        |    SELECT 5;
        |  END;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(2)),    // select
      Seq(Row(3)),    // select
      Seq(Row(-1)),   // select flag
      Seq(Row(1))     // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }

  test("handler - exit resolve in outer block") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE VARIABLE flag INT = -1;
        |  l1: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 1;
        |    END;
        |    SELECT 2;
        |    SELECT 3;
        |    l2: BEGIN
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
      Seq(Row(2)),    // select
      Seq(Row(3)),    // select
      Seq(Row(4)),    // select
      Seq(Row(-1)),   // select flag
      Seq(Row(1))     // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }

  test("handler - chained handlers for different exceptions") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE VARIABLE flag INT = -1;
        |  l1: BEGIN
        |    DECLARE EXIT HANDLER FOR UNRESOLVED_COLUMN.WITHOUT_SUGGESTION
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 2;
        |    END;
        |    l2: BEGIN
        |      DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
        |      BEGIN
        |        SELECT flag;
        |        SET VAR flag = 1;
        |        select X; -- select non existing variable
        |        SELECT 2;
        |      END;
        |      SELECT 5;
        |      SELECT 1/0; -- divide by zero
        |      SELECT 6;
        |    END;
        |  END;
        |
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(5)),    // select
      Seq(Row(-1)),   // select flag from handler in l2
      Seq(Row(1)),    // select flag from handler in l1
      Seq(Row(2))     // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }

  test("handler - double chained handlers") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE VARIABLE flag INT = -1;
        |  l1: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 2;
        |    END;
        |    l2: BEGIN
        |      DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |      BEGIN
        |        SELECT flag;
        |        SET VAR flag = 1;
        |        SELECT 1/0;
        |        SELECT 2;
        |      END;
        |      SELECT 5;
        |      SELECT 1/0;
        |      SELECT 6;
        |    END;
        |  END;
        |
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(5)),    // select
      Seq(Row(-1)),   // select flag from handler in l2
      Seq(Row(1)),    // select flag from handler in l1
      Seq(Row(2))     // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }

  test("handler - triple chained handlers") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE VARIABLE flag INT = -1;
        |  l1: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 3;
        |    END;
        |    l2: BEGIN
        |      DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |      BEGIN
        |        SELECT flag;
        |        SET VAR flag = 2;
        |        SELECT 1/0;
        |        SELECT 2;
        |      END;
        |
        |      l3: BEGIN
        |        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |        BEGIN
        |          SELECT flag;
        |          SET VAR flag = 1;
        |          SELECT 1/0;
        |          SELECT 2;
        |        END;
        |
        |        SELECT 5;
        |        SELECT 1/0;
        |        SELECT 6;
        |      END;
        |    END;
        |  END;
        |
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(5)),    // select
      Seq(Row(-1)),   // select flag from handler in l3
      Seq(Row(1)),    // select flag from handler in l2
      Seq(Row(2)),    // select flag from handler in l1
      Seq(Row(3))     // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }

  test("handler in handler") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE VARIABLE flag INT = -1;
        |  lbl_0: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |      lbl_1: BEGIN
        |        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |        lbl_2: BEGIN
        |          SELECT flag;
        |          SET VAR flag = 2;
        |        END;
        |
        |        SELECT flag;
        |        SET VAR flag = 1;
        |        SELECT 1/0;
        |        SELECT 2;
        |      END;
        |
        |      SELECT 5;
        |      SELECT 1/0;
        |      SELECT 6;
        |  END;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(5)),    // select
      Seq(Row(-1)),   // select flag from outer handler
      Seq(Row(1)),    // select flag from inner handler
      Seq(Row(2))     // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }

  test("triple nested handler") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE VARIABLE flag INT = -1;
        |  lbl_0: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |    lbl_1: BEGIN
        |      DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |      lbl_2: BEGIN
        |        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |        lbl_3: BEGIN
        |          SELECT flag; -- third select flag (2)
        |          SET VAR flag = 3;
        |        END;
        |
        |        SELECT flag; -- second select flag (1)
        |        SET VAR flag = 2;
        |        SELECT 1/0; -- third error will be thrown here
        |        SELECT 2;
        |      END;
        |
        |      SELECT flag; -- first select flag (-1)
        |      SET VAR flag = 1;
        |      SELECT 1/0; -- second error will be thrown here
        |      SELECT 2;
        |    END;
        |
        |    SELECT 5;
        |    SELECT 1/0;  -- first error will be thrown here
        |    SELECT 6;
        |  END;
        |  SELECT flag; -- fourth select flag (3)
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(5)),    // select
      Seq(Row(-1)),   // select flag in handler
      Seq(Row(1)),    // select flag in handler
      Seq(Row(2)),    // select flag in handler
      Seq(Row(3))     // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }

  test("handler - exit catch-all in the same block") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  l1: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag;
        |      SET VAR flag = 1;
        |    END;
        |    SELECT 2;
        |    SELECT 3;
        |    l2: BEGIN
        |      DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |      BEGIN
        |        SELECT flag;
        |        SET VAR flag = 2;
        |      END;
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
      Seq(Row(2)),    // select
      Seq(Row(3)),    // select
      Seq(Row(4)),    // select
      Seq(Row(-1)),   // select flag
      Seq(Row(6)),    // select
      Seq(Row(2))     // select flag from the outer body
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }

  test("handler with condition and sqlState with equal string value") {
    // This test is intended to verify that the condition and sqlState are not
    // treated equally if they have the same string value. Conditions are prioritized
    // over sqlStates when choosing most appropriate Error Handler.
    val sqlScript1 =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  DECLARE `22012` CONDITION FOR SQLSTATE '12345';
        |  DECLARE EXIT HANDLER FOR `22012`
        |  BEGIN
        |    SET VAR flag = 1;
        |  END;
        |  SELECT 1/0;
        |  SELECT flag;
        |END
        |""".stripMargin
    checkError(
      exception = intercept[SparkArithmeticException] {
        verifySqlScriptResult(sqlScript1, Seq.empty)
      },
      condition = "DIVIDE_BY_ZERO",
      parameters = Map("config" -> "\"spark.sql.ansi.enabled\""),
      queryContext = Array(ExpectedContext("", "", 174, 176, "1/0")))

    val sqlScript2 =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  BEGIN
        |    DECLARE `22012` CONDITION FOR SQLSTATE '12345';
        |    DECLARE EXIT HANDLER FOR `22012`
        |    BEGIN
        |      SET VAR flag = 1;
        |    END;
        |
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SET VAR flag = 2;
        |    END;
        |
        |    SELECT 1/0;
        |  END;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected2 = Seq(Seq(Row(2))) // select flag from the outer body
    verifySqlScriptResult(sqlScript2, expected = expected2)

    val sqlScript3 =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  BEGIN
        |    DECLARE `22012` CONDITION FOR SQLSTATE '12345';
        |    DECLARE EXIT HANDLER FOR `22012`, SQLSTATE '22012'
        |    BEGIN
        |      SET VAR flag = 1;
        |    END;
        |
        |    SELECT 1/0;
        |  END;
        |  SELECT flag;
        |END
        |""".stripMargin
    val expected3 = Seq(Seq(Row(1))) // select flag from the outer body
    verifySqlScriptResult(sqlScript3, expected = expected3)
  }

  test("handler - no appropriate handler is defined") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
        |  BEGIN
        |    SELECT 1; -- this will not execute
        |  END;
        |  SELECT flag;
        |END
        |""".stripMargin
    checkError(
      exception = intercept[AnalysisException] {
        verifySqlScriptResult(sqlScript, Seq.empty)
      },
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      parameters = Map("objectName" -> toSQLId("flag")),
      queryContext = Array(ExpectedContext("", "", 112, 115, "flag")))
  }

  test("invalid sqlState in handler declaration") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE EXIT HANDLER FOR SQLSTATE 'X22012'
        |  BEGIN
        |    SELECT 1;
        |  END;
        |END
        |""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        verifySqlScriptResult(sqlScript, Seq.empty)
      },
      condition = "INVALID_SQLSTATE",
      parameters = Map("sqlState" -> "X22012"))
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
        Seq.empty[Row], // select
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
      val expected = Seq(Seq(Row(false)))
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
    val expected = Seq(Seq(Row(2)))
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
    val expected = Seq(Seq(Row(2)))
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
      Seq(Row(1)), // select
      Seq(Row(2)), // select
      Seq(Row(4)) // select
    )
    verifySqlScriptResult(sqlScript, expected)
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
    val expected = Seq(Seq(Row(2)))
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

  test("if elseif going in elseif") {
    val commands =
      """
        |BEGIN
        |  IF 1=2
        |  THEN
        |    SELECT 42;
        |  ELSEIF 1=1
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

  test("if elseif going in else") {
    val commands =
      """
        |BEGIN
        |  IF 1=2
        |  THEN
        |    SELECT 42;
        |  ELSEIF 1=3
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
      val expected = Seq(Seq(Row(43)))
      verifySqlScriptResult(commands, expected)
    }
  }

  test("if elseif with count") {
    withTable("t") {
      val commands =
        """
          |BEGIN
          |  CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |  IF (SELECT COUNT(*) > 2 FROM t) THEN
          |    SELECT 42;
          |  ELSEIF (SELECT COUNT(*) > 1 FROM t) THEN
          |    SELECT 43;
          |  ELSE
          |    SELECT 44;
          |  END IF;
          |END
          |""".stripMargin
      val expected = Seq(Seq(Row(43)))
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
      val expected = Seq(Seq(Row(43)))
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
      val expected = Seq(Seq(Row(43)))
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
    val expected = Seq.empty
    verifySqlScriptResult(commands, expected)
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
      val expected = Seq(Seq(Row(42)))
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
      val expected = Seq(Seq(Row(44)))
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
    val expected = Seq.empty
    verifySqlScriptResult(commands, expected)
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
      val expected = Seq(Seq(Row(43)))
      verifySqlScriptResult(commands, expected)
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
      Seq(Row(0)), // select i
      Seq(Row(1)), // select i
      Seq(Row(2)) // select i
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
    val expected = Seq.empty
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
      Seq(Row(0, 0)), // select i, j
      Seq(Row(0, 1)), // select i, j
      Seq(Row(1, 0)), // select i, j
      Seq(Row(1, 1)) // select i, j
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
        Seq(Row(42)), // select
        Seq(Row(42)) // select
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
      Seq(Row(0)), // select i
      Seq(Row(1)), // select i
      Seq(Row(2)) // select i
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

    val expected = Seq(Seq(Row(3)))
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
      Seq(Row(0, 0)), // select i, j
      Seq(Row(0, 1)), // select i, j
      Seq(Row(1, 0)), // select i, j
      Seq(Row(1, 1)) // select i, j
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
        Seq(Row(42)), // select
        Seq(Row(42)) // select
      )
      verifySqlScriptResult(commands, expected)
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
    val expected = Seq(Seq(Row(1)))
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
    val expected = Seq(Seq(Row(1)))
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
    val expected = Seq(Seq(Row(1)))
    verifySqlScriptResult(sqlScriptText, expected)
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
    val expected = Seq(Seq(Row(2)))
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
    val expected = Seq(Seq(Row(2)))
    verifySqlScriptResult(sqlScriptText, expected)
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
    val expected = Seq(Seq(Row(1)))
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
    val expected = Seq(Seq(Row(1)))
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
      Seq(Row(1)), // select 1
      Seq(Row(1)), // select 1
      Seq(Row(2)) // select x
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
      Seq(Row(1)), // select 1
      Seq(Row(2)), // select 2
      Seq(Row(1)), // select 1
      Seq(Row(2)), // select 2
      Seq(Row(2)) // select x
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
      Seq(Row(1)), // select 1
      Seq(Row(1)), // select 1
      Seq(Row(2)) // select x
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
      Seq(Row(1)), // select x
      Seq(Row(2)), // select x
      Seq(Row(3)), // select x
      Seq(Row(3)) // select x
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
      Seq(Row(0, 0)), // select x, y
      Seq(Row(0, 1)), // select x, y
      Seq(Row(1, 0)), // select x, y
      Seq(Row(1, 1)) // select x, y
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
    val expected = Seq(Seq(Row(2)))
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
    // Execution immediately leaves the outer loop after SELECT,
    //   so we expect only a single row in the result set.
    val expected = Seq(Seq(Row(1)))
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
      Seq(Row(1)), // select 1
      Seq(Row(1)), // select 1
      Seq(Row(3)) // select x
    )
    verifySqlScriptResult(sqlScriptText, expected)
  }
}
