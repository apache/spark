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
    super.sparkConf
      .set(SQLConf.ANSI_ENABLED.key, "true")
      .set(SQLConf.SQL_SCRIPTING_ENABLED.key, "true")
  }

  // Helpers
  private def runSqlScript(
      sqlText: String,
      args: Map[String, Expression] = Map.empty): Seq[Array[Row]] = {
    val compoundBody = spark.sessionState.sqlParser.parsePlan(sqlText).asInstanceOf[CompoundBody]

    val sse = new SqlScriptingExecution(compoundBody, spark, args)
    sse.withLocalVariableManager {
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
        |  DECLARE flag INT = -1;
        |  DECLARE EXIT HANDLER FOR duplicate_condition
        |  BEGIN
        |    SET flag = 1;
        |  END;
        |  DECLARE EXIT HANDLER FOR duplicate_condition
        |  BEGIN
        |    SET flag = 2;
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
        |  DECLARE flag INT = -1;
        |  DECLARE EXIT HANDLER FOR SQLSTATE '12345'
        |  BEGIN
        |    SET flag = 1;
        |  END;
        |  DECLARE EXIT HANDLER FOR SQLSTATE '12345'
        |  BEGIN
        |    SET flag = 2;
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
        |  DECLARE flag INT = -1;
        |  BEGIN
        |    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
        |    BEGIN
        |      SELECT flag;
        |      SET flag = 1;
        |    END;
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag;
        |      SET flag = 2;
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
        |  DECLARE flag INT = -1;
        |  DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
        |  BEGIN
        |    SELECT flag;
        |    SET flag = 1;
        |  END;
        |  BEGIN
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag;
        |      SET flag = 2;
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
        |  DECLARE VARIABLE flag INT = -1;
        |  scope_to_exit: BEGIN
        |    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
        |    BEGIN
        |      SELECT flag;
        |      SET flag = 1;
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
        |  DECLARE VARIABLE flag INT = -1;
        |  scope_to_exit: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag;
        |      SET flag = 1;
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
        |  DECLARE VARIABLE flag INT = -1;
        |  l1: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag;
        |      SET flag = 1;
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
        |  DECLARE VARIABLE flag INT = -1;
        |  l1: BEGIN
        |    DECLARE EXIT HANDLER FOR UNRESOLVED_COLUMN.WITHOUT_SUGGESTION
        |    BEGIN
        |      SELECT flag;
        |      SET flag = 2;
        |    END;
        |    l2: BEGIN
        |      DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
        |      BEGIN
        |        SELECT flag;
        |        SET flag = 1;
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
        |  DECLARE VARIABLE flag INT = -1;
        |  l1: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |    BEGIN
        |      SELECT flag;
        |      SET flag = 2;
        |    END;
        |    l2: BEGIN
        |      DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |      BEGIN
        |        SELECT flag;
        |        SET flag = 1;
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
        |  DECLARE VARIABLE flag INT = -1;
        |  l1: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |    BEGIN
        |      SELECT flag;
        |      SET flag = 3;
        |    END;
        |    l2: BEGIN
        |      DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |      BEGIN
        |        SELECT flag;
        |        SET flag = 2;
        |        SELECT 1/0;
        |        SELECT 2;
        |      END;
        |
        |      l3: BEGIN
        |        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |        BEGIN
        |          SELECT flag;
        |          SET flag = 1;
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
        |  DECLARE VARIABLE flag INT = -1;
        |  lbl_0: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |      lbl_1: BEGIN
        |        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |        lbl_2: BEGIN
        |          SELECT flag;
        |          SET flag = 2;
        |        END;
        |
        |        SELECT flag;
        |        SET flag = 1;
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
        |  DECLARE VARIABLE flag INT = -1;
        |  lbl_0: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |    lbl_1: BEGIN
        |      DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |      lbl_2: BEGIN
        |        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |        lbl_3: BEGIN
        |          SELECT flag; -- third select flag (2)
        |          SET flag = 3;
        |        END;
        |
        |        SELECT flag; -- second select flag (1)
        |        SET flag = 2;
        |        SELECT 1/0; -- third error will be thrown here
        |        SELECT 2;
        |      END;
        |
        |      SELECT flag; -- first select flag (-1)
        |      SET flag = 1;
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
        |  DECLARE flag INT = -1;
        |  l1: BEGIN
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SELECT flag;
        |      SET flag = 1;
        |    END;
        |    SELECT 2;
        |    SELECT 3;
        |    l2: BEGIN
        |      DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |      BEGIN
        |        SELECT flag;
        |        SET flag = 2;
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
        |  DECLARE flag INT = -1;
        |  DECLARE `22012` CONDITION FOR SQLSTATE '12345';
        |  DECLARE EXIT HANDLER FOR `22012`
        |  BEGIN
        |    SET flag = 1;
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
      queryContext = Array(ExpectedContext("", "", 159, 161, "1/0")))

    val sqlScript2 =
      """
        |BEGIN
        |  DECLARE flag INT = -1;
        |  BEGIN
        |    DECLARE `22012` CONDITION FOR SQLSTATE '12345';
        |    DECLARE EXIT HANDLER FOR `22012`
        |    BEGIN
        |      SET flag = 1;
        |    END;
        |
        |    DECLARE EXIT HANDLER FOR SQLSTATE '22012'
        |    BEGIN
        |      SET flag = 2;
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
        |  DECLARE flag INT = -1;
        |  BEGIN
        |    DECLARE `22012` CONDITION FOR SQLSTATE '12345';
        |    DECLARE EXIT HANDLER FOR `22012`, SQLSTATE '22012'
        |    BEGIN
        |      SET flag = 1;
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
        |   SET var = var + 1;
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
        |   SET i = i + 1;
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
        |   SET i = i + 1;
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
        |   SET j = 0;
        |   WHILE j < 2 DO
        |     SELECT i, j;
        |     SET j = j + 1;
        |   END WHILE;
        |   SET i = i + 1;
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
        |   SET i = i + 1;
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
        |   SET i = i + 1;
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
        |   SET j = 0;
        |   REPEAT
        |     SELECT i, j;
        |     SET j = j + 1;
        |   UNTIL j >= 2
        |   END REPEAT;
        |   SET i = i + 1;
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
        |   SET y = 0;
        |   lbl2: LOOP
        |     SELECT x, y;
        |     SET y = y + 1;
        |     IF y >= 2 THEN
        |       LEAVE lbl2;
        |     END IF;
        |   END LOOP;
        |   SET x = x + 1;
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

  test("local variable") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SELECT localVar;
        |    SELECT lbl.localVar;
        |  END;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq(Row(1)), // select localVar
      Seq(Row(1)) // select lbl.localVar
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("local variable - nested compounds") {
    val sqlScript =
      """
        |BEGIN
        |  lbl1: BEGIN
        |    DECLARE localVar = 1;
        |    lbl2: BEGIN
        |      DECLARE localVar = 2;
        |      SELECT localVar;
        |      SELECT lbl1.localVar;
        |      SELECT lbl2.localVar;
        |    END;
        |  END;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq(Row(2)), // select localVar
      Seq(Row(1)), // select lbl1.localVar
      Seq(Row(2)) // select lbl2.localVar
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("local variable - resolved over session variable") {
    withSessionVariable("localVar") {
      spark.sql("DECLARE VARIABLE localVar = 1")

      val sqlScript =
        """
          |BEGIN
          |  lbl: BEGIN
          |    DECLARE localVar = 5;
          |    SELECT localVar;
          |  END;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq(Row(5)) // select localVar
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("local variable - resolved over session variable nested") {
    withSessionVariable("localVar") {
      spark.sql("DECLARE VARIABLE localVar = 1")

      val sqlScript =
        """
          |BEGIN
          |  SELECT localVar;
          |  lbl: BEGIN
          |    DECLARE localVar = 5;
          |    SELECT localVar;
          |  END;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq(Row(1)), // select localVar
        Seq(Row(5)) // select localVar
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("local variable - session variable resolved over local if fully qualified") {
    withSessionVariable("localVar") {
      spark.sql("DECLARE VARIABLE localVar = 1")

      val sqlScript =
        """
          |BEGIN
          |  lbl: BEGIN
          |    DECLARE localVar = 5;
          |    SELECT system.session.localVar;
          |    SELECT localVar;
          |  END;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq(Row(1)), // select system.session.localVar
        Seq(Row(5)) // select localVar
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("local variable - session variable resolved over local if qualified with session.varname") {
    withSessionVariable("localVar") {
      spark.sql("DECLARE VARIABLE localVar = 1")

      val sqlScript =
        """
          |BEGIN
          |  lbl: BEGIN
          |    DECLARE localVar = 5;
          |    SELECT session.localVar;
          |    SELECT localVar;
          |  END;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq(Row(1)), // select system.session.localVar
        Seq(Row(5)) // select localVar
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("local variable - case insensitive name") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SELECT LOCALVAR;
        |  END;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq(Row(1)) // select LOCALVAR
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("local variable - case sensitive name") {
    val e = intercept[AnalysisException] {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> true.toString) {
        val sqlScript =
          """
            |BEGIN
            |  lbl: BEGIN
            |    DECLARE localVar = 1;
            |    SELECT LOCALVAR;
            |  END;
            |END
            |""".stripMargin

        verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
      }
    }
    checkError(
      exception = e,
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> toSQLId("LOCALVAR")),
      context = ExpectedContext(
        fragment = "LOCALVAR",
        start = 57,
        stop = 64)
    )
  }

  test("local variable - case insensitive label") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SELECT LBL.localVar;
        |  END;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq(Row(1)) // select LBL.localVar
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("local variable - case sensitive label") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SELECT LBL.localVar;
        |  END;
        |END
        |""".stripMargin

    val e = intercept[AnalysisException] {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> true.toString) {
        verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
      }
    }

    checkError(
      exception = e,
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> toSQLId("LBL.localVar")),
      context = ExpectedContext(
        fragment = "LBL.localVar",
        start = 57,
        stop = 68)
    )
  }

  test("local variable - leaves scope unqualified") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SELECT localVar;
        |  END;
        |  SELECT localVar;
        |END
        |""".stripMargin

    val e = intercept[AnalysisException] {
      verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
    }

    checkError(
      exception = e,
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> toSQLId("localVar")),
      context = ExpectedContext(fragment = "localVar", start = 83, stop = 90)
    )
  }

  test("local variable - leaves scope qualified") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SELECT lbl.localVar;
        |  END;
        |  SELECT lbl.localVar;
        |END
        |""".stripMargin

    val e = intercept[AnalysisException] {
      verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
    }

    checkError(
      exception = e,
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> toSQLId("lbl.localVar")),
      context = ExpectedContext(fragment = "lbl.localVar", start = 87, stop = 98)
    )
  }

  test("local variable - leaves inner scope") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE localVar = 1;
        |  lbl: BEGIN
        |    DECLARE localVar = 2;
        |    SELECT localVar;
        |  END;
        |  SELECT localVar;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq(Row(2)), // select localVar
      Seq(Row(1)) // select localVar
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("local variable - inner inner scope -> inner scope -> session var") {
    withSessionVariable("localVar") {
      spark.sql("DECLARE VARIABLE localVar = 0")

      val sqlScript =
        """
          |BEGIN
          |  lbl1: BEGIN
          |    DECLARE localVar = 1;
          |    lbl: BEGIN
          |       DECLARE localVar = 2;
          |       SELECT localVar;
          |    END;
          |    SELECT localVar;
          |  END;
          |  SELECT localVar;
          |END
          |""".stripMargin

      val expected = Seq(
        Seq(Row(2)), // select localVar
        Seq(Row(1)), // select localVar
        Seq(Row(0)) // select localVar
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("local variable - declare - qualified") {
      val sqlScript =
        """
          |BEGIN
          |  lbl: BEGIN
          |    DECLARE lbl.localVar = 1;
          |    SELECT lbl.localVar;
          |  END;
          |END
          |""".stripMargin

    val e = intercept[AnalysisException] {
      verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
    }

    checkError(
      exception = e,
      condition = "INVALID_VARIABLE_DECLARATION.QUALIFIED_LOCAL_VARIABLE",
      sqlState = "42K0M",
      parameters = Map("varName" -> toSQLId("lbl.localVar"))
    )
  }

  test("local variable - declare - declare or replace") {
      val sqlScript =
        """
          |BEGIN
          |  lbl: BEGIN
          |    DECLARE OR REPLACE localVar = 1;
          |    SELECT localVar;
          |  END;
          |END
          |""".stripMargin

    val e = intercept[AnalysisException] {
      verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
    }

    checkError(
      exception = e,
      condition = "INVALID_VARIABLE_DECLARATION.REPLACE_LOCAL_VARIABLE",
      sqlState = "42K0M",
      parameters = Map("varName" -> toSQLId("localVar"))
    )
  }

  test("local variable - declare - duplicate names") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    DECLARE localVar = 2;
        |    SELECT lbl.localVar;
        |  END;
        |END
        |""".stripMargin

    val e = intercept[AnalysisException] {
      verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
    }

    checkError(
      exception = e,
      condition = "VARIABLE_ALREADY_EXISTS",
      sqlState = "42723",
      parameters = Map("variableName" -> toSQLId("lbl.localvar"))
    )
  }

  // Variables cannot be dropped within SQL scripts.
  test("local variable - drop") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE localVar = 1;
        |  SELECT localVar;
        |  DROP TEMPORARY VARIABLE localVar;
        |END
        |""".stripMargin
    val e = intercept[AnalysisException] {
      verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
    }
    checkError(
      exception = e,
      condition = "UNSUPPORTED_FEATURE.SQL_SCRIPTING_DROP_TEMPORARY_VARIABLE",
      parameters = Map.empty
    )
  }

  test("drop variable - drop - too many nameparts") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SELECT localVar;
        |    DROP TEMPORARY VARIABLE a.b.c.d;
        |  END;
        |END
        |""".stripMargin
    val e = intercept[AnalysisException] {
      verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
    }
    checkError(
      exception = e,
      condition = "UNSUPPORTED_FEATURE.SQL_SCRIPTING_DROP_TEMPORARY_VARIABLE",
      parameters = Map.empty
    )
  }

  test("local variable - drop session variable without EXECUTE IMMEDIATE") {
    withSessionVariable("localVar") {
      spark.sql("DECLARE VARIABLE localVar = 0")

      val sqlScript =
      """
        |BEGIN
        |  DECLARE localVar = 1;
        |  SELECT system.session.localVar;
        |  DROP TEMPORARY VARIABLE system.session.localVar;
        |  SELECT system.session.localVar;
        |END
        |""".stripMargin
      val e = intercept[AnalysisException] {
        verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
      }
      checkError(
        exception = e,
        condition = "UNSUPPORTED_FEATURE.SQL_SCRIPTING_DROP_TEMPORARY_VARIABLE",
        parameters = Map.empty
      )
    }
  }

  test("local variable - drop session variable with EXECUTE IMMEDIATE") {
    withSessionVariable("localVar") {
      spark.sql("DECLARE VARIABLE localVar = 0")

      val sqlScript =
      """
        |BEGIN
        |  DECLARE localVar = 1;
        |  SELECT system.session.localVar;
        |  EXECUTE IMMEDIATE 'DROP TEMPORARY VARIABLE localVar';
        |  SELECT system.session.localVar;
        |END
        |""".stripMargin
      val e = intercept[AnalysisException] {
        verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
      }
      checkError(
        exception = e,
        condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
        sqlState = "42703",
        parameters = Map("objectName" -> toSQLId("system.session.localVar")),
        context = ExpectedContext(
          fragment = "system.session.localVar",
          start = 130,
          stop = 152)
      )
    }
  }

  test("local variable - set - qualified") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SELECT lbl.localVar;
        |    SET lbl.localVar = 5;
        |    SELECT lbl.localVar;
        |  END;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq(Row(1)), // select lbl.localVar
      Seq(Row(5)) // select lbl.localVar
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("local variable - set - unqualified") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SELECT localVar;
        |    SET localVar = 5;
        |    SELECT localVar;
        |  END;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq(Row(1)), // select localVar
      Seq(Row(5)) // select localVar
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("local variable - set - set unqualified select qualified") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SELECT lbl.localVar;
        |    SET localVar = 5;
        |    SELECT lbl.localVar;
        |  END;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq(Row(1)), // select lbl.localVar
      Seq(Row(5)) // select lbl.localVar
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("local variable - set - nested") {
    val sqlScript =
      """
        |BEGIN
        | lbl1: BEGIN
        |   DECLARE localVar = 1;
        |   lbl2: BEGIN
        |     DECLARE localVar = 2;
        |     SELECT localVar;
        |     SELECT lbl1.localVar;
        |     SELECT lbl2.localVar;
        |     SET lbl1.localVar = 5;
        |     SELECT lbl1.localVar;
        |   END;
        | END;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq(Row(2)), // select localVar
      Seq(Row(1)), // select lbl1.localVar
      Seq(Row(2)), // select lbl2.localVar
      Seq(Row(5)) // select lbl1.localVar
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("local variable - set - case insensitive name") {
    val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SET LOCALVAR = 5;
        |    SELECT localVar;
        |  END;
        |END
        |""".stripMargin

    val expected = Seq(
      Seq(Row(5)) // select localVar
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("local variable - set - case sensitive name") {
    val e = intercept[AnalysisException] {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> true.toString) {
        val sqlScript =
          """
            |BEGIN
            |  lbl: BEGIN
            |    DECLARE localVar = 1;
            |    SET LOCALVAR = 5;
            |    SELECT localVar;
            |  END;
            |END
            |""".stripMargin

        verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
      }
    }

    checkError(
      exception = e,
      condition = "UNRESOLVED_VARIABLE",
      sqlState = "42883",
      parameters = Map(
        "variableName" -> toSQLId("LOCALVAR"),
        "searchPath" -> toSQLId("SYSTEM.SESSION"))
    )
  }

  test("local variable - set - session variable") {
    withSessionVariable("localVar") {
      spark.sql("DECLARE VARIABLE localVar = 0").collect()

      val sqlScript =
      """
        |BEGIN
        |  SELECT localVar;
        |  SET localVar = 1;
        |  SELECT localVar;
        |END
        |""".stripMargin
      val expected = Seq(
        Seq(Row(0)), // select localVar
        Seq(Row(1)) // select localVar
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("local variable - set - duplicate assignment") {
    val sqlScript =
      """
        |BEGIN
        |  lbl1: BEGIN
        |    DECLARE localVar = 1;
        |    lbl2: BEGIN
        |      SELECT localVar;
        |      SET (localVar, lbl1.localVar) = (select 1, 2);
        |    END;
        |  END;
        |END
        |""".stripMargin

    val e = intercept[AnalysisException] {
      verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
    }

    checkError(
      exception = e,
      condition = "DUPLICATE_ASSIGNMENTS",
      sqlState = "42701",
      parameters = Map("nameList" -> toSQLId("localvar"))
    )
  }

  test("local variable - set - no duplicate assignment error with session var") {
    withSessionVariable("localVar") {
      spark.sql("DECLARE localVar = 0")

      val sqlScript =
      """
        |BEGIN
        |  lbl: BEGIN
        |    DECLARE localVar = 1;
        |    SET (localVar, session.localVar) = (select 2, 3);
        |    SELECT localVar;
        |    SELECT session.localVar;
        |  END;
        |END
        |""".stripMargin
      val expected = Seq(
        Seq(Row(2)), // select localVar
        Seq(Row(3)) // select session.localVar
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("local variable - set - duplicate assignment of session vars") {
    withSessionVariable("sessionVar") {
      spark.sql("DECLARE sessionVar = 0")

      val sqlScript =
      """
        |BEGIN
        |  lbl1: BEGIN
        |    SET (sessionVar, session.sessionVar) = (select 1, 2);
        |  END;
        |END
        |""".stripMargin
      val e = intercept[AnalysisException] {
        verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
      }

      checkError(
        exception = e,
        condition = "DUPLICATE_ASSIGNMENTS",
        sqlState = "42701",
        parameters = Map("nameList" -> toSQLId("sessionvar"))
      )
    }
  }

  test("local variable - execute immediate using local var") {
    withSessionVariable("testVar") {
      spark.sql("DECLARE testVar = 0")
      val sqlScript =
        """
          |BEGIN
          |  DECLARE testVar = 5;
          |  EXECUTE IMMEDIATE 'SELECT ?' USING testVar;
          |END
          |""".stripMargin
      val expected = Seq(
        Seq(Row(5)) // select testVar
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("local variable - execute immediate into local var") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE localVar = 1;
        |  SELECT localVar;
        |  EXECUTE IMMEDIATE 'SELECT 5' INTO localVar;
        |  SELECT localVar;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(1)), // select localVar
      Seq(Row(5)) // select localVar
    )
    verifySqlScriptResult(sqlScript, expected)
  }

  test("local variable - execute immediate can't access local var") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE localVar = 5;
        |  EXECUTE IMMEDIATE 'SELECT localVar';
        |END
        |""".stripMargin
    val e = intercept[AnalysisException] {
      verifySqlScriptResult(sqlScript, Seq.empty[Seq[Row]])
    }

    checkError(
      exception = e,
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> toSQLId("localVar")),
      context = ExpectedContext(
        fragment = "localVar",
        start = 7,
        stop = 14)
    )
  }

  test("local variable - execute immediate create session var") {
    withSessionVariable("sessionVar") {
      val sqlScript =
        """
          |BEGIN
          |  EXECUTE IMMEDIATE 'DECLARE sessionVar = 5';
          |  SELECT system.session.sessionVar;
          |  SELECT sessionVar;
          |END
          |""".stripMargin
      val expected = Seq(
        Seq(Row(5)), // select system.session.sessionVar
        Seq(Row(5)) // select sessionVar
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("local variable - execute immediate create qualified session var") {
    withSessionVariable("sessionVar") {
    val sqlScript =
      """
        |BEGIN
        |  EXECUTE IMMEDIATE 'DECLARE system.session.sessionVar = 5';
        |  SELECT system.session.sessionVar;
        |  SELECT sessionVar;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(5)), // select system.session.sessionVar
      Seq(Row(5)) // select sessionVar
    )
    verifySqlScriptResult(sqlScript, expected)
      }
  }

  test("local variable - execute immediate set session var") {
    withSessionVariable("testVar") {
      spark.sql("DECLARE testVar = 0")
      val sqlScript =
        """
          |BEGIN
          |  DECLARE testVar = 1;
          |  SELECT system.session.testVar;
          |  SELECT testVar;
          |  EXECUTE IMMEDIATE 'SET VARIABLE testVar = 5';
          |  SELECT system.session.testVar;
          |  SELECT testVar;
          |END
          |""".stripMargin
      val expected = Seq(
        Seq(Row(0)), // select system.session.testVar
        Seq(Row(1)), // select testVar
        Seq(Row(5)), // select system.session.testVar
        Seq(Row(1)) // select testVar
      )
      verifySqlScriptResult(sqlScript, expected)
    }
  }

  test("local variable - handlers - triple chained handlers") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE VARIABLE varOuter INT = 0;
        |  l1: BEGIN
        |    DECLARE VARIABLE varL1 INT = 1;
        |    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |    BEGIN
        |      SELECT varOuter;
        |      SELECT varL1;
        |    END;
        |    l2: BEGIN
        |      DECLARE VARIABLE varL2 = 2;
        |      DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |      BEGIN
        |        SELECT varOuter;
        |        SELECT varL1;
        |        SELECT varL2;
        |        SELECT 1/0;
        |      END;
        |      l3: BEGIN
        |        DECLARE VARIABLE varL3 = 3;
        |        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |        BEGIN
        |          SELECT varOuter;
        |          SELECT varL1;
        |          SELECT varL2;
        |          SELECT varL3;
        |          SELECT 1/0;
        |        END;

        |        SELECT 5;
        |        SELECT 1/0;
        |        SELECT 6;
        |      END;
        |    END;
        |  END;
        |END
        |""".stripMargin
    val expected = Seq(
      Seq(Row(5)),
      Seq(Row(0)),
      Seq(Row(1)),
      Seq(Row(2)),
      Seq(Row(3)),
      Seq(Row(0)),
      Seq(Row(1)),
      Seq(Row(2)),
      Seq(Row(0)),
      Seq(Row(1))
    )
    verifySqlScriptResult(sqlScript, expected = expected)
  }
}
