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
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.{CompoundBody, CreateVariable, IfElseStatement, SingleStatement, WhileStatement}
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/** For tests only */
object SparkSqlParser extends SparkSqlParser

/**
 * SQL Scripting parser tests.
 * Various SQL scripts are provided to the parser and the output plan is checked,
 *  including the whole plan hierarchy.
 */
class SqlScriptingParserSuite extends SparkFunSuite with SQLHelper with SharedSparkSession {
  import SparkSqlParser._

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
  test("multi select") {
    val sqlScriptText = "BEGIN SELECT 1;SELECT 2; END"
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 2)
    assert(tree.collection.forall(_.isInstanceOf[SingleStatement]))

    sqlScriptText.split(";")
      .map(cleanupStatementString)
      .zip(tree.collection)
      .foreach { case (expected, statement) =>
        val sparkStatement = statement.asInstanceOf[SingleStatement]
        val statementText = sparkStatement.getText
        assert(statementText == expected)
      }
  }

  test("empty BEGIN END block") {
    val sqlScriptText =
      """
        |BEGIN
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.isEmpty)
  }

  test("multiple ; in row - should fail") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;;
        |  SELECT 2;
        |END""".stripMargin
    val e = intercept[ParseException] {
      parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    }
    assert(e.getErrorClass === "PARSE_SYNTAX_ERROR")
    assert(e.getMessage.contains("Syntax error"))
    assert(e.getMessage.contains("at or near ';'"))
  }

  test("without ; in last statement - should fail") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;
        |  SELECT 2
        |END""".stripMargin
    val e = intercept[ParseException] {
      parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    }
    assert(e.getErrorClass === "PARSE_SYNTAX_ERROR")
    assert(e.getMessage.contains("Syntax error"))
    assert(e.getMessage.contains("at or near end of input"))
  }

  test("multi statement") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |  INSERT INTO A VALUES (a, b, 3);
        |  SELECT a, b, c FROM T;
        |  SELECT * FROM T;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 5)
    assert(tree.collection.forall(_.isInstanceOf[SingleStatement]))
    sqlScriptText.split(";")
      .map(cleanupStatementString)
      .zip(tree.collection)
      .foreach { case (expected, statement) =>
        val sparkStatement = statement.asInstanceOf[SingleStatement]
        val statementText = sparkStatement.getText
        assert(statementText == expected)
      }
  }

  test("nested begin end") {
    val sqlScriptText =
      """
        |BEGIN
        |  BEGIN
        |  SELECT 1;
        |  END;
        |  BEGIN
        |    BEGIN
        |      SELECT 2;
        |      SELECT 3;
        |    END;
        |  END;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 2)
    assert(tree.collection.head.isInstanceOf[CompoundBody])
    val body1 = tree.collection.head.asInstanceOf[CompoundBody]
    assert(body1.collection.length == 1)
    assert(body1.collection.head.asInstanceOf[SingleStatement].getText
      == "SELECT 1")

    val body2 = tree.collection(1).asInstanceOf[CompoundBody]
    assert(body2.collection.length == 1)
    assert(body2.collection.head.isInstanceOf[CompoundBody])
    val nestedBody = body2.collection.head.asInstanceOf[CompoundBody]
    assert(nestedBody.collection.head.asInstanceOf[SingleStatement].getText
      == "SELECT 2")
    assert(nestedBody.collection(1).asInstanceOf[SingleStatement].getText
      == "SELECT 3")
  }

  // TODO: to be removed once the parser rule for top level compound is fixed to support labels!
  test("top level compound: labels not allowed") {
    val sqlScriptText =
      """
        |lbl: BEGIN
        |  SELECT 1;
        |END""".stripMargin
    checkError(
      exception = intercept[ParseException] {
        parsePlan(sqlScriptText)
      },
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'lbl'", "hint" -> ""))
  }

  test("compound: beginLabel") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END;
        |END""".stripMargin
    val rootTree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(rootTree.collection.length == 1)

    val tree = rootTree.collection.head.asInstanceOf[CompoundBody]
    assert(tree.collection.length == 5)
    assert(tree.collection.forall(_.isInstanceOf[SingleStatement]))
    assert(tree.label.contains("lbl"))
  }

  test("compound: beginLabel + endLabel") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END lbl;
        |END""".stripMargin
    val rootTree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(rootTree.collection.length == 1)

    val tree = rootTree.collection.head.asInstanceOf[CompoundBody]
    assert(tree.collection.length == 5)
    assert(tree.collection.forall(_.isInstanceOf[SingleStatement]))
    assert(tree.label.contains("lbl"))
  }

  test("compound: beginLabel + endLabel with different values") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl_begin: BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END lbl_end;
        |END""".stripMargin

    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScriptText)
      },
      errorClass = "LABELS_MISMATCH",
      parameters = Map("beginLabel" -> "lbl_begin", "endLabel" -> "lbl_end"))
  }

  test("compound: endLabel") {
    val sqlScriptText =
      """
        |BEGIN
        |  BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END lbl;
        |END""".stripMargin

    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScriptText)
      },
      errorClass = "END_LABEL_WITHOUT_BEGIN_LABEL",
      parameters = Map("endLabel" -> "lbl"))
  }

  test("compound: beginLabel + endLabel with different casing") {
    val sqlScriptText =
      """
        |BEGIN
        |  LBL: BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END lbl;
        |END""".stripMargin
    val rootTree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(rootTree.collection.length == 1)

    val tree = rootTree.collection.head.asInstanceOf[CompoundBody]
    assert(tree.collection.length == 5)
    assert(tree.collection.forall(_.isInstanceOf[SingleStatement]))
    assert(tree.label.contains("lbl"))
  }

  test("compound: no labels provided, random label should be generated") {
    val sqlScriptText =
      """
        |BEGIN
        |  BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END;
        |END""".stripMargin
    val rootTree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(rootTree.collection.length == 1)

    val tree = rootTree.collection.head.asInstanceOf[CompoundBody]
    assert(tree.collection.length == 5)
    assert(tree.collection.forall(_.isInstanceOf[SingleStatement]))
    assert(tree.label.nonEmpty)
  }

  test("declare at the beginning") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE testVariable1 VARCHAR(50);
        |  DECLARE testVariable2 INTEGER;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 2)
    assert(tree.collection.forall(_.isInstanceOf[SingleStatement]))
    assert(tree.collection.forall(
      _.asInstanceOf[SingleStatement].parsedPlan.isInstanceOf[CreateVariable]))
  }

  test("declare after beginning") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;
        |  DECLARE testVariable INTEGER;
        |END""".stripMargin
    checkError(
        exception = intercept[SqlScriptingException] {
          parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
        },
        errorClass = "INVALID_VARIABLE_DECLARATION.ONLY_AT_BEGINNING",
        parameters = Map("varName" -> "`testVariable`", "lineNumber" -> "4"))
  }

  test("declare in wrong scope") {
    val sqlScriptText =
      """
        |BEGIN
        | IF 1=1 THEN
        |   DECLARE testVariable INTEGER;
        | END IF;
        |END""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
      },
      errorClass = "INVALID_VARIABLE_DECLARATION.NOT_ALLOWED_IN_SCOPE",
      parameters = Map("varName" -> "`testVariable`", "lineNumber" -> "4"))
  }

  test("SET VAR statement test") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE totalInsCnt = 0;
        |  SET VAR totalInsCnt = (SELECT x FROM y WHERE id = 1);
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 2)
    assert(tree.collection.head.isInstanceOf[SingleStatement])
    assert(tree.collection(1).isInstanceOf[SingleStatement])
  }

  test("SET VARIABLE statement test") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE totalInsCnt = 0;
        |  SET VARIABLE totalInsCnt = (SELECT x FROM y WHERE id = 1);
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 2)
    assert(tree.collection.head.isInstanceOf[SingleStatement])
    assert(tree.collection(1).isInstanceOf[SingleStatement])
  }

  test("SET statement test") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE totalInsCnt = 0;
        |  SET totalInsCnt = (SELECT x FROM y WHERE id = 1);
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 2)
    assert(tree.collection.head.isInstanceOf[SingleStatement])
    assert(tree.collection(1).isInstanceOf[SingleStatement])
  }

  test("SET statement test - should fail") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE totalInsCnt = 0;
        |  SET totalInsCnt = (SELECT x FROMERROR y WHERE id = 1);
        |END""".stripMargin
    val e = intercept[ParseException] {
      parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    }
    assert(e.getErrorClass === "PARSE_SYNTAX_ERROR")
    assert(e.getMessage.contains("Syntax error"))
  }

  test("if") {
    val sqlScriptText =
      """
        |BEGIN
        | IF 1=1 THEN
        |   SELECT 42;
        | END IF;
        |END
        |""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[IfElseStatement])
    val ifStmt = tree.collection.head.asInstanceOf[IfElseStatement]
    assert(ifStmt.conditions.length == 1)
    assert(ifStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditions.head.getText == "1=1")
  }

  test("if else") {
    val sqlScriptText =
      """BEGIN
        |IF 1 = 1 THEN
        |  SELECT 1;
        |ELSE
        |  SELECT 2;
        |END IF;
        |END
        """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[IfElseStatement])

    val ifStmt = tree.collection.head.asInstanceOf[IfElseStatement]
    assert(ifStmt.conditions.length == 1)
    assert(ifStmt.conditionalBodies.length == 1)
    assert(ifStmt.elseBody.isDefined)

    assert(ifStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditions.head.getText == "1 = 1")

    assert(ifStmt.conditionalBodies.head.collection.length == 1)
    assert(ifStmt.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 1")

    assert(ifStmt.elseBody.get.collection.length == 1)
    assert(ifStmt.elseBody.get.collection.head.isInstanceOf[SingleStatement])
    assert(ifStmt.elseBody.get.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 2")
  }

  test("if else if") {
    val sqlScriptText =
      """BEGIN
        |IF 1 = 1 THEN
        |  SELECT 1;
        |ELSE IF 2 = 2 THEN
        |  SELECT 2;
        |ELSE
        |  SELECT 3;
        |END IF;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[IfElseStatement])

    val ifStmt = tree.collection.head.asInstanceOf[IfElseStatement]
    assert(ifStmt.conditions.length == 2)
    assert(ifStmt.conditionalBodies.length == 2)
    assert(ifStmt.elseBody.isDefined)

    assert(ifStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditions.head.getText == "1 = 1")

    assert(ifStmt.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 1")

    assert(ifStmt.conditions(1).isInstanceOf[SingleStatement])
    assert(ifStmt.conditions(1).getText == "2 = 2")

    assert(ifStmt.conditionalBodies(1).collection.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditionalBodies(1).collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 2")

    assert(ifStmt.elseBody.get.collection.head.isInstanceOf[SingleStatement])
    assert(ifStmt.elseBody.get.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 3")
  }

  test("if multi else if") {
    val sqlScriptText =
      """BEGIN
        |IF 1 = 1 THEN
        |  SELECT 1;
        |ELSE IF 2 = 2 THEN
        |  SELECT 2;
        |ELSE IF 3 = 3 THEN
        |  SELECT 3;
        |END IF;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[IfElseStatement])

    val ifStmt = tree.collection.head.asInstanceOf[IfElseStatement]
    assert(ifStmt.conditions.length == 3)
    assert(ifStmt.conditionalBodies.length == 3)
    assert(ifStmt.elseBody.isEmpty)

    assert(ifStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditions.head.getText == "1 = 1")

    assert(ifStmt.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 1")

    assert(ifStmt.conditions(1).isInstanceOf[SingleStatement])
    assert(ifStmt.conditions(1).getText == "2 = 2")

    assert(ifStmt.conditionalBodies(1).collection.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditionalBodies(1).collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 2")

    assert(ifStmt.conditions(2).isInstanceOf[SingleStatement])
    assert(ifStmt.conditions(2).getText == "3 = 3")

    assert(ifStmt.conditionalBodies(2).collection.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditionalBodies(2).collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 3")
  }

  test("if nested") {
    val sqlScriptText =
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[IfElseStatement])

    val ifStmt = tree.collection.head.asInstanceOf[IfElseStatement]
    assert(ifStmt.conditions.length == 1)
    assert(ifStmt.conditionalBodies.length == 1)
    assert(ifStmt.elseBody.isEmpty)

    assert(ifStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditions.head.getText == "1=1")

    assert(ifStmt.conditionalBodies.head.collection.head.isInstanceOf[IfElseStatement])
    val nestedIfStmt = ifStmt.conditionalBodies.head.collection.head.asInstanceOf[IfElseStatement]

    assert(nestedIfStmt.conditions.length == 1)
    assert(nestedIfStmt.conditionalBodies.length == 1)
    assert(nestedIfStmt.elseBody.isDefined)

    assert(nestedIfStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(nestedIfStmt.conditions.head.getText == "2=1")

    assert(nestedIfStmt.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(nestedIfStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 41")

    assert(nestedIfStmt.elseBody.get.collection.head.isInstanceOf[SingleStatement])
    assert(nestedIfStmt.elseBody.get.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 42")
  }

  test("while") {
    val sqlScriptText =
      """BEGIN
        |lbl: WHILE 1 = 1 DO
        |  SELECT 1;
        |END WHILE lbl;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[WhileStatement])

    val whileStmt = tree.collection.head.asInstanceOf[WhileStatement]
    assert(whileStmt.condition.isInstanceOf[SingleStatement])
    assert(whileStmt.condition.getText == "1 = 1")

    assert(whileStmt.body.isInstanceOf[CompoundBody])
    assert(whileStmt.body.collection.length == 1)
    assert(whileStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(whileStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(whileStmt.label.contains("lbl"))
  }

  test("while with complex condition") {
    val sqlScriptText =
    """
      |BEGIN
      |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
      |WHILE (SELECT COUNT(*) < 2 FROM t) DO
      |  SELECT 42;
      |END WHILE;
      |END
      |""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 2)
    assert(tree.collection(1).isInstanceOf[WhileStatement])

    val whileStmt = tree.collection(1).asInstanceOf[WhileStatement]
    assert(whileStmt.condition.isInstanceOf[SingleStatement])
    assert(whileStmt.condition.getText == "(SELECT COUNT(*) < 2 FROM t)")

    assert(whileStmt.body.isInstanceOf[CompoundBody])
    assert(whileStmt.body.collection.length == 1)
    assert(whileStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(whileStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 42")
  }

  test("while with if else block") {
    val sqlScriptText =
      """BEGIN
        |lbl: WHILE 1 = 1 DO
        |  IF 1 = 1 THEN
        |    SELECT 1;
        |  ELSE
        |    SELECT 2;
        |  END IF;
        |END WHILE lbl;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[WhileStatement])

    val whileStmt = tree.collection.head.asInstanceOf[WhileStatement]
    assert(whileStmt.condition.isInstanceOf[SingleStatement])
    assert(whileStmt.condition.getText == "1 = 1")

    assert(whileStmt.body.isInstanceOf[CompoundBody])
    assert(whileStmt.body.collection.length == 1)
    assert(whileStmt.body.collection.head.isInstanceOf[IfElseStatement])
    val ifStmt = whileStmt.body.collection.head.asInstanceOf[IfElseStatement]

    assert(ifStmt.conditions.length == 1)
    assert(ifStmt.conditionalBodies.length == 1)
    assert(ifStmt.elseBody.isDefined)

    assert(ifStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditions.head.getText == "1 = 1")

    assert(ifStmt.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(ifStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 1")

    assert(ifStmt.elseBody.get.collection.head.isInstanceOf[SingleStatement])
    assert(ifStmt.elseBody.get.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 2")

    assert(whileStmt.label.contains("lbl"))
  }

  test("nested while") {
    val sqlScriptText =
      """BEGIN
        |lbl: WHILE 1 = 1 DO
        |  WHILE 2 = 2 DO
        |    SELECT 42;
        |  END WHILE;
        |END WHILE lbl;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[WhileStatement])

    val whileStmt = tree.collection.head.asInstanceOf[WhileStatement]
    assert(whileStmt.condition.isInstanceOf[SingleStatement])
    assert(whileStmt.condition.getText == "1 = 1")

    assert(whileStmt.body.isInstanceOf[CompoundBody])
    assert(whileStmt.body.collection.length == 1)
    assert(whileStmt.body.collection.head.isInstanceOf[WhileStatement])
    val nestedWhileStmt = whileStmt.body.collection.head.asInstanceOf[WhileStatement]

    assert(nestedWhileStmt.condition.isInstanceOf[SingleStatement])
    assert(nestedWhileStmt.condition.getText == "2 = 2")

    assert(nestedWhileStmt.body.isInstanceOf[CompoundBody])
    assert(nestedWhileStmt.body.collection.length == 1)
    assert(nestedWhileStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(nestedWhileStmt.body.collection.
      head.asInstanceOf[SingleStatement].getText == "SELECT 42")

    assert(whileStmt.label.contains("lbl"))
  }

  // Helper methods
  def cleanupStatementString(statementStr: String): String = {
    statementStr
      .replace("\n", "")
      .replace("BEGIN", "")
      .replace("END", "")
      .trim
  }
}
