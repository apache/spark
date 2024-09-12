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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, Expression, In, Literal, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.{CreateVariable, Project}
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.exceptions.SqlScriptingException

class SqlScriptingParserSuite extends SparkFunSuite with SQLHelper {
  import CatalystSqlParser._

  test("single select") {
    val sqlScriptText = "SELECT 1;"
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SingleStatement])
    val sparkStatement = tree.collection.head.asInstanceOf[SingleStatement]
    assert(sparkStatement.getText == "SELECT 1;")
  }

  test("single select without ;") {
    val sqlScriptText = "SELECT 1"
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SingleStatement])
    val sparkStatement = tree.collection.head.asInstanceOf[SingleStatement]
    assert(sparkStatement.getText == "SELECT 1")
  }

  test("multi select without ; - should fail") {
    val sqlScriptText = "SELECT 1 SELECT 1"
    val e = intercept[ParseException] {
      parseScript(sqlScriptText)
    }
    assert(e.getErrorClass === "PARSE_SYNTAX_ERROR")
    assert(e.getMessage.contains("Syntax error"))
    assert(e.getMessage.contains("SELECT"))
  }

  test("multi select") {
    val sqlScriptText = "BEGIN SELECT 1;SELECT 2; END"
    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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
      parseScript(sqlScriptText)
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
      parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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

  test("compound: beginLabel") {
    val sqlScriptText =
      """
        |lbl: BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |  INSERT INTO A VALUES (a, b, 3);
        |  SELECT a, b, c FROM T;
        |  SELECT * FROM T;
        |END""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 5)
    assert(tree.collection.forall(_.isInstanceOf[SingleStatement]))
    assert(tree.label.contains("lbl"))
  }

  test("compound: beginLabel + endLabel") {
    val sqlScriptText =
      """
        |lbl: BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |  INSERT INTO A VALUES (a, b, 3);
        |  SELECT a, b, c FROM T;
        |  SELECT * FROM T;
        |END lbl""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 5)
    assert(tree.collection.forall(_.isInstanceOf[SingleStatement]))
    assert(tree.label.contains("lbl"))
  }

  test("compound: beginLabel + endLabel with different values") {
    val sqlScriptText =
      """
        |lbl_begin: BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |  INSERT INTO A VALUES (a, b, 3);
        |  SELECT a, b, c FROM T;
        |  SELECT * FROM T;
        |END lbl_end""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parseScript(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "LABELS_MISMATCH",
      parameters = Map("beginLabel" -> toSQLId("lbl_begin"), "endLabel" -> toSQLId("lbl_end")))
    assert(exception.origin.line.contains(2))
  }

  test("compound: endLabel") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |  INSERT INTO A VALUES (a, b, 3);
        |  SELECT a, b, c FROM T;
        |  SELECT * FROM T;
        |END lbl""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parseScript(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "END_LABEL_WITHOUT_BEGIN_LABEL",
      parameters = Map("endLabel" -> toSQLId("lbl")))
    assert(exception.origin.line.isDefined)
    assert(exception.origin.line.get == 8)
  }

  test("compound: beginLabel + endLabel with different casing") {
    val sqlScriptText =
      """
        |LBL: BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |  INSERT INTO A VALUES (a, b, 3);
        |  SELECT a, b, c FROM T;
        |  SELECT * FROM T;
        |END lbl""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 5)
    assert(tree.collection.forall(_.isInstanceOf[SingleStatement]))
    assert(tree.label.contains("lbl"))
  }

  test("compound: no labels provided") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |  INSERT INTO A VALUES (a, b, 3);
        |  SELECT a, b, c FROM T;
        |  SELECT * FROM T;
        |END""".stripMargin
    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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
    val exception = intercept[SqlScriptingException] {
      parseScript(sqlScriptText)
    }
    checkError(
        exception = exception,
        condition = "INVALID_VARIABLE_DECLARATION.ONLY_AT_BEGINNING",
        parameters = Map("varName" -> "`testVariable`"))
    assert(exception.origin.line.isDefined)
    assert(exception.origin.line.get == 4)
  }

  test("declare in wrong scope") {
    val sqlScriptText =
      """
        |BEGIN
        | IF 1=1 THEN
        |   DECLARE testVariable INTEGER;
        | END IF;
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parseScript(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "INVALID_VARIABLE_DECLARATION.NOT_ALLOWED_IN_SCOPE",
      parameters = Map("varName" -> "`testVariable`"))
    assert(exception.origin.line.isDefined)
    assert(exception.origin.line.get == 4)
  }

  test("SET VAR statement test") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE totalInsCnt = 0;
        |  SET VAR totalInsCnt = (SELECT x FROM y WHERE id = 1);
        |END""".stripMargin
    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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
      parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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

    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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
    val tree = parseScript(sqlScriptText)
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

  test("leave compound block") {
    val sqlScriptText =
      """
        |lbl: BEGIN
        |  SELECT 1;
        |  LEAVE lbl;
        |END""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 2)
    assert(tree.collection.head.isInstanceOf[SingleStatement])
    assert(tree.collection(1).isInstanceOf[LeaveStatement])
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
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[WhileStatement])

    val whileStmt = tree.collection.head.asInstanceOf[WhileStatement]
    assert(whileStmt.condition.isInstanceOf[SingleStatement])
    assert(whileStmt.condition.getText == "1 = 1")

    assert(whileStmt.body.isInstanceOf[CompoundBody])
    assert(whileStmt.body.collection.length == 2)

    assert(whileStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(whileStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(whileStmt.body.collection(1).isInstanceOf[LeaveStatement])
    assert(whileStmt.body.collection(1).asInstanceOf[LeaveStatement].label == "lbl")
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
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[RepeatStatement])

    val repeatStmt = tree.collection.head.asInstanceOf[RepeatStatement]
    assert(repeatStmt.condition.isInstanceOf[SingleStatement])
    assert(repeatStmt.condition.getText == "1 = 2")

    assert(repeatStmt.body.isInstanceOf[CompoundBody])
    assert(repeatStmt.body.collection.length == 2)

    assert(repeatStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(repeatStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(repeatStmt.body.collection(1).isInstanceOf[LeaveStatement])
    assert(repeatStmt.body.collection(1).asInstanceOf[LeaveStatement].label == "lbl")
  }

  test ("iterate compound block - should fail") {
    val sqlScriptText =
      """
        |lbl: BEGIN
        |  SELECT 1;
        |  ITERATE lbl;
        |END""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        parseScript(sqlScriptText)
      },
      condition = "INVALID_LABEL_USAGE.ITERATE_IN_COMPOUND",
      parameters = Map("labelName" -> "LBL"))
  }

  test("iterate while loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: WHILE 1 = 1 DO
        |    SELECT 1;
        |    ITERATE lbl;
        |  END WHILE;
        |END""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[WhileStatement])

    val whileStmt = tree.collection.head.asInstanceOf[WhileStatement]
    assert(whileStmt.condition.isInstanceOf[SingleStatement])
    assert(whileStmt.condition.getText == "1 = 1")

    assert(whileStmt.body.isInstanceOf[CompoundBody])
    assert(whileStmt.body.collection.length == 2)

    assert(whileStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(whileStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(whileStmt.body.collection(1).isInstanceOf[IterateStatement])
    assert(whileStmt.body.collection(1).asInstanceOf[IterateStatement].label == "lbl")
  }

  test("iterate repeat loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: REPEAT
        |    SELECT 1;
        |    ITERATE lbl;
        |  UNTIL 1 = 2
        |  END REPEAT;
        |END""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[RepeatStatement])

    val repeatStmt = tree.collection.head.asInstanceOf[RepeatStatement]
    assert(repeatStmt.condition.isInstanceOf[SingleStatement])
    assert(repeatStmt.condition.getText == "1 = 2")

    assert(repeatStmt.body.isInstanceOf[CompoundBody])
    assert(repeatStmt.body.collection.length == 2)

    assert(repeatStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(repeatStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(repeatStmt.body.collection(1).isInstanceOf[IterateStatement])
    assert(repeatStmt.body.collection(1).asInstanceOf[IterateStatement].label == "lbl")
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
        parseScript(sqlScriptText)
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
        parseScript(sqlScriptText)
      },
      condition = "INVALID_LABEL_USAGE.DOES_NOT_EXIST",
      parameters = Map("labelName" -> "RANDOMLBL", "statementType" -> "ITERATE"))
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
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[WhileStatement])

    val whileStmt = tree.collection.head.asInstanceOf[WhileStatement]
    assert(whileStmt.condition.isInstanceOf[SingleStatement])
    assert(whileStmt.condition.getText == "1 = 1")

    assert(whileStmt.body.isInstanceOf[CompoundBody])
    assert(whileStmt.body.collection.length == 1)

    val nestedWhileStmt = whileStmt.body.collection.head.asInstanceOf[WhileStatement]
    assert(nestedWhileStmt.condition.isInstanceOf[SingleStatement])
    assert(nestedWhileStmt.condition.getText == "2 = 2")

    assert(nestedWhileStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(nestedWhileStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(nestedWhileStmt.body.collection(1).isInstanceOf[LeaveStatement])
    assert(nestedWhileStmt.body.collection(1).asInstanceOf[LeaveStatement].label == "lbl")
  }

  test("leave outer loop from nested repeat loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: REPEAT
        |    lbl2: REPEAT
        |      SELECT 1;
        |      LEAVE lbl;
        |    UNTIL 2 = 2
        |    END REPEAT;
        |  UNTIL 1 = 1
        |  END REPEAT;
        |END""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[RepeatStatement])

    val repeatStmt = tree.collection.head.asInstanceOf[RepeatStatement]
    assert(repeatStmt.condition.isInstanceOf[SingleStatement])
    assert(repeatStmt.condition.getText == "1 = 1")

    assert(repeatStmt.body.isInstanceOf[CompoundBody])
    assert(repeatStmt.body.collection.length == 1)

    val nestedRepeatStmt = repeatStmt.body.collection.head.asInstanceOf[RepeatStatement]
    assert(nestedRepeatStmt.condition.isInstanceOf[SingleStatement])
    assert(nestedRepeatStmt.condition.getText == "2 = 2")

    assert(nestedRepeatStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(
      nestedRepeatStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(nestedRepeatStmt.body.collection(1).isInstanceOf[LeaveStatement])
    assert(nestedRepeatStmt.body.collection(1).asInstanceOf[LeaveStatement].label == "lbl")
  }

  test("iterate outer loop from nested while loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: WHILE 1 = 1 DO
        |    lbl2: WHILE 2 = 2 DO
        |      SELECT 1;
        |      ITERATE lbl;
        |    END WHILE;
        |  END WHILE;
        |END""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[WhileStatement])

    val whileStmt = tree.collection.head.asInstanceOf[WhileStatement]
    assert(whileStmt.condition.isInstanceOf[SingleStatement])
    assert(whileStmt.condition.getText == "1 = 1")

    assert(whileStmt.body.isInstanceOf[CompoundBody])
    assert(whileStmt.body.collection.length == 1)

    val nestedWhileStmt = whileStmt.body.collection.head.asInstanceOf[WhileStatement]
    assert(nestedWhileStmt.condition.isInstanceOf[SingleStatement])
    assert(nestedWhileStmt.condition.getText == "2 = 2")

    assert(nestedWhileStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(nestedWhileStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(nestedWhileStmt.body.collection(1).isInstanceOf[IterateStatement])
    assert(nestedWhileStmt.body.collection(1).asInstanceOf[IterateStatement].label == "lbl")
  }

  test("iterate outer loop from nested repeat loop") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: REPEAT
        |    lbl2: REPEAT
        |      SELECT 1;
        |      ITERATE lbl;
        |    UNTIL 2 = 2
        |    END REPEAT;
        |  UNTIL 1 = 1
        |  END REPEAT;
        |END""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[RepeatStatement])

    val repeatStmt = tree.collection.head.asInstanceOf[RepeatStatement]
    assert(repeatStmt.condition.isInstanceOf[SingleStatement])
    assert(repeatStmt.condition.getText == "1 = 1")

    assert(repeatStmt.body.isInstanceOf[CompoundBody])
    assert(repeatStmt.body.collection.length == 1)

    val nestedRepeatStmt = repeatStmt.body.collection.head.asInstanceOf[RepeatStatement]
    assert(nestedRepeatStmt.condition.isInstanceOf[SingleStatement])
    assert(nestedRepeatStmt.condition.getText == "2 = 2")

    assert(nestedRepeatStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(
      nestedRepeatStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(nestedRepeatStmt.body.collection(1).isInstanceOf[IterateStatement])
    assert(nestedRepeatStmt.body.collection(1).asInstanceOf[IterateStatement].label == "lbl")
  }

  test("repeat") {
    val sqlScriptText =
      """BEGIN
        |lbl: REPEAT
        |  SELECT 1;
        | UNTIL 1 = 1
        |END REPEAT lbl;
        |END
      """.stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[RepeatStatement])

    val repeatStmt = tree.collection.head.asInstanceOf[RepeatStatement]
    assert(repeatStmt.condition.isInstanceOf[SingleStatement])
    assert(repeatStmt.condition.getText == "1 = 1")

    assert(repeatStmt.body.isInstanceOf[CompoundBody])
    assert(repeatStmt.body.collection.length == 1)
    assert(repeatStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(repeatStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(repeatStmt.label.contains("lbl"))
  }

  test("repeat with complex condition") {
    val sqlScriptText =
      """
        |BEGIN
        |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
        |REPEAT
        | SELECT 42;
        |UNTIL
        | (SELECT COUNT(*) < 2 FROM t)
        |END REPEAT;
        |END
        |""".stripMargin

    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 2)
    assert(tree.collection(1).isInstanceOf[RepeatStatement])

    val repeatStmt = tree.collection(1).asInstanceOf[RepeatStatement]
    assert(repeatStmt.condition.isInstanceOf[SingleStatement])
    assert(repeatStmt.condition.getText == "(SELECT COUNT(*) < 2 FROM t)")

    assert(repeatStmt.body.isInstanceOf[CompoundBody])
    assert(repeatStmt.body.collection.length == 1)
    assert(repeatStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(repeatStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 42")
  }

  test("repeat with if else block") {
    val sqlScriptText =
      """BEGIN
        |lbl: REPEAT
        |  IF 1 = 1 THEN
        |    SELECT 1;
        |  ELSE
        |    SELECT 2;
        |  END IF;
        |UNTIL
        |  1 = 1
        |END REPEAT lbl;
        |END
      """.stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[RepeatStatement])

    val whileStmt = tree.collection.head.asInstanceOf[RepeatStatement]
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

  test("nested repeat") {
    val sqlScriptText =
      """BEGIN
        |lbl: REPEAT
        |  REPEAT
        |    SELECT 42;
        |  UNTIL
        |    2 = 2
        |  END REPEAT;
        |UNTIL
        |   1 = 1
        |END REPEAT lbl;
        |END
      """.stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[RepeatStatement])

    val whileStmt = tree.collection.head.asInstanceOf[RepeatStatement]
    assert(whileStmt.condition.isInstanceOf[SingleStatement])
    assert(whileStmt.condition.getText == "1 = 1")

    assert(whileStmt.body.isInstanceOf[CompoundBody])
    assert(whileStmt.body.collection.length == 1)
    assert(whileStmt.body.collection.head.isInstanceOf[RepeatStatement])
    val nestedWhileStmt = whileStmt.body.collection.head.asInstanceOf[RepeatStatement]

    assert(nestedWhileStmt.condition.isInstanceOf[SingleStatement])
    assert(nestedWhileStmt.condition.getText == "2 = 2")

    assert(nestedWhileStmt.body.isInstanceOf[CompoundBody])
    assert(nestedWhileStmt.body.collection.length == 1)
    assert(nestedWhileStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(nestedWhileStmt.body.collection.
      head.asInstanceOf[SingleStatement].getText == "SELECT 42")

    assert(whileStmt.label.contains("lbl"))

  }

  test("searched case statement") {
    val sqlScriptText =
      """
        |BEGIN
        | CASE
        |   WHEN 1 = 1 THEN
        |     SELECT 42;
        | END CASE;
        |END
        |""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[CaseStatement])
    val caseStmt = tree.collection.head.asInstanceOf[CaseStatement]
    assert(caseStmt.conditions.length == 1)
    assert(caseStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditions.head.getText == "1 = 1")
  }

  test("searched case statement - multi when") {
    val sqlScriptText =
      """
        |BEGIN
        | CASE
        |   WHEN 1 IN (1,2,3) THEN
        |     SELECT 1;
        |   WHEN (SELECT * FROM t) THEN
        |     SELECT * FROM b;
        |   WHEN 1 = 1 THEN
        |     SELECT 42;
        | END CASE;
        |END
        |""".stripMargin
    val tree = parseScript(sqlScriptText)

    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[CaseStatement])

    val caseStmt = tree.collection.head.asInstanceOf[CaseStatement]
    assert(caseStmt.conditions.length == 3)
    assert(caseStmt.conditionalBodies.length == 3)
    assert(caseStmt.elseBody.isEmpty)

    assert(caseStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditions.head.getText == "1 IN (1,2,3)")

    assert(caseStmt.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 1")

    assert(caseStmt.conditions(1).isInstanceOf[SingleStatement])
    assert(caseStmt.conditions(1).getText == "(SELECT * FROM t)")

    assert(caseStmt.conditionalBodies(1).collection.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditionalBodies(1).collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT * FROM b")

    assert(caseStmt.conditions(2).isInstanceOf[SingleStatement])
    assert(caseStmt.conditions(2).getText == "1 = 1")

    assert(caseStmt.conditionalBodies(2).collection.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditionalBodies(2).collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 42")
  }

  test("searched case statement with else") {
    val sqlScriptText =
      """
        |BEGIN
        | CASE
        |   WHEN 1 = 1 THEN
        |     SELECT 42;
        |   ELSE
        |     SELECT 43;
        | END CASE;
        |END
        |""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[CaseStatement])
    val caseStmt = tree.collection.head.asInstanceOf[CaseStatement]
    assert(caseStmt.elseBody.isDefined)
    assert(caseStmt.conditions.length == 1)
    assert(caseStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditions.head.getText == "1 = 1")

    assert(caseStmt.elseBody.get.collection.head.isInstanceOf[SingleStatement])
    assert(caseStmt.elseBody.get.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 43")
  }

  test("searched case statement nested") {
    val sqlScriptText =
      """
        |BEGIN
        | CASE
        |   WHEN 1 = 1 THEN
        |     CASE
        |       WHEN 2 = 1 THEN
        |         SELECT 41;
        |       ELSE
        |         SELECT 42;
        |     END CASE;
        |  END CASE;
        |END
        |""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[CaseStatement])

    val caseStmt = tree.collection.head.asInstanceOf[CaseStatement]
    assert(caseStmt.conditions.length == 1)
    assert(caseStmt.conditionalBodies.length == 1)
    assert(caseStmt.elseBody.isEmpty)

    assert(caseStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditions.head.getText == "1 = 1")

    assert(caseStmt.conditionalBodies.head.collection.head.isInstanceOf[CaseStatement])
    val nestedCaseStmt =
      caseStmt.conditionalBodies.head.collection.head.asInstanceOf[CaseStatement]

    assert(nestedCaseStmt.conditions.length == 1)
    assert(nestedCaseStmt.conditionalBodies.length == 1)
    assert(nestedCaseStmt.elseBody.isDefined)

    assert(nestedCaseStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(nestedCaseStmt.conditions.head.getText == "2 = 1")

    assert(nestedCaseStmt.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(nestedCaseStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 41")

    assert(nestedCaseStmt.elseBody.get.collection.head.isInstanceOf[SingleStatement])
    assert(nestedCaseStmt.elseBody.get.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 42")
  }

  test("simple case statement") {
    val sqlScriptText =
      """
        |BEGIN
        | CASE 1
        |   WHEN 1 THEN
        |     SELECT 1;
        | END CASE;
        |END
        |""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[CaseStatement])
    val caseStmt = tree.collection.head.asInstanceOf[CaseStatement]
    assert(caseStmt.conditions.length == 1)
    assert(caseStmt.conditions.head.isInstanceOf[SingleStatement])
    checkSimpleCaseStatementCondition(caseStmt.conditions.head, _ == Literal(1), _ == Literal(1))
  }


  test("simple case statement - multi when") {
    val sqlScriptText =
      """
        |BEGIN
        | CASE 1
        |   WHEN 1 THEN
        |     SELECT 1;
        |   WHEN (SELECT 2) THEN
        |     SELECT * FROM b;
        |   WHEN 3 IN (1,2,3) THEN
        |     SELECT 42;
        | END CASE;
        |END
        |""".stripMargin
    val tree = parseScript(sqlScriptText)

    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[CaseStatement])

    val caseStmt = tree.collection.head.asInstanceOf[CaseStatement]
    assert(caseStmt.conditions.length == 3)
    assert(caseStmt.conditionalBodies.length == 3)
    assert(caseStmt.elseBody.isEmpty)

    assert(caseStmt.conditions.head.isInstanceOf[SingleStatement])
    checkSimpleCaseStatementCondition(caseStmt.conditions.head, _ == Literal(1), _ == Literal(1))

    assert(caseStmt.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 1")

    assert(caseStmt.conditions(1).isInstanceOf[SingleStatement])
    checkSimpleCaseStatementCondition(
    caseStmt.conditions(1), _ == Literal(1), _.isInstanceOf[ScalarSubquery])

    assert(caseStmt.conditionalBodies(1).collection.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditionalBodies(1).collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT * FROM b")

    assert(caseStmt.conditions(2).isInstanceOf[SingleStatement])
    checkSimpleCaseStatementCondition(
      caseStmt.conditions(2), _ == Literal(1), _.isInstanceOf[In])

    assert(caseStmt.conditionalBodies(2).collection.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditionalBodies(2).collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 42")
  }

  test("simple case statement with else") {
    val sqlScriptText =
      """
        |BEGIN
        | CASE 1
        |   WHEN 1 THEN
        |     SELECT 42;
        |   ELSE
        |     SELECT 43;
        | END CASE;
        |END
        |""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[CaseStatement])
    val caseStmt = tree.collection.head.asInstanceOf[CaseStatement]
    assert(caseStmt.elseBody.isDefined)
    assert(caseStmt.conditions.length == 1)
    assert(caseStmt.conditions.head.isInstanceOf[SingleStatement])
    checkSimpleCaseStatementCondition(caseStmt.conditions.head, _ == Literal(1), _ == Literal(1))

    assert(caseStmt.elseBody.get.collection.head.isInstanceOf[SingleStatement])
    assert(caseStmt.elseBody.get.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 43")
  }

  test("simple case statement nested") {
    val sqlScriptText =
      """
        |BEGIN
        | CASE (SELECT 1)
        |   WHEN 1 THEN
        |     CASE 2
        |       WHEN 2 THEN
        |         SELECT 41;
        |       ELSE
        |         SELECT 42;
        |     END CASE;
        |  END CASE;
        |END
        |""".stripMargin
    val tree = parseScript(sqlScriptText)
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[CaseStatement])

    val caseStmt = tree.collection.head.asInstanceOf[CaseStatement]
    assert(caseStmt.conditions.length == 1)
    assert(caseStmt.conditionalBodies.length == 1)
    assert(caseStmt.elseBody.isEmpty)

    assert(caseStmt.conditions.head.isInstanceOf[SingleStatement])
    checkSimpleCaseStatementCondition(
      caseStmt.conditions.head, _.isInstanceOf[ScalarSubquery], _ == Literal(1))

    assert(caseStmt.conditionalBodies.head.collection.head.isInstanceOf[CaseStatement])
    val nestedCaseStmt =
      caseStmt.conditionalBodies.head.collection.head.asInstanceOf[CaseStatement]

    assert(nestedCaseStmt.conditions.length == 1)
    assert(nestedCaseStmt.conditionalBodies.length == 1)
    assert(nestedCaseStmt.elseBody.isDefined)

    assert(nestedCaseStmt.conditions.head.isInstanceOf[SingleStatement])
    checkSimpleCaseStatementCondition(
      nestedCaseStmt.conditions.head, _ == Literal(2), _ == Literal(2))

    assert(nestedCaseStmt.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(nestedCaseStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 41")

    assert(nestedCaseStmt.elseBody.get.collection.head.isInstanceOf[SingleStatement])
    assert(nestedCaseStmt.elseBody.get.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 42")
  }

  // Helper methods
  def cleanupStatementString(statementStr: String): String = {
    statementStr
      .replace("\n", "")
      .replace("BEGIN", "")
      .replace("END", "")
      .trim
  }

  private def checkSimpleCaseStatementCondition(
      conditionStatement: SingleStatement,
      predicateLeft: Expression => Boolean,
      predicateRight: Expression => Boolean): Unit = {
    assert(conditionStatement.parsedPlan.isInstanceOf[Project])
    val project = conditionStatement.parsedPlan.asInstanceOf[Project]
    assert(project.projectList.head.isInstanceOf[Alias])
    assert(project.projectList.head.asInstanceOf[Alias].child.isInstanceOf[EqualTo])
    val equalTo = project.projectList.head.asInstanceOf[Alias].child.asInstanceOf[EqualTo]
    assert(predicateLeft(equalTo.left))
    assert(predicateRight(equalTo.right))
  }
}
