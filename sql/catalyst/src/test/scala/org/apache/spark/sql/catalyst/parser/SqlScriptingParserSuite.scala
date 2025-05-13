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
import org.apache.spark.sql.catalyst.expressions.{In, Literal, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.{CompoundBody, CreateVariable, ExceptionHandler, ForStatement, IfElseStatement, IterateStatement, LeaveStatement, LoopStatement, Project, RepeatStatement, SearchedCaseStatement, SetVariable, SimpleCaseStatement, SingleStatement, WhileStatement}
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.internal.SQLConf

class SqlScriptingParserSuite extends SparkFunSuite with SQLHelper {
  import CatalystSqlParser._

  // Tests setup
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    conf.setConfString(SQLConf.SQL_SCRIPTING_ENABLED.key, "true")
  }

  protected override def afterAll(): Unit = {
    conf.unsetConf(SQLConf.SQL_SCRIPTING_ENABLED.key)
    super.afterAll()
  }

  // Tests
  test("single select") {
    val sqlScriptText = "SELECT 1;"
    val statement = parsePlan(sqlScriptText)
    assert(!statement.isInstanceOf[CompoundBody])
  }

  test("multi select without ; - should fail") {
    val sqlScriptText = "SELECT 1 SELECT 1"
    val e = intercept[ParseException] {
      parsePlan(sqlScriptText)
    }
    assert(e.getCondition === "PARSE_SYNTAX_ERROR")
    assert(e.getMessage.contains("Syntax error"))
    assert(e.getMessage.contains("SELECT"))
  }

  test("multi select with ; - should fail") {
    val sqlScriptText = "SELECT 1; SELECT 1;"
    val e = intercept[ParseException] {
      parsePlan(sqlScriptText)
    }
    assert(e.getCondition === "PARSE_SYNTAX_ERROR")
    assert(e.getMessage.contains("Syntax error"))
    assert(e.getMessage.contains("SELECT"))
  }

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

  test("empty singleCompoundStatement") {
    val sqlScriptText =
      """
        |BEGIN
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.isEmpty)
  }

  test("empty beginEndCompoundBlock") {
    val sqlScriptText =
      """
        |BEGIN
        | BEGIN
        | END;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[CompoundBody])
    val innerBody = tree.collection.head.asInstanceOf[CompoundBody]
    assert(innerBody.collection.isEmpty)
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
    assert(e.getCondition === "PARSE_SYNTAX_ERROR")
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
    assert(e.getCondition === "PARSE_SYNTAX_ERROR")
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

  test("not atomic body") {
    val sqlScriptText =
      """
        |BEGIN NOT ATOMIC
        |  SELECT 1;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText)
    assert(tree.isInstanceOf[CompoundBody])
  }

  test("nested not atomic body") {
    val sqlScriptText =
      """
        |BEGIN NOT ATOMIC
        |  BEGIN NOT ATOMIC
        |    SELECT 1;
        |  END;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.head.isInstanceOf[CompoundBody])
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
      condition = "PARSE_SYNTAX_ERROR",
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
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "LABELS_MISMATCH",
      parameters = Map("beginLabel" -> toSQLId("lbl_begin"), "endLabel" -> toSQLId("lbl_end")))
    assert(exception.origin.line.contains(3))
  }

  test("compound: forbidden label - system") {
    val sqlScriptText =
      """
        |BEGIN
        |  system: BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END;
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "LABEL_NAME_FORBIDDEN",
      parameters = Map("label" -> toSQLId("system")))
    assert(exception.origin.line.contains(3))
  }

  test("compound: forbidden label - starting with sys") {
    val sqlScriptText =
      """
        |BEGIN
        |  sysXYZ: BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END;
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "LABEL_NAME_FORBIDDEN",
      parameters = Map("label" -> toSQLId("sysxyz")))
    assert(exception.origin.line.contains(3))
  }

  test("compound: forbidden label - session") {
    val sqlScriptText =
      """
        |BEGIN
        |  session: BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END;
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "LABEL_NAME_FORBIDDEN",
      parameters = Map("label" -> toSQLId("session")))
    assert(exception.origin.line.contains(3))
  }

  test("compound: forbidden label - builtin") {
    val sqlScriptText =
      """
        |BEGIN
        |  builtin: BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END;
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "LABEL_NAME_FORBIDDEN",
      parameters = Map("label" -> toSQLId("builtin")))
    assert(exception.origin.line.contains(3))
  }

  test("compound: forbidden label - system - case insensitive") {
    val sqlScriptText =
      """
        |BEGIN
        |  SySTeM: BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END;
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "LABEL_NAME_FORBIDDEN",
      parameters = Map("label" -> toSQLId("system")))
    assert(exception.origin.line.contains(3))
  }

  test("compound: forbidden label - session - case insensitive") {
    val sqlScriptText =
      """
        |BEGIN
        |  SEsSiON: BEGIN
        |    SELECT 1;
        |    SELECT 2;
        |    INSERT INTO A VALUES (a, b, 3);
        |    SELECT a, b, c FROM T;
        |    SELECT * FROM T;
        |  END;
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "LABEL_NAME_FORBIDDEN",
      parameters = Map("label" -> toSQLId("session")))
    assert(exception.origin.line.contains(3))
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
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "END_LABEL_WITHOUT_BEGIN_LABEL",
      parameters = Map("endLabel" -> toSQLId("lbl")))
    assert(exception.origin.line.contains(9))
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
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "INVALID_VARIABLE_DECLARATION.ONLY_AT_BEGINNING",
      parameters = Map("varName" -> "`testVariable`"))
    assert(exception.origin.line.contains(4))
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
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "INVALID_VARIABLE_DECLARATION.NOT_ALLOWED_IN_SCOPE",
      parameters = Map("varName" -> "`testVariable`"))
    assert(exception.origin.line.contains(4))
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
    assert(e.getCondition === "PARSE_SYNTAX_ERROR")
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

  test("if with empty body") {
    val sqlScriptText =
      """BEGIN
        | IF 1 = 1 THEN
        | END IF;
        |END
      """.stripMargin
    checkError(
      exception = intercept[ParseException] {
        parsePlan(sqlScriptText)
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'IF'", "hint" -> ""))
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

  test("if elseif") {
    val sqlScriptText =
      """BEGIN
        |IF 1 = 1 THEN
        |  SELECT 1;
        |ELSEIF 2 = 2 THEN
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

  test("if multi elseif") {
    val sqlScriptText =
      """BEGIN
        |IF 1 = 1 THEN
        |  SELECT 1;
        |ELSEIF 2 = 2 THEN
        |  SELECT 2;
        |ELSEIF 3 = 3 THEN
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

  test("if - multi elseif - else nested") {
    val sqlScriptText =
      """BEGIN
        |IF 1 = 1 THEN
        |  SELECT 1;
        |ELSEIF 2 = 2 THEN
        |  SELECT 2;
        |ELSE
        |  IF 3 = 3 THEN
        |    SELECT 3;
        |  ELSEIF 4 = 4 THEN
        |    SELECT 4;
        |  ELSE
        |    IF 5 = 5 THEN
        |      SELECT 5;
        |    ELSE
        |      SELECT 6;
        |    END IF;
        |  END IF;
        |END IF;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[IfElseStatement])

    val ifStmt = tree.collection.head.asInstanceOf[IfElseStatement]
    assert(ifStmt.conditions.length == 2)
    assert(ifStmt.conditionalBodies.length == 2)
    assert(ifStmt.elseBody.nonEmpty)

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

    assert(ifStmt.elseBody.get.collection.head.isInstanceOf[IfElseStatement])
    val nestedIf_1 = ifStmt.elseBody.get.collection.head.asInstanceOf[IfElseStatement]

    assert(nestedIf_1.conditions.length == 2)
    assert(nestedIf_1.conditionalBodies.length == 2)
    assert(nestedIf_1.elseBody.nonEmpty)


    assert(nestedIf_1.conditions.head.isInstanceOf[SingleStatement])
    assert(nestedIf_1.conditions.head.getText == "3 = 3")

    assert(nestedIf_1.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(nestedIf_1.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 3")

    assert(nestedIf_1.conditions(1).isInstanceOf[SingleStatement])
    assert(nestedIf_1.conditions(1).getText == "4 = 4")

    assert(nestedIf_1.conditionalBodies(1).collection.head.isInstanceOf[SingleStatement])
    assert(nestedIf_1.conditionalBodies(1).collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 4")

    assert(nestedIf_1.elseBody.get.collection.head.isInstanceOf[IfElseStatement])
    val nestedIf_2 = nestedIf_1.elseBody.get.collection.head.asInstanceOf[IfElseStatement]

    assert(nestedIf_2.conditions.length == 1)
    assert(nestedIf_2.conditionalBodies.length == 1)
    assert(nestedIf_2.elseBody.nonEmpty)

    assert(nestedIf_2.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 5")

    assert(nestedIf_2.elseBody.get.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 6")
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

  test("while with empty body") {
    val sqlScriptText =
      """BEGIN
        | WHILE 1 = 1 DO
        | END WHILE;
        |END
      """.stripMargin
    checkError(
      exception = intercept[ParseException] {
        parsePlan(sqlScriptText)
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'WHILE'", "hint" -> ""))
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

  test("leave compound block") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: BEGIN
        |    SELECT 1;
        |    LEAVE lbl;
        |  END;
        |END""".stripMargin
    val rootTree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(rootTree.collection.length == 1)

    val tree = rootTree.collection.head.asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
        |BEGIN
        |  lbl: BEGIN
        |    SELECT 1;
        |    ITERATE lbl;
        |  END;
        |END""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
        |BEGIN
        |  lbl: BEGIN
        |    SELECT 1;
        |    LEAVE randomlbl;
        |  END;
        |END""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
        parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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

  test("repeat with empty body") {
    val sqlScriptText =
      """BEGIN
        | REPEAT UNTIL 1 = 1
        | END REPEAT;
        |END
      """.stripMargin
    checkError(
      exception = intercept[ParseException] {
        parsePlan(sqlScriptText)
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'1'", "hint" -> ""))
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

    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SearchedCaseStatement])
    val caseStmt = tree.collection.head.asInstanceOf[SearchedCaseStatement]
    assert(caseStmt.conditions.length == 1)
    assert(caseStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditions.head.getText == "1 = 1")
  }

  test("searched case statement with empty body") {
    val sqlScriptText =
      """BEGIN
        | CASE
        |  WHEN 1 = 1 THEN
        | END CASE;
        |END
      """.stripMargin
    checkError(
      exception = intercept[ParseException] {
        parsePlan(sqlScriptText)
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'CASE'", "hint" -> ""))
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]

    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SearchedCaseStatement])

    val caseStmt = tree.collection.head.asInstanceOf[SearchedCaseStatement]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SearchedCaseStatement])
    val caseStmt = tree.collection.head.asInstanceOf[SearchedCaseStatement]
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SearchedCaseStatement])

    val caseStmt = tree.collection.head.asInstanceOf[SearchedCaseStatement]
    assert(caseStmt.conditions.length == 1)
    assert(caseStmt.conditionalBodies.length == 1)
    assert(caseStmt.elseBody.isEmpty)

    assert(caseStmt.conditions.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditions.head.getText == "1 = 1")

    assert(caseStmt.conditionalBodies.head.collection.head.isInstanceOf[SearchedCaseStatement])
    val nestedCaseStmt =
      caseStmt.conditionalBodies.head.collection.head.asInstanceOf[SearchedCaseStatement]

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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SimpleCaseStatement])
    val caseStmt = tree.collection.head.asInstanceOf[SimpleCaseStatement]
    assert(caseStmt.caseVariableExpression == Literal(1))
    assert(caseStmt.conditionExpressions.length == 1)
    assert(caseStmt.conditionExpressions.head == Literal(1))

    assert(caseStmt.conditionalBodies.length == 1)
    assert(caseStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 1")
  }

  test("simple case statement with empty body") {
    val sqlScriptText =
      """BEGIN
        | CASE 1
        |  WHEN 1 THEN
        | END CASE;
        |END
      """.stripMargin
    checkError(
      exception = intercept[ParseException] {
        parsePlan(sqlScriptText)
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'CASE'", "hint" -> ""))
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]

    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SimpleCaseStatement])

    val caseStmt = tree.collection.head.asInstanceOf[SimpleCaseStatement]
    assert(caseStmt.caseVariableExpression == Literal(1))
    assert(caseStmt.conditionExpressions.length == 3)
    assert(caseStmt.conditionalBodies.length == 3)
    assert(caseStmt.elseBody.isEmpty)

    assert(caseStmt.conditionExpressions.head == Literal(1))

    assert(caseStmt.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 1")

    assert(caseStmt.conditionExpressions(1).isInstanceOf[ScalarSubquery])

    assert(caseStmt.conditionalBodies(1).collection.head.isInstanceOf[SingleStatement])
    assert(caseStmt.conditionalBodies(1).collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT * FROM b")

    assert(caseStmt.conditionExpressions(2).isInstanceOf[In])

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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SimpleCaseStatement])
    val caseStmt = tree.collection.head.asInstanceOf[SimpleCaseStatement]

    assert(caseStmt.caseVariableExpression == Literal(1))
    assert(caseStmt.elseBody.isDefined)
    assert(caseStmt.conditionExpressions.length == 1)
    assert(caseStmt.conditionExpressions.head  == Literal(1))

    assert(caseStmt.conditionalBodies.length == 1)
    assert(caseStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 42")

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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[SimpleCaseStatement])

    val caseStmt = tree.collection.head.asInstanceOf[SimpleCaseStatement]

    assert(caseStmt.caseVariableExpression.isInstanceOf[ScalarSubquery])
    assert(caseStmt.conditionExpressions.length == 1)
    assert(caseStmt.conditionalBodies.length == 1)
    assert(caseStmt.elseBody.isEmpty)

    assert(caseStmt.conditionExpressions.head == Literal(1))

    assert(caseStmt.conditionalBodies.head.collection.head.isInstanceOf[SimpleCaseStatement])
    val nestedCaseStmt =
      caseStmt.conditionalBodies.head.collection.head.asInstanceOf[SimpleCaseStatement]

    assert(nestedCaseStmt.caseVariableExpression == Literal(2))
    assert(nestedCaseStmt.conditionExpressions.length == 1)
    assert(nestedCaseStmt.conditionalBodies.length == 1)
    assert(nestedCaseStmt.elseBody.isDefined)

    assert(nestedCaseStmt.conditionExpressions.head == Literal(2))

    assert(nestedCaseStmt.conditionalBodies.head.collection.head.isInstanceOf[SingleStatement])
    assert(nestedCaseStmt.conditionalBodies.head.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 41")

    assert(nestedCaseStmt.elseBody.get.collection.head.isInstanceOf[SingleStatement])
    assert(nestedCaseStmt.elseBody.get.collection.head.asInstanceOf[SingleStatement]
      .getText == "SELECT 42")
  }

  test("loop statement") {
    val sqlScriptText =
      """BEGIN
        |lbl: LOOP
        |  SELECT 1;
        |  SELECT 2;
        |END LOOP lbl;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[LoopStatement])

    val whileStmt = tree.collection.head.asInstanceOf[LoopStatement]

    assert(whileStmt.body.isInstanceOf[CompoundBody])
    assert(whileStmt.body.collection.length == 2)
    assert(whileStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(whileStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(whileStmt.label.contains("lbl"))
  }

  test("loop with empty body") {
    val sqlScriptText =
      """BEGIN
        | LOOP
        | END LOOP;
        |END
      """.stripMargin
    checkError(
      exception = intercept[ParseException] {
        parsePlan(sqlScriptText)
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'LOOP'", "hint" -> ""))
  }

  test("loop with if else block") {
    val sqlScriptText =
      """BEGIN
        |lbl: LOOP
        | IF 1 = 1 THEN
        |   SELECT 1;
        | ELSE
        |   SELECT 2;
        | END IF;
        |END LOOP lbl;
        |END
      """.stripMargin

    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[LoopStatement])

    val loopStmt = tree.collection.head.asInstanceOf[LoopStatement]

    assert(loopStmt.body.isInstanceOf[CompoundBody])
    assert(loopStmt.body.collection.length == 1)
    assert(loopStmt.body.collection.head.isInstanceOf[IfElseStatement])
    val ifStmt = loopStmt.body.collection.head.asInstanceOf[IfElseStatement]

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

    assert(loopStmt.label.contains("lbl"))
  }

  test("nested loop") {
    val sqlScriptText =
      """BEGIN
        |lbl: LOOP
        |  LOOP
        |    SELECT 42;
        |  END LOOP;
        |END LOOP lbl;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[LoopStatement])

    val loopStmt = tree.collection.head.asInstanceOf[LoopStatement]

    assert(loopStmt.body.isInstanceOf[CompoundBody])
    assert(loopStmt.body.collection.length == 1)
    assert(loopStmt.body.collection.head.isInstanceOf[LoopStatement])
    val nestedLoopStmt = loopStmt.body.collection.head.asInstanceOf[LoopStatement]

    assert(nestedLoopStmt.body.isInstanceOf[CompoundBody])
    assert(nestedLoopStmt.body.collection.length == 1)
    assert(nestedLoopStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(nestedLoopStmt.body.collection.
      head.asInstanceOf[SingleStatement].getText == "SELECT 42")

    assert(loopStmt.label.contains("lbl"))
  }

  test("leave loop statement") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: LOOP
        |    SELECT 1;
        |    LEAVE lbl;
        |  END LOOP;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[LoopStatement])

    val loopStmt = tree.collection.head.asInstanceOf[LoopStatement]

    assert(loopStmt.body.isInstanceOf[CompoundBody])
    assert(loopStmt.body.collection.length == 2)

    assert(loopStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(loopStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(loopStmt.body.collection(1).isInstanceOf[LeaveStatement])
    assert(loopStmt.body.collection(1).asInstanceOf[LeaveStatement].label == "lbl")
  }

  test("iterate loop statement") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: LOOP
        |    SELECT 1;
        |    ITERATE lbl;
        |  END LOOP;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[LoopStatement])

    val loopStmt = tree.collection.head.asInstanceOf[LoopStatement]

    assert(loopStmt.body.isInstanceOf[CompoundBody])
    assert(loopStmt.body.collection.length == 2)

    assert(loopStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(loopStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(loopStmt.body.collection(1).isInstanceOf[IterateStatement])
    assert(loopStmt.body.collection(1).asInstanceOf[IterateStatement].label == "lbl")
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
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[LoopStatement])

    val loopStmt = tree.collection.head.asInstanceOf[LoopStatement]

    assert(loopStmt.body.isInstanceOf[CompoundBody])
    assert(loopStmt.body.collection.length == 1)

    val nestedLoopStmt = loopStmt.body.collection.head.asInstanceOf[LoopStatement]

    assert(nestedLoopStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(
      nestedLoopStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(nestedLoopStmt.body.collection(1).isInstanceOf[LeaveStatement])
    assert(nestedLoopStmt.body.collection(1).asInstanceOf[LeaveStatement].label == "lbl")
  }

  test("iterate outer loop from nested loop statement") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: LOOP
        |    lbl2: LOOP
        |      SELECT 1;
        |      ITERATE lbl;
        |    END LOOP;
        |  END LOOP;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[LoopStatement])

    val loopStmt = tree.collection.head.asInstanceOf[LoopStatement]

    assert(loopStmt.body.isInstanceOf[CompoundBody])
    assert(loopStmt.body.collection.length == 1)

    val nestedLoopStmt = loopStmt.body.collection.head.asInstanceOf[LoopStatement]

    assert(nestedLoopStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(
      nestedLoopStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(nestedLoopStmt.body.collection(1).isInstanceOf[IterateStatement])
    assert(nestedLoopStmt.body.collection(1).asInstanceOf[IterateStatement].label == "lbl")
  }

  test("unique label names: nested begin-end blocks") {
    val sqlScriptText =
      """BEGIN
        |lbl: BEGIN
        |  lbl: BEGIN
        |    SELECT 1;
        |  END;
        |END;
        |END
      """.stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    }
    checkError(
      exception = exception,
      condition = "LABEL_ALREADY_EXISTS",
      parameters = Map("label" -> toSQLId("lbl")))
  }

  test("unique label names: nested begin-end blocks with same prefix") {
    val sqlScriptText =
      """BEGIN
        |lbl_1: BEGIN
        |  lbl_11: BEGIN
        |    SELECT 1;
        |  END;
        |END;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[CompoundBody])
    val body_1 = tree.collection.head.asInstanceOf[CompoundBody]
    assert(body_1.label.get == "lbl_1")
    assert(body_1.collection.length == 1)
    assert(body_1.collection.head.isInstanceOf[CompoundBody])
    val body_11 = body_1.collection.head.asInstanceOf[CompoundBody]
    assert(body_11.label.get == "lbl_11")
  }

  test("unique label names: multi-level nested begin-end blocks") {
    val sqlScriptText =
      """BEGIN
        |lbl_1: BEGIN
        |  lbl_2: BEGIN
        |    lbl_1: BEGIN
        |      SELECT 1;
        |    END;
        |  END;
        |END;
        |END
      """.stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    }
    checkError(
      exception = exception,
      condition = "LABEL_ALREADY_EXISTS",
      parameters = Map("label" -> toSQLId("lbl_1")))
  }

  test("unique label names: while loop in begin-end block") {
    val sqlScriptText =
      """BEGIN
        |lbl: BEGIN
        |  lbl: WHILE 1=1 DO
        |    SELECT 1;
        |  END WHILE;
        |END;
        |END
      """.stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    }
    checkError(
      exception = exception,
      condition = "LABEL_ALREADY_EXISTS",
      parameters = Map("label" -> toSQLId("lbl")))
  }

  test("unique label names: begin-end block in while loop") {
    val sqlScriptText =
      """BEGIN
        |lbl: WHILE 1=1 DO
        |  lbl: BEGIN
        |    SELECT 1;
        |  END;
        |END WHILE;
        |END
      """.stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    }
    checkError(
      exception = exception,
      condition = "LABEL_ALREADY_EXISTS",
      parameters = Map("label" -> toSQLId("lbl")))
  }

  test("unique label names: nested while loops") {
    val sqlScriptText =
      """BEGIN
        |w_loop: WHILE 1=1 DO
        |  w_loop: WHILE 2=2 DO
        |    SELECT 1;
        |  END WHILE;
        |END WHILE;
        |END
      """.stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    }
    checkError(
      exception = exception,
      condition = "LABEL_ALREADY_EXISTS",
      parameters = Map("label" -> toSQLId("w_loop")))
  }

  test("unique label names: nested repeat loops") {
    val sqlScriptText =
      """BEGIN
        |r_loop: REPEAT
        |  r_loop: REPEAT
        |    SELECT 1;
        |  UNTIL 1 = 1
        |  END REPEAT;
        |UNTIL 1 = 1
        |END REPEAT;
        |END
      """.stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    }
    checkError(
      exception = exception,
      condition = "LABEL_ALREADY_EXISTS",
      parameters = Map("label" -> toSQLId("r_loop")))
  }

  test("unique label names: nested loop loops") {
    val sqlScriptText =
      """BEGIN
        |l_loop: LOOP
        |  l_loop: LOOP
        |    SELECT 1;
        |  END LOOP;
        |END LOOP;
        |END
      """.stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    }
    checkError(
      exception = exception,
      condition = "LABEL_ALREADY_EXISTS",
      parameters = Map("label" -> toSQLId("l_loop")))
  }

  test("unique label names: nested for loops") {
    val sqlScriptText =
      """BEGIN
        |f_loop: FOR x AS SELECT 1 DO
        |  f_loop: FOR y AS SELECT 2 DO
        |    SELECT 1;
        |  END FOR;
        |END FOR;
        |END
      """.stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    }
    checkError(
      exception = exception,
      condition = "LABEL_ALREADY_EXISTS",
      parameters = Map("label" -> toSQLId("f_loop")))
  }

  test("unique label names: begin-end block on the same level") {
    val sqlScriptText =
      """BEGIN
        |lbl: BEGIN
        |  SELECT 1;
        |END;
        |lbl: BEGIN
        |  SELECT 2;
        |END;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 2)
    assert(tree.collection.head.isInstanceOf[CompoundBody])
    assert(tree.collection.head.asInstanceOf[CompoundBody].label.get == "lbl")
    assert(tree.collection(1).isInstanceOf[CompoundBody])
    assert(tree.collection(1).asInstanceOf[CompoundBody].label.get == "lbl")
  }

  test("unique label names: begin-end block and loops on the same level") {
    val sqlScriptText =
      """BEGIN
        |lbl: BEGIN
        |  SELECT 1;
        |END;
        |lbl: WHILE 1=1 DO
        |  SELECT 2;
        |END WHILE;
        |lbl: LOOP
        |  SELECT 3;
        |END LOOP;
        |lbl: REPEAT
        |  SELECT 4;
        |UNTIL 1=1
        |END REPEAT;
        |lbl: FOR x AS SELECT 1 DO
        |  SELECT 5;
        |END FOR;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 5)
    assert(tree.collection.head.isInstanceOf[CompoundBody])
    assert(tree.collection.head.asInstanceOf[CompoundBody].label.get == "lbl")
    assert(tree.collection(1).isInstanceOf[WhileStatement])
    assert(tree.collection(1).asInstanceOf[WhileStatement].label.get == "lbl")
    assert(tree.collection(2).isInstanceOf[LoopStatement])
    assert(tree.collection(2).asInstanceOf[LoopStatement].label.get == "lbl")
    assert(tree.collection(3).isInstanceOf[RepeatStatement])
    assert(tree.collection(3).asInstanceOf[RepeatStatement].label.get == "lbl")
    assert(tree.collection(4).isInstanceOf[ForStatement])
    assert(tree.collection(4).asInstanceOf[ForStatement].label.get == "lbl")
  }

  test("qualified label name: label cannot be qualified") {
    val sqlScriptText =
      """
        |BEGIN
        |  part1.part2: BEGIN
        |  END;
        |END""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScriptText)
      },
      condition = "INVALID_LABEL_USAGE.QUALIFIED_LABEL_NAME",
      parameters = Map("labelName" -> "PART1.PART2"))
  }

  test("qualified label name: label cannot be qualified + end label") {
    val sqlScriptText =
      """
        |BEGIN
        |  part1.part2: BEGIN
        |  END part1.part2;
        |END""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScriptText)
      },
      condition = "INVALID_LABEL_USAGE.QUALIFIED_LABEL_NAME",
      parameters = Map("labelName" -> "PART1.PART2"))
  }

  test("unique label names: nested labeled scope statements") {
    val sqlScriptText =
      """BEGIN
        |lbl_0: BEGIN
        |  lbl_1: WHILE 1=1 DO
        |    lbl_2: LOOP
        |      lbl_3: REPEAT
        |        lbl_4: FOR x AS SELECT 1 DO
        |          SELECT 4;
        |        END FOR;
        |      UNTIL 1=1
        |      END REPEAT;
        |    END LOOP;
        |  END WHILE;
        |END;
        |END
      """.stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[CompoundBody])
    // Compound body
    val body = tree.collection.head.asInstanceOf[CompoundBody]
    assert(body.label.get == "lbl_0")
    assert(body.collection.head.isInstanceOf[WhileStatement])
    // While statement
    val whileStatement = body.collection.head.asInstanceOf[WhileStatement]
    assert(whileStatement.label.get == "lbl_1")
    assert(whileStatement.body.collection.head.isInstanceOf[LoopStatement])
    // Loop statement
    val loopStatement = whileStatement.body.collection.head.asInstanceOf[LoopStatement]
    assert(loopStatement.label.get == "lbl_2")
    assert(loopStatement.body.collection.head.isInstanceOf[RepeatStatement])
    // Repeat statement
    val repeatStatement = loopStatement.body.collection.head.asInstanceOf[RepeatStatement]
    assert(repeatStatement.label.get == "lbl_3")
    // For statement
    val forStatement = repeatStatement.body.collection.head.asInstanceOf[ForStatement]
    assert(forStatement.label.get == "lbl_4")
  }

  test("for statement") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: FOR x AS SELECT 5 DO
        |    SELECT 1;
        |  END FOR;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[ForStatement])

    val forStmt = tree.collection.head.asInstanceOf[ForStatement]
    assert(forStmt.query.isInstanceOf[SingleStatement])
    assert(forStmt.query.getText == "SELECT 5")
    assert(forStmt.variableName.contains("x"))

    assert(forStmt.body.isInstanceOf[CompoundBody])
    assert(forStmt.body.collection.length == 1)
    assert(forStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(forStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(forStmt.label.contains("lbl"))
  }

  test("for statement - empty body") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: FOR x AS SELECT 5 DO
        |  END FOR;
        |END""".stripMargin
    checkError(
      exception = intercept[ParseException] {
        parsePlan(sqlScriptText)
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'FOR'", "hint" -> ""))
  }

  test("for statement - no label") {
    val sqlScriptText =
      """
        |BEGIN
        |  FOR x AS SELECT 5 DO
        |    SELECT 1;
        |  END FOR;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[ForStatement])

    val forStmt = tree.collection.head.asInstanceOf[ForStatement]
    assert(forStmt.query.isInstanceOf[SingleStatement])
    assert(forStmt.query.getText == "SELECT 5")
    assert(forStmt.variableName.contains("x"))

    assert(forStmt.body.isInstanceOf[CompoundBody])
    assert(forStmt.body.collection.length == 1)
    assert(forStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(forStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    // when not explicitly set, label is random UUID
    assert(forStmt.label.isDefined)
  }

  test("for statement - with complex subquery") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: FOR x AS SELECT c1, c2 FROM t WHERE c2 = 5 GROUP BY c1 ORDER BY c1 DO
        |    SELECT x.c1;
        |    SELECT x.c2;
        |  END FOR;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[ForStatement])

    val forStmt = tree.collection.head.asInstanceOf[ForStatement]
    assert(forStmt.query.isInstanceOf[SingleStatement])
    assert(forStmt.query.getText == "SELECT c1, c2 FROM t WHERE c2 = 5 GROUP BY c1 ORDER BY c1")
    assert(forStmt.variableName.contains("x"))

    assert(forStmt.body.isInstanceOf[CompoundBody])
    assert(forStmt.body.collection.length == 2)
    assert(forStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(forStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT x.c1")
    assert(forStmt.body.collection(1).isInstanceOf[SingleStatement])
    assert(forStmt.body.collection(1).asInstanceOf[SingleStatement].getText == "SELECT x.c2")

    assert(forStmt.label.contains("lbl"))
  }

  test("for statement - nested") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl1: FOR i AS SELECT 1 DO
        |    lbl2: FOR j AS SELECT 2 DO
        |      SELECT i + j;
        |    END FOR lbl2;
        |  END FOR lbl1;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[ForStatement])

    val forStmt = tree.collection.head.asInstanceOf[ForStatement]
    assert(forStmt.query.isInstanceOf[SingleStatement])
    assert(forStmt.query.getText == "SELECT 1")
    assert(forStmt.variableName.contains("i"))
    assert(forStmt.label.contains("lbl1"))

    assert(forStmt.body.isInstanceOf[CompoundBody])
    assert(forStmt.body.collection.length == 1)
    assert(forStmt.body.collection.head.isInstanceOf[ForStatement])
    val nestedForStmt = forStmt.body.collection.head.asInstanceOf[ForStatement]

    assert(nestedForStmt.query.isInstanceOf[SingleStatement])
    assert(nestedForStmt.query.getText == "SELECT 2")
    assert(nestedForStmt.variableName.contains("j"))
    assert(nestedForStmt.label.contains("lbl2"))

    assert(nestedForStmt.body.isInstanceOf[CompoundBody])
    assert(nestedForStmt.body.collection.length == 1)
    assert(nestedForStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(nestedForStmt.body.collection.
      head.asInstanceOf[SingleStatement].getText == "SELECT i + j")
  }

  test("for statement - no variable") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: FOR SELECT 5 DO
        |    SELECT 1;
        |  END FOR;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[ForStatement])

    val forStmt = tree.collection.head.asInstanceOf[ForStatement]
    assert(forStmt.query.isInstanceOf[SingleStatement])
    assert(forStmt.query.getText == "SELECT 5")
    assert(forStmt.variableName.isEmpty)

    assert(forStmt.body.isInstanceOf[CompoundBody])
    assert(forStmt.body.collection.length == 1)
    assert(forStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(forStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    assert(forStmt.label.contains("lbl"))
  }

  test("for statement - no variable - empty body") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: FOR SELECT 5 DO
        |  END FOR;
        |END""".stripMargin
    checkError(
      exception = intercept[ParseException] {
        parsePlan(sqlScriptText)
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'FOR'", "hint" -> ""))
  }

  test("for statement - no variable - no label") {
    val sqlScriptText =
      """
        |BEGIN
        |  FOR SELECT 5 DO
        |    SELECT 1;
        |  END FOR;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[ForStatement])

    val forStmt = tree.collection.head.asInstanceOf[ForStatement]
    assert(forStmt.query.isInstanceOf[SingleStatement])
    assert(forStmt.query.getText == "SELECT 5")
    assert(forStmt.variableName.isEmpty)

    assert(forStmt.body.isInstanceOf[CompoundBody])
    assert(forStmt.body.collection.length == 1)
    assert(forStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(forStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")

    // when not explicitly set, label is random UUID
    assert(forStmt.label.isDefined)
  }

  test("for statement - no variable - with complex subquery") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl: FOR SELECT c1, c2 FROM t WHERE c2 = 5 GROUP BY c1 ORDER BY c1 DO
        |    SELECT 1;
        |    SELECT 2;
        |  END FOR;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[ForStatement])

    val forStmt = tree.collection.head.asInstanceOf[ForStatement]
    assert(forStmt.query.isInstanceOf[SingleStatement])
    assert(forStmt.query.getText == "SELECT c1, c2 FROM t WHERE c2 = 5 GROUP BY c1 ORDER BY c1")
    assert(forStmt.variableName.isEmpty)

    assert(forStmt.body.isInstanceOf[CompoundBody])
    assert(forStmt.body.collection.length == 2)
    assert(forStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(forStmt.body.collection.head.asInstanceOf[SingleStatement].getText == "SELECT 1")
    assert(forStmt.body.collection(1).isInstanceOf[SingleStatement])
    assert(forStmt.body.collection(1).asInstanceOf[SingleStatement].getText == "SELECT 2")

    assert(forStmt.label.contains("lbl"))
  }

  test("for statement - no variable - nested") {
    val sqlScriptText =
      """
        |BEGIN
        |  lbl1: FOR SELECT 1 DO
        |    lbl2: FOR SELECT 2 DO
        |      SELECT 3;
        |    END FOR lbl2;
        |  END FOR lbl1;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.collection.length == 1)
    assert(tree.collection.head.isInstanceOf[ForStatement])

    val forStmt = tree.collection.head.asInstanceOf[ForStatement]
    assert(forStmt.query.isInstanceOf[SingleStatement])
    assert(forStmt.query.getText == "SELECT 1")
    assert(forStmt.variableName.isEmpty)
    assert(forStmt.label.contains("lbl1"))

    assert(forStmt.body.isInstanceOf[CompoundBody])
    assert(forStmt.body.collection.length == 1)
    assert(forStmt.body.collection.head.isInstanceOf[ForStatement])
    val nestedForStmt = forStmt.body.collection.head.asInstanceOf[ForStatement]

    assert(nestedForStmt.query.isInstanceOf[SingleStatement])
    assert(nestedForStmt.query.getText == "SELECT 2")
    assert(nestedForStmt.variableName.isEmpty)
    assert(nestedForStmt.label.contains("lbl2"))

    assert(nestedForStmt.body.isInstanceOf[CompoundBody])
    assert(nestedForStmt.body.collection.length == 1)
    assert(nestedForStmt.body.collection.head.isInstanceOf[SingleStatement])
    assert(nestedForStmt.body.collection.
      head.asInstanceOf[SingleStatement].getText == "SELECT 3")
  }

  test("declare condition: custom sqlstate") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE test CONDITION FOR SQLSTATE '12000';
        |  SELECT 1;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.conditions.size == 1)
    assert(tree.conditions("TEST").equals("12000"))
  }

  test("declare condition: default sqlstate") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE test CONDITION;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.conditions.size == 1)
    assert(tree.conditions("TEST").equals("45000")) // Default SQLSTATE
  }

  test("declare condition in wrong place") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;
        |  DECLARE test_condition CONDITION FOR SQLSTATE '12345';
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "INVALID_ERROR_CONDITION_DECLARATION.ONLY_AT_BEGINNING",
      parameters = Map("conditionName" -> "`TEST_CONDITION`"))
    assert(exception.origin.line.contains(2))
  }

  test("declare qualified condition") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE TEST.CONDITION CONDITION FOR SQLSTATE '12345';
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "INVALID_ERROR_CONDITION_DECLARATION.QUALIFIED_CONDITION_NAME",
      parameters = Map("conditionName" -> "TEST.CONDITION"))
    assert(exception.origin.line.contains(3))
  }

  test("declare condition with special characters") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE `test-condition` CONDITION FOR SQLSTATE '12345';
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "INVALID_ERROR_CONDITION_DECLARATION.SPECIAL_CHARACTER_FOUND",
      parameters = Map("conditionName" -> toSQLId("test-condition")))
    assert(exception.origin.line.contains(3))
  }

  test("continue handler not supported") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  DECLARE CONTINUE HANDLER FOR SQLSTATE '22012'
        |  BEGIN
        |    SET flag = 1;
        |  END;
        |  SELECT 1/0;
        |  SELECT flag;
        |END
        |""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScript)
      },
      condition = "UNSUPPORTED_FEATURE.CONTINUE_EXCEPTION_HANDLER",
      parameters = Map.empty)
  }

  test("declare handler for qualified condition name that is not supported") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  DECLARE EXIT HANDLER FOR qualified.condition.name
        |  BEGIN
        |    SET flag = 1;
        |  END;
        |  SELECT 1/0;
        |  SELECT flag;
        |END
        |""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScript)
      },
      condition = "INVALID_HANDLER_DECLARATION.CONDITION_NOT_FOUND",
      parameters = Map("condition" -> "QUALIFIED.CONDITION.NAME"))
  }

  test("declare handler for undefined condition") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE EXIT HANDLER FOR undefined_condition BEGIN SELECT 1; END;
        |  SELECT 1;
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "INVALID_HANDLER_DECLARATION.CONDITION_NOT_FOUND",
      parameters = Map("condition" -> "UNDEFINED_CONDITION"))
    assert(exception.origin.line.contains(2))
  }

  test("declare handler in wrong place") {
    val sqlScriptText =
      """
        |BEGIN
        |  SELECT 1;
        |  DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO BEGIN SELECT 1; END;
        |END""".stripMargin
    val exception = intercept[SqlScriptingException] {
      parsePlan(sqlScriptText)
    }
    checkError(
      exception = exception,
      condition = "INVALID_HANDLER_DECLARATION.WRONG_PLACE_OF_DECLARATION",
      parameters = Map.empty)
    assert(exception.origin.line.contains(2))
  }

  test("duplicate condition in handler declaration") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  DECLARE DUPLICATE_CONDITION CONDITION FOR SQLSTATE '12345';
        |  DECLARE EXIT HANDLER FOR duplicate_condition, duplicate_condition
        |  BEGIN
        |    SET flag = 1;
        |  END;
        |  SELECT 1/0;
        |  SELECT flag;
        |END
        |""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScript)
      },
      condition = "INVALID_HANDLER_DECLARATION.DUPLICATE_CONDITION_IN_HANDLER_DECLARATION",
      parameters = Map("condition" -> "DUPLICATE_CONDITION"))
  }

  test("duplicate sqlState in handler declaration") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  DECLARE EXIT HANDLER FOR SQLSTATE '12345', SQLSTATE '12345'
        |  BEGIN
        |    SET flag = 1;
        |  END;
        |  SELECT 1/0;
        |  SELECT flag;
        |END
        |""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScript)
      },
      condition = "INVALID_HANDLER_DECLARATION.DUPLICATE_SQLSTATE_IN_HANDLER_DECLARATION",
      parameters = Map("sqlState" -> "12345"))
  }

  test("invalid condition combination in handler declaration") {
    val sqlScript =
      """
        |BEGIN
        |  DECLARE OR REPLACE flag INT = -1;
        |  DECLARE EXIT HANDLER FOR SQLEXCEPTION, SQLSTATE '12345'
        |  BEGIN
        |    SET flag = 1;
        |  END;
        |  SELECT 1/0;
        |  SELECT flag;
        |END
        |""".stripMargin
    checkError(
      exception = intercept[SqlScriptingException] {
        parsePlan(sqlScript)
      },
      condition = "INVALID_HANDLER_DECLARATION.INVALID_CONDITION_COMBINATION",
      parameters = Map.empty)
  }

  test("declare handler with compound body") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO BEGIN SELECT 1; END;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.handlers.length == 1)
    assert(tree.handlers.head.isInstanceOf[ExceptionHandler])
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.size == 1)
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.contains("DIVIDE_BY_ZERO"))
    assert(tree.handlers.head.body.collection.size == 1)
    assert(tree.handlers.head.body.collection.head.isInstanceOf[SingleStatement])
    assert(tree.handlers.head.body.collection.head.asInstanceOf[SingleStatement]
      .parsedPlan.isInstanceOf[Project])
  }

  // This test works because END is not keyword here but a part of the statement.
  // It represents the name of the column in returned dataframe.
  test("declare handler single statement with END") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO SELECT 1 END;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.handlers.length == 1)
    assert(tree.handlers.head.isInstanceOf[ExceptionHandler])
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.size == 1)
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.contains("DIVIDE_BY_ZERO"))
    assert(tree.handlers.head.body.collection.size == 1)
    assert(tree.handlers.head.body.collection.head.isInstanceOf[SingleStatement])
    assert(tree.handlers.head.body.collection.head.asInstanceOf[SingleStatement]
      .parsedPlan.isInstanceOf[Project])
  }

  test("declare handler single statement") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO SELECT 1;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.handlers.length == 1)
    assert(tree.handlers.head.isInstanceOf[ExceptionHandler])
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.size == 1)
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.contains("DIVIDE_BY_ZERO"))
    assert(tree.handlers.head.body.collection.size == 1)
    assert(tree.handlers.head.body.collection.head.isInstanceOf[SingleStatement])
    assert(tree.handlers.head.body.collection.head.asInstanceOf[SingleStatement]
      .parsedPlan.isInstanceOf[Project])
  }

  test("declare handler set statement") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO SET test_var = 1;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.handlers.length == 1)
    assert(tree.handlers.head.isInstanceOf[ExceptionHandler])
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.size == 1)
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.contains("DIVIDE_BY_ZERO"))
    assert(tree.handlers.head.body.collection.size == 1)
    assert(tree.handlers.head.body.collection.head.isInstanceOf[SingleStatement])
    assert(tree.handlers.head.body.collection.head.asInstanceOf[SingleStatement]
      .parsedPlan.isInstanceOf[SetVariable])
  }

  test("declare handler with multiple conditions/sqlstates") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE TEST_CONDITION_1 CONDITION FOR SQLSTATE '12345';
        |  DECLARE TEST_CONDITION_2 CONDITION FOR SQLSTATE '54321';
        |  DECLARE EXIT HANDLER FOR SQLSTATE '22012', TEST_CONDITION_1, test_condition_2
        |    SET test_var = 1;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.handlers.length == 1)
    assert(tree.handlers.head.isInstanceOf[ExceptionHandler])
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.size == 2)
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.contains("TEST_CONDITION_1"))
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.contains("TEST_CONDITION_2"))
    assert(tree.handlers.head.exceptionHandlerTriggers.sqlStates.size == 1)
    assert(tree.handlers.head.exceptionHandlerTriggers.sqlStates.contains("22012"))
    assert(tree.handlers.head.body.collection.size == 1)
    assert(tree.handlers.head.body.collection.head.isInstanceOf[SingleStatement])
    assert(tree.handlers.head.body.collection.head.asInstanceOf[SingleStatement]
      .parsedPlan.isInstanceOf[SetVariable])
  }

  test("declare handler for SQLEXCEPTION") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE EXIT HANDLER FOR SQLEXCEPTION SET test_var = 1;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.handlers.length == 1)
    assert(tree.handlers.head.isInstanceOf[ExceptionHandler])
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.isEmpty)
    assert(tree.handlers.head.exceptionHandlerTriggers.sqlStates.isEmpty)
    assert(tree.handlers.head.exceptionHandlerTriggers.sqlException) // true
    assert(!tree.handlers.head.exceptionHandlerTriggers.notFound) // false
    assert(tree.handlers.head.body.collection.size == 1)
  }

  test("declare handler for NOT FOUND") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE EXIT HANDLER FOR NOT FOUND SET test_var = 1;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.handlers.length == 1)
    assert(tree.handlers.head.isInstanceOf[ExceptionHandler])
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.isEmpty)
    assert(tree.handlers.head.exceptionHandlerTriggers.sqlStates.isEmpty)
    assert(!tree.handlers.head.exceptionHandlerTriggers.sqlException) // true
    assert(tree.handlers.head.exceptionHandlerTriggers.notFound) // false
    assert(tree.handlers.head.body.collection.size == 1)
  }

  test("declare handler with condition and sqlstate with same value") {
    val sqlScriptText =
      """
        |BEGIN
        |  DECLARE K2000 CONDITION FOR SQLSTATE '12345';
        |  DECLARE EXIT HANDLER FOR K2000, SQLSTATE VALUE 'K2000' SET test_var = 1;
        |END""".stripMargin
    val tree = parsePlan(sqlScriptText).asInstanceOf[CompoundBody]
    assert(tree.handlers.length == 1)
    assert(tree.handlers.head.isInstanceOf[ExceptionHandler])
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.size == 1)
    assert(tree.handlers.head.exceptionHandlerTriggers.conditions.contains("K2000"))
    assert(tree.handlers.head.exceptionHandlerTriggers.sqlStates.size == 1)
    assert(tree.handlers.head.exceptionHandlerTriggers.sqlStates.contains("K2000"))
    assert(!tree.handlers.head.exceptionHandlerTriggers.sqlException) // true
    assert(!tree.handlers.head.exceptionHandlerTriggers.notFound) // false
    assert(tree.handlers.head.body.collection.size == 1)
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
