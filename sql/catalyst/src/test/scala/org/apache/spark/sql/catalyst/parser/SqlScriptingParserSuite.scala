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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.plans.SQLHelper

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
    assert(e.getMessage.contains("SELECT 1 SELECT 1"))
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
    val e = intercept[SparkException] {
      parseScript(sqlScriptText)
    }
    assert(e.getErrorClass === "INTERNAL_ERROR")
    assert(e.getMessage.contains("Both labels should be same."))
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
    val e = intercept[SparkException] {
      parseScript(sqlScriptText)
    }
    assert(e.getErrorClass === "INTERNAL_ERROR")
    assert(e.getMessage.contains("End label can't exist without begin label."))
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

  // Helper methods
  def cleanupStatementString(statementStr: String): String = {
    statementStr
      .replace("\n", "")
      .replace("BEGIN", "")
      .replace("END", "")
      .trim
  }
}
