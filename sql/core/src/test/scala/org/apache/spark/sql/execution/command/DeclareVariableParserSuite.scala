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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.EvaluateUnresolvedInlineTable
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedAttribute, UnresolvedFunction, UnresolvedIdentifier, UnresolvedInlineTable}
import org.apache.spark.sql.catalyst.expressions.{Add, Cast, Divide, Literal, ScalarSubquery}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{CreateVariable, DefaultValueExpression, Project, SubqueryAlias}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{Decimal, DecimalType, DoubleType, IntegerType, MapType, NullType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class DeclareVariableParserSuite extends AnalysisTest with SharedSparkSession {

  test("declare variable") {
    comparePlans(
      parsePlan("DECLARE var1 INT = 1"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Cast(Literal(1, IntegerType), IntegerType), "1"),
        replace = false))
    comparePlans(
      parsePlan("DECLARE var1 INT"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Literal(null, IntegerType), "null"),
        replace = false))
    comparePlans(
      parsePlan("DECLARE var1 = 1"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Literal(1, IntegerType), "1"),
        replace = false))
    comparePlans(
      parsePlan("DECLARE VARIABLE var1 = 1"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Literal(1, IntegerType), "1"),
        replace = false))
    comparePlans(
      parsePlan("DECLARE VAR var1 = 1"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Literal(1, IntegerType), "1"),
        replace = false))
    comparePlans(
      parsePlan("DECLARE VARIABLE var1 DEFAULT 1"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Literal(1, IntegerType), "1"),
        replace = false))
    comparePlans(
      parsePlan("DECLARE VARIABLE var1 INT DEFAULT 1"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Cast(Literal(1, IntegerType), IntegerType), "1"),
        replace = false))
    comparePlans(
      parsePlan("DECLARE VARIABLE system.session.var1 DEFAULT 1"),
      CreateVariable(
        UnresolvedIdentifier(Seq("system", "session", "var1")),
        DefaultValueExpression(Literal(1, IntegerType), "1"),
        replace = false))
    comparePlans(
      parsePlan("DECLARE VARIABLE session.var1 DEFAULT 1"),
      CreateVariable(
        UnresolvedIdentifier(Seq("session", "var1")),
        DefaultValueExpression(Literal(1, IntegerType), "1"),
        replace = false))
    comparePlans(
      parsePlan("DECLARE VARIABLE var1 STRING DEFAULT CURRENT_DATABASE()"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(
          Cast(UnresolvedFunction("CURRENT_DATABASE", Nil, isDistinct = false), StringType),
          "CURRENT_DATABASE()"),
        replace = false))
    val subqueryAliasChild =
      if (conf.getConf(SQLConf.EAGER_EVAL_OF_UNRESOLVED_INLINE_TABLE_ENABLED)) {
        EvaluateUnresolvedInlineTable.evaluate(
          UnresolvedInlineTable(Seq("c1"), Seq(Literal(1)) :: Nil))
      } else {
        UnresolvedInlineTable(Seq("c1"), Seq(Literal(1)) :: Nil)
      }
    comparePlans(
      parsePlan("DECLARE VARIABLE var1 INT DEFAULT (SELECT c1 FROM VALUES(1) AS T(c1))"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(
          Cast(ScalarSubquery(
            Project(UnresolvedAttribute("c1") :: Nil,
              SubqueryAlias(Seq("T"),
                subqueryAliasChild))), IntegerType),
          "(SELECT c1 FROM VALUES(1) AS T(c1))"),
        replace = false))
  }

  test("declare or replace variable") {
    comparePlans(
      parsePlan("DECLARE OR REPLACE VARIABLE var1 = 1"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Literal(1, IntegerType), "1"),
        replace = true))
    comparePlans(
      parsePlan("DECLARE OR REPLACE VARIABLE var1 DOUBLE DEFAULT 1 + RAND(5)"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(
          Cast(
            Add(Literal(1, IntegerType),
              UnresolvedFunction("RAND", Seq(Literal(5, IntegerType)), isDistinct = false)),
            DoubleType),
          "1 + RAND(5)"),
        replace = true))
    comparePlans(
      parsePlan("DECLARE OR REPLACE VARIABLE var1 DEFAULT NULL"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Literal(null, NullType), "NULL"),
        replace = true))
    comparePlans(
      parsePlan("DECLARE OR REPLACE VARIABLE INT DEFAULT 5.0"),
      CreateVariable(
        UnresolvedIdentifier(Seq("INT")),
        DefaultValueExpression(Literal(Decimal("5.0"), DecimalType(2, 1)), "5.0"),
        replace = true))
    comparePlans(
      parsePlan("DECLARE OR REPLACE VARIABLE var1 MAP<string, double> " +
        "DEFAULT MAP('Hello', 5.1, 'World', -7.1E10)"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Cast(
          UnresolvedFunction("MAP", Seq(
            Literal(UTF8String.fromString("Hello"), StringType),
            Literal(Decimal("5.1"), DecimalType(2, 1)),
            Literal(UTF8String.fromString("World"), StringType),
            Literal(-7.1E10, DoubleType)), isDistinct = false),
          MapType(StringType, DoubleType)),
          "MAP('Hello', 5.1, 'World', -7.1E10)"),
        replace = true))
    comparePlans(
      parsePlan("DECLARE OR REPLACE VARIABLE var1 INT DEFAULT NULL"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Cast(Literal(null, NullType), IntegerType), "NULL"),
        replace = true))
    comparePlans(
      parsePlan("DECLARE OR REPLACE VARIABLE var1 INT DEFAULT 1 / 0"),
      CreateVariable(
        UnresolvedIdentifier(Seq("var1")),
        DefaultValueExpression(Cast(
          Divide(Literal(1, IntegerType), Literal(0, IntegerType)), IntegerType),
          "1 / 0"),
        replace = true))
  }

  test("declare variable - not support syntax") {
    // IF NOT EXISTS
    checkError(
      exception = intercept[ParseException] {
        parsePlan("DECLARE VARIABLE IF NOT EXISTS var1 INT")
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'EXISTS'", "hint" -> "")
    )

    // The datatype or default value of a variable must be specified
    val sqlText = "DECLARE VARIABLE var1"
    checkError(
      exception = intercept[ParseException] {
        parsePlan(sqlText)
      },
      condition = "INVALID_SQL_SYNTAX.VARIABLE_TYPE_OR_DEFAULT_REQUIRED",
      parameters = Map.empty,
      context = ExpectedContext(fragment = sqlText, start = 0, stop = 20)
    )
  }
}
