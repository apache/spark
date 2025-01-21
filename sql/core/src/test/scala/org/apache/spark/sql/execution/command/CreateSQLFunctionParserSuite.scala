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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.catalog.LanguageSQL
import org.apache.spark.sql.catalyst.plans.logical.CreateUserDefinedFunction
import org.apache.spark.sql.execution.SparkSqlParser

class CreateSQLFunctionParserSuite extends AnalysisTest {
  private lazy val parser = new SparkSqlParser()

  private def intercept(sqlCommand: String, messages: String*): Unit =
    interceptParseException(parser.parsePlan)(sqlCommand, messages: _*)()

  private def checkParseError(
      sqlCommand: String,
      errorClass: String,
      parameters: Map[String, String],
      queryContext: Array[ExpectedContext] = Array.empty): Unit =
    assertParseErrorClass(parser.parsePlan, sqlCommand, errorClass, parameters, queryContext)

  // scalastyle:off argcount
  private def createSQLFunction(
      nameParts: Seq[String],
      inputParamText: Option[String] = None,
      returnTypeText: String = "INT",
      exprText: Option[String] = None,
      queryText: Option[String] = None,
      comment: Option[String] = None,
      isDeterministic: Option[Boolean] = None,
      containsSQL: Option[Boolean] = None,
      isTableFunc: Boolean = false,
      ignoreIfExists: Boolean = false,
      replace: Boolean = false): CreateUserDefinedFunction = {
    // scalastyle:on argcount
    CreateUserDefinedFunction(
      UnresolvedIdentifier(nameParts),
      inputParamText = inputParamText,
      returnTypeText = returnTypeText,
      exprText = exprText,
      queryText = queryText,
      comment = comment,
      isDeterministic = isDeterministic,
      containsSQL = containsSQL,
      language = LanguageSQL,
      isTableFunc = isTableFunc,
      ignoreIfExists = ignoreIfExists,
      replace = replace)
  }

  // scalastyle:off argcount
  private def createSQLFunctionCommand(
      name: String,
      inputParamText: Option[String] = None,
      returnTypeText: String = "INT",
      exprText: Option[String] = None,
      queryText: Option[String] = None,
      comment: Option[String] = None,
      isDeterministic: Option[Boolean] = None,
      containsSQL: Option[Boolean] = None,
      isTableFunc: Boolean = false,
      ignoreIfExists: Boolean = false,
      replace: Boolean = false): CreateSQLFunctionCommand = {
    // scalastyle:on argcount
    CreateSQLFunctionCommand(
      FunctionIdentifier(name),
      inputParamText = inputParamText,
      returnTypeText = returnTypeText,
      exprText = exprText,
      queryText = queryText,
      comment = comment,
      isDeterministic = isDeterministic,
      containsSQL = containsSQL,
      isTableFunc = isTableFunc,
      isTemp = true,
      ignoreIfExists = ignoreIfExists,
      replace = replace)
  }

  test("create temporary SQL functions") {
    comparePlans(
      parser.parsePlan("CREATE TEMPORARY FUNCTION a() RETURNS INT RETURN 1"),
      createSQLFunctionCommand("a", exprText = Some("1")))

    comparePlans(
      parser.parsePlan(
        "CREATE TEMPORARY FUNCTION a(x INT) RETURNS TABLE (a INT) RETURN SELECT x"),
      createSQLFunctionCommand(
        name = "a",
        inputParamText = Some("x INT"),
        returnTypeText = "a INT",
        queryText = Some("SELECT x"),
        isTableFunc = true))

    comparePlans(
      parser.parsePlan("CREATE OR REPLACE TEMPORARY FUNCTION a() RETURNS INT RETURN 1"),
      createSQLFunctionCommand("a", exprText = Some("1"), replace = true))

    checkParseError(
      "CREATE TEMPORARY FUNCTION a.b() RETURNS INT RETURN 1",
      errorClass = "INVALID_SQL_SYNTAX.CREATE_TEMP_FUNC_WITH_DATABASE",
      parameters = Map("database" -> "`a`"),
      queryContext = Array(
        ExpectedContext("CREATE TEMPORARY FUNCTION a.b() RETURNS INT RETURN 1", 0, 51)
      )
    )

    checkParseError(
      "CREATE TEMPORARY FUNCTION a.b.c() RETURNS INT RETURN 1",
      errorClass = "INVALID_SQL_SYNTAX.MULTI_PART_NAME",
      parameters = Map(
        "statement" -> "CREATE TEMPORARY FUNCTION",
        "name" -> "`a`.`b`.`c`"),
      queryContext = Array(
        ExpectedContext("CREATE TEMPORARY FUNCTION a.b.c() RETURNS INT RETURN 1", 0, 53)
      )
    )

    checkParseError(
      "CREATE TEMPORARY FUNCTION IF NOT EXISTS a() RETURNS INT RETURN 1",
      errorClass = "INVALID_SQL_SYNTAX.CREATE_TEMP_FUNC_WITH_IF_NOT_EXISTS",
      parameters = Map.empty,
      queryContext = Array(
        ExpectedContext("CREATE TEMPORARY FUNCTION IF NOT EXISTS a() RETURNS INT RETURN 1", 0, 63)
      )
    )
  }

  test("create persistent SQL functions") {
    comparePlans(
      parser.parsePlan("CREATE FUNCTION a() RETURNS INT RETURN 1"),
      createSQLFunction(Seq("a"), exprText = Some("1")))

    comparePlans(
      parser.parsePlan("CREATE FUNCTION a.b(x INT) RETURNS INT RETURN x"),
      createSQLFunction(Seq("a", "b"), Some("x INT"), exprText = Some("x")))

    comparePlans(parser.parsePlan(
      "CREATE FUNCTION a.b.c(x INT) RETURNS TABLE (a INT) RETURN SELECT x"),
      createSQLFunction(Seq("a", "b", "c"), Some("x INT"), returnTypeText = "a INT", None,
        Some("SELECT x"), isTableFunc = true))

    comparePlans(parser.parsePlan("CREATE FUNCTION IF NOT EXISTS a() RETURNS INT RETURN 1"),
      createSQLFunction(Seq("a"), exprText = Some("1"), ignoreIfExists = true)
    )

    comparePlans(parser.parsePlan("CREATE OR REPLACE FUNCTION a() RETURNS INT RETURN 1"),
      createSQLFunction(Seq("a"), exprText = Some("1"), replace = true))

    comparePlans(
      parser.parsePlan(
        """
          |CREATE FUNCTION a(x INT COMMENT 'x') RETURNS INT
          |LANGUAGE SQL DETERMINISTIC CONTAINS SQL
          |COMMENT 'function'
          |RETURN x
          |""".stripMargin),
      createSQLFunction(Seq("a"), inputParamText = Some("x INT COMMENT 'x'"),
        exprText = Some("x"), isDeterministic = Some(true), containsSQL = Some(true),
        comment = Some("function"))
    )

    intercept("CREATE OR REPLACE FUNCTION IF NOT EXISTS a() RETURNS INT RETURN 1",
      "Cannot create a routine with both IF NOT EXISTS and REPLACE specified")
  }

  test("create SQL functions with unsupported routine characteristics") {
    intercept("CREATE FUNCTION foo() RETURNS INT LANGUAGE blah RETURN 1",
      "Operation not allowed: Unsupported language for user defined functions: blah")

    intercept("CREATE FUNCTION foo() RETURNS INT SPECIFIC foo1 RETURN 1",
      "Operation not allowed: SQL function with SPECIFIC name is not supported")

    intercept("CREATE FUNCTION foo() RETURNS INT NO SQL RETURN 1",
      "Operation not allowed: SQL function with NO SQL is not supported")

    intercept("CREATE FUNCTION foo() RETURNS INT NO SQL CONTAINS SQL RETURN 1",
      "Found duplicate clauses: SQL DATA ACCESS")

    intercept("CREATE FUNCTION foo() RETURNS INT RETURNS NULL ON NULL INPUT RETURN 1",
      "Operation not allowed: SQL function with RETURNS NULL ON NULL INPUT is not supported")

    intercept("CREATE FUNCTION foo() RETURNS INT SQL SECURITY INVOKER RETURN 1",
      "Operation not allowed: SQL function with SQL SECURITY INVOKER is not supported")
  }
}
