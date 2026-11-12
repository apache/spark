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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{
  ExpressionWithUnresolvedIdentifier,
  NamedParameter,
  OptionalReferences,
  PosParameter,
  UnresolvedAttribute,
  UnresolvedExtractValue,
  UnresolvedFunction
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Cast,
  Expression,
  InSubquery,
  LambdaFunction,
  Literal,
  SemiStructuredExtract,
  SubqueryExpression,
  UnresolvedNamedLambdaVariable
}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SetVariable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

/**
 * Tests the parsing of optional references: a `?` after an identifier of a column, field, or
 * variable reference, or after a named parameter marker.
 */
class OptionalReferencesParserSuite extends PlanTest {

  private def withOptionalReferences(enabled: Boolean)(f: => Unit): Unit = {
    withSQLConf(SQLConf.OPTIONAL_REFERENCES_ENABLED.key -> enabled.toString)(f)
  }

  private def parseExpression(sqlText: String): Expression =
    CatalystSqlParser.parseExpression(sqlText)

  private def parsePlan(sqlText: String, optionalReferences: Boolean): LogicalPlan = {
    var plan: LogicalPlan = null
    withOptionalReferences(optionalReferences) {
      plan = CatalystSqlParser.parsePlan(sqlText)
    }
    plan
  }

  /** Returns the positional parameters of `plan` and of the plans of its subqueries. */
  private def positionalParameters(plan: LogicalPlan): Seq[PosParameter] = {
    plan
      .flatMap(_.expressions.flatMap(_.flatMap {
        case parameter: PosParameter => Seq(parameter)
        case subquery: SubqueryExpression => positionalParameters(subquery.plan)
        case _ => Nil
      }))
      .distinct
  }

  /** Returns the expression that an `ExpressionWithUnresolvedIdentifier` builds for `nameParts`. */
  private def buildIdentifierExpression(
      expression: Expression,
      nameParts: Seq[String]): Expression = expression match {
    case expressionWithUnresolvedIdentifier: ExpressionWithUnresolvedIdentifier =>
      expressionWithUnresolvedIdentifier
        .exprBuilder(nameParts, expressionWithUnresolvedIdentifier.otherExprs)
    case other =>
      other
  }

  private def assertMultipartName(
      expression: Expression,
      expectedNameParts: Seq[String],
      expectedOptionalNamePartIndices: Set[Int]): Unit = expression match {
    case unresolvedAttribute: UnresolvedAttribute =>
      assert(unresolvedAttribute.nameParts == expectedNameParts)
      assert(
        OptionalReferences.optionalNamePartIndices(unresolvedAttribute) ==
        expectedOptionalNamePartIndices
      )
    case unresolvedNamedLambdaVariable: UnresolvedNamedLambdaVariable =>
      assert(unresolvedNamedLambdaVariable.nameParts == expectedNameParts)
      assert(
        OptionalReferences.optionalNamePartIndices(unresolvedNamedLambdaVariable) ==
        expectedOptionalNamePartIndices
      )
    case other =>
      fail(s"Expected an unresolved multipart name, but got $other")
  }

  /** Returns the expected query context of the first occurrence of `fragment` in `sqlText`. */
  private def expectedContext(sqlText: String, fragment: String): ExpectedContext = {
    val start = sqlText.indexOf(fragment)
    assert(start >= 0, s"`$fragment` does not occur in `$sqlText`")
    ExpectedContext(fragment = fragment, start = start, stop = start + fragment.length - 1)
  }

  /**
   * Checks that `parse` fails with the syntax error that the parser raises for an unexpected
   * token, at the `?` at `position` of `sqlText`.
   */
  private def checkUnexpectedMarker(sqlText: String, position: Int)(parse: String => Any): Unit = {
    assert(sqlText.charAt(position) == '?')
    val exception = intercept[ParseException](parse(sqlText))
    assert(exception.getCondition == "PARSE_SYNTAX_ERROR")
    assert(exception.getMessageParameters.get("error") == "'?'")
    assert(exception.getQueryContext.isEmpty)
    assert(exception.start.line.contains(1))
    assert(exception.start.startPosition.contains(position))
    assert(exception.getMessage.contains(sqlText))
  }

  test("a ? after an identifier marks that name part as optional") {
    withOptionalReferences(enabled = true) {
      assertMultipartName(parseExpression("x?"), Seq("x"), Set(0))
      assertMultipartName(parseExpression("x"), Seq("x"), Set.empty)
      assertMultipartName(parseExpression("s?.x"), Seq("s", "x"), Set(0))
      assertMultipartName(parseExpression("s.x?"), Seq("s", "x"), Set(1))
      assertMultipartName(parseExpression("s?.x?"), Seq("s", "x"), Set(0, 1))
      assertMultipartName(parseExpression("a.b?.c"), Seq("a", "b", "c"), Set(1))
      assertMultipartName(parseExpression("a?.b?.c?"), Seq("a", "b", "c"), Set(0, 1, 2))
      assertMultipartName(parseExpression("`a.b`?"), Seq("a.b"), Set(0))
    }
  }

  test("whitespace and comments before ? are insignificant") {
    withOptionalReferences(enabled = true) {
      assertMultipartName(parseExpression("x ?"), Seq("x"), Set(0))
      assertMultipartName(parseExpression("s /* comment */ ? . x"), Seq("s", "x"), Set(0))
      assertMultipartName(parseExpression("s . x\n?"), Seq("s", "x"), Set(1))
    }
  }

  test("a ? after an IDENTIFIER clause marks its last name part as optional") {
    withOptionalReferences(enabled = true) {
      // A string literal is resolved by the parser.
      assertMultipartName(parseExpression("IDENTIFIER('t.c')?"), Seq("t", "c"), Set(1))
      assertMultipartName(parseExpression("IDENTIFIER('t')?.c"), Seq("t", "c"), Set(0))
      assertMultipartName(parseExpression("s.IDENTIFIER('x')?"), Seq("s", "x"), Set(1))
      // Any other expression is resolved by the analyzer.
      Seq("IDENTIFIER(name)?", "IDENTIFIER('t' || '.c') ?").foreach { sqlText =>
        val identifierClause = parseExpression(sqlText)
        assert(identifierClause.isInstanceOf[ExpressionWithUnresolvedIdentifier])
        assert(OptionalReferences.hasOptionalMarker(identifierClause))
        assertMultipartName(
          buildIdentifierExpression(identifierClause, Seq("t", "c")),
          Seq("t", "c"),
          Set(1)
        )
      }
      val identifierClause = parseExpression("IDENTIFIER(name)")
      assert(!OptionalReferences.hasOptionalMarker(identifierClause))
      assertMultipartName(
        buildIdentifierExpression(identifierClause, Seq("c")),
        Seq("c"),
        Set.empty
      )
    }
  }

  test("a ? after an IDENTIFIER clause with the legacy IDENTIFIER clause") {
    withSQLConf(
      SQLConf.OPTIONAL_REFERENCES_ENABLED.key -> "true",
      SQLConf.LEGACY_IDENTIFIER_CLAUSE_ONLY.key -> "true") {
      assertMultipartName(
        buildIdentifierExpression(parseExpression("IDENTIFIER('t.c')?"), Seq("t", "c")),
        Seq("t", "c"),
        Set(1)
      )
    }
  }

  test("? is a syntax error after an IDENTIFIER clause that is not a column name") {
    withOptionalReferences(enabled = true) {
      // Like without the `?`, a field name can only be a string literal.
      checkUnexpectedMarker("s.IDENTIFIER('x' || 'y')?", position = 24)(parseExpression)
      // A function name is not a reference.
      intercept[ParseException](parseExpression("IDENTIFIER('abs')?(1)"))
    }
  }

  test("a ? after a field of another expression marks the extraction as optional") {
    withOptionalReferences(enabled = true) {
      parseExpression("array(named_struct('a', 1))[0].b?") match {
        case unresolvedExtractValue: UnresolvedExtractValue =>
          assert(unresolvedExtractValue.extraction == Literal("b"))
          assert(OptionalReferences.isOptional(unresolvedExtractValue))
        case other =>
          fail(s"Expected an UnresolvedExtractValue, but got $other")
      }
      parseExpression("array(named_struct('a', 1))[0].b") match {
        case unresolvedExtractValue: UnresolvedExtractValue =>
          assert(!OptionalReferences.isOptional(unresolvedExtractValue))
        case other =>
          fail(s"Expected an UnresolvedExtractValue, but got $other")
      }
    }
  }

  test("a ? before :: is an optional reference that is cast") {
    withOptionalReferences(enabled = true) {
      Seq("x?::INT", "x? ::INT", "(x?)::INT", "CAST(x? AS INT)").foreach { sqlText =>
        parseExpression(sqlText) match {
          case cast: Cast =>
            assert(cast.dataType == IntegerType)
            assertMultipartName(cast.child, Seq("x"), Set(0))
          case other =>
            fail(s"Expected a Cast, but got $other")
        }
      }
      parseExpression("?::INT") match {
        case cast: Cast =>
          assert(cast.child.isInstanceOf[PosParameter])
        case other =>
          fail(s"Expected a Cast, but got $other")
      }
    }
  }

  test("a ? before a semi-structured extraction path is an optional reference") {
    withOptionalReferences(enabled = true) {
      parseExpression("v?:a") match {
        case Alias(semiStructuredExtract: SemiStructuredExtract, "a") =>
          assertMultipartName(semiStructuredExtract.child, Seq("v"), Set(0))
        case other =>
          fail(s"Expected an aliased SemiStructuredExtract, but got $other")
      }
    }
  }

  test("lambda variables keep their optional name parts") {
    withOptionalReferences(enabled = true) {
      parseExpression("transform(a, x -> x.f?)") match {
        case unresolvedFunction: UnresolvedFunction =>
          unresolvedFunction.arguments(1) match {
            case lambdaFunction: LambdaFunction =>
              assertMultipartName(lambdaFunction.function, Seq("x", "f"), Set(1))
            case other =>
              fail(s"Expected a LambdaFunction, but got $other")
          }
        case other =>
          fail(s"Expected an UnresolvedFunction, but got $other")
      }
    }
  }

  test("optional named parameter markers") {
    withOptionalReferences(enabled = true) {
      Seq(":p?" -> true, ":p ?" -> true, ":p" -> false).foreach {
        case (sqlText, optional) =>
          parseExpression(sqlText) match {
            case namedParameter: NamedParameter =>
              assert(namedParameter.name == "p")
              assert(OptionalReferences.isOptional(namedParameter) == optional)
            case other =>
              fail(s"Expected a NamedParameter, but got $other")
          }
      }
      assert(parseExpression("?").isInstanceOf[PosParameter])
    }
  }

  test("? is a syntax error while optional references are disabled") {
    withOptionalReferences(enabled = false) {
      Seq(
        "x?" -> 1,
        "s.x?" -> 3,
        "s?.x" -> 1,
        "array(1)[0].x?" -> 13,
        "IDENTIFIER('x')?" -> 15,
        "IDENTIFIER(name)?" -> 16
      ).foreach {
        case (sqlText, position) =>
          checkUnexpectedMarker(sqlText, position)(parseExpression)
      }
      assertMultipartName(parseExpression("s.x"), Seq("s", "x"), Set.empty)
    }
  }

  test("optional references are disabled by default") {
    checkUnexpectedMarker("x?", position = 1)(parseExpression)
    checkUnexpectedMarker("SELECT a, b ? FROM t", position = 12)(CatalystSqlParser.parsePlan)
  }

  test("? is a syntax error after anything other than a reference") {
    withOptionalReferences(enabled = true) {
      Seq(
        "abs(1)?" -> 6,
        "(x)?" -> 3,
        "1 ?" -> 2,
        "a[0]?" -> 4,
        "x??" -> 2,
        "x? ?" -> 3
      ).foreach {
        case (sqlText, position) =>
          checkUnexpectedMarker(sqlText, position)(parseExpression)
      }
      Seq("t.*?", "x? -> x").foreach { sqlText =>
        intercept[ParseException](parseExpression(sqlText))
      }
    }
  }

  test("? is a syntax error after a column name that is a regular expression") {
    withSQLConf(
      SQLConf.OPTIONAL_REFERENCES_ENABLED.key -> "true",
      SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "true") {
      checkUnexpectedMarker("SELECT `(a)+`? FROM t", position = 13)(CatalystSqlParser.parsePlan)
      checkUnexpectedMarker("SELECT t.`(a)+`? FROM t", position = 15)(CatalystSqlParser.parsePlan)
      checkUnexpectedMarker("SELECT t?.`(a)+` FROM t", position = 8)(CatalystSqlParser.parsePlan)
    }
  }

  test("a ? that is a positional parameter marker stays one") {
    withOptionalReferences(enabled = true) {
      Seq(
        "SELECT DISTINCT ? FROM t" -> 1,
        "SELECT ALL ? FROM t" -> 1,
        "SELECT NOT ? FROM t" -> 1,
        "SELECT CASE ? WHEN ? THEN ? ELSE ? END" -> 4,
        "SELECT trim(BOTH ? FROM s) FROM t" -> 1,
        "SELECT a FROM t WHERE b = ? AND c IN (?, ?) LIMIT ?" -> 4,
        "SELECT a? FROM t WHERE b? = ?" -> 1
      ).foreach {
        case (sqlText, expectedPositionalParameters) =>
          val plan = parsePlan(sqlText, optionalReferences = true)
          assert(positionalParameters(plan).length == expectedPositionalParameters, sqlText)
      }
    }
  }

  test("a statement that is valid without optional references keeps its meaning") {
    // Each `?` follows a keyword that is also a valid name of a column, so it could mark an
    // optional reference to that column.
    Seq(
      "SELECT a FROM t WHERE a IN (SELECT ?)",
      "SELECT a FROM t WHERE a NOT IN (SELECT ? + 1)",
      "SELECT a FROM t WHERE (a, b) IN (SELECT ?, ?)",
      "SELECT a FROM t WHERE a IN (VALUES ?, ?)",
      "SELECT a FROM t WHERE a = (SELECT ?, ?)",
      "SELECT first(DISTINCT ?), last(ALL ?), any_value(DISTINCT ?) FROM t",
      "SELECT trim(FROM ?) FROM t",
      "SELECT json_array(ALL ?, ?) FROM t",
      "SELECT NOT LIKE ? FROM t",
      "SELECT CASE WHEN WHEN ? THEN ? END FROM t",
      "SELECT count(DISTINCT ?) FROM t"
    ).foreach { sqlText =>
      val plan = parsePlan(sqlText, optionalReferences = true)
      comparePlans(plan, parsePlan(sqlText, optionalReferences = false), checkAnalysis = false)
      assert(positionalParameters(plan).length == sqlText.count(_ == '?'), sqlText)
    }

    // Parameter markers in string literals are only supported when they are substituted.
    Seq(
      "SELECT INTERVAL ? DAY",
      "SELECT 'a' ? 'b'",
      "SELECT trim(FROM ?) FROM t WHERE a IN (SELECT ?)"
    ).foreach { sqlText =>
      val parameters = List.fill(sqlText.count(_ == '?'))("'1'")
      Seq(true, false).foreach { enabled =>
        withOptionalReferences(enabled) {
          val substituted =
            new SubstituteParamsParser().substitute(sqlText, positionalParams = parameters)._1
          assert(substituted == sqlText.replace("?", "'1'"), sqlText)
        }
      }
    }
  }

  test("a ? after SELECT or VALUES in an IN subquery is a positional parameter marker") {
    withOptionalReferences(enabled = true) {
      // The optional reference `b?` makes these statements invalid without optional references.
      Seq(
        "SELECT b? FROM t WHERE a IN (SELECT ?)" -> 1,
        "SELECT b? FROM t WHERE a NOT IN (SELECT ? + 1)" -> 1,
        "SELECT b? FROM t WHERE (a, b) IN (SELECT ?, ?)" -> 2,
        "SELECT b? FROM t WHERE a IN (VALUES ?, ?)" -> 2
      ).foreach {
        case (sqlText, expectedPositionalParameters) =>
          val subqueries = CatalystSqlParser
            .parsePlan(sqlText)
            .flatMap(_.expressions.flatMap(_.collect { case in: InSubquery => in.query.plan }))
          assert(subqueries.length == 1, sqlText)
          assert(
            positionalParameters(subqueries.head).length == expectedPositionalParameters,
            sqlText
          )
      }
      // A quoted name is a reference.
      val plan = CatalystSqlParser.parsePlan("SELECT a FROM t WHERE a IN (`select`?)")
      assert(plan.flatMap(_.expressions.flatMap(_.collect { case in: InSubquery => in })).isEmpty)
    }
  }

  test("optional references in a statement that a rule for free text accepts without them") {
    withOptionalReferences(enabled = true) {
      // Without optional references, `SET .*?` is the only rule that parses these statements.
      Seq(
        "SET VAR v = a?",
        "SET VARIABLE v = (SELECT max(a?) FROM t)",
        "SET VAR (v, w) = (SELECT a?, b FROM t)"
      ).foreach { sqlText =>
        assert(CatalystSqlParser.parsePlan(sqlText).isInstanceOf[SetVariable], sqlText)
      }

      // The parameter markers of such a statement are substituted.
      val substitutor = new SubstituteParamsParser()
      assert(substitutor.substitute("SET VAR v = :p?", Map("p" -> "1"))._1 == "SET VAR v = 1")
      assert(substitutor.substitute("SET VAR v = :p?", Map("q" -> "1"))._1 == "SET VAR v = NULL")
      assert(
        substitutor.substitute("SET VAR v = coalesce(a?, ?)", positionalParams = List("1"))._1 ==
        "SET VAR v = coalesce(a?, 1)"
      )
      // As before, a `?` in free text is not a parameter marker.
      assert(
        substitutor.substitute("SET spark.a.b=c?d", positionalParams = List("1"))._1 ==
        "SET spark.a.b=c?d"
      )
    }
  }

  test("optional references in a query that is parsed without its end") {
    withSQLConf(
      SQLConf.OPTIONAL_REFERENCES_ENABLED.key -> "true",
      SQLConf.LEGACY_PARSE_QUERY_WITHOUT_EOF.key -> "true") {
      // The rule for a query stops before a `?` that is not a parameter marker.
      val sqlText = "SELECT a, b? FROM t WHERE c? IS NULL"
      val plan = CatalystSqlParser.parseQuery(sqlText)
      comparePlans(plan, CatalystSqlParser.parsePlan(sqlText), checkAnalysis = false)
      val optionalNames = plan.flatMap(_.expressions.flatMap(_.collect {
        case attribute: UnresolvedAttribute
            if OptionalReferences.optionalNamePartIndices(attribute).nonEmpty =>
          attribute.name
      }))
      assert(optionalNames.toSet == Set("b", "c"))
    }
  }

  test("statements with optional references are split like other statements") {
    withOptionalReferences(enabled = true) {
      val result =
        SqlStatementSplitter.split("BEGIN SELECT a? FROM t; SELECT 1; END; SELECT b?; SELECT ?")
      assert(
        result.completeStatements.map(_.statement) ==
        Seq("BEGIN SELECT a? FROM t; SELECT 1; END", "SELECT b?")
      )
      assert(result.partialStatement == "SELECT ?")
      // Without optional references, this is the start of an interval literal.
      assert(
        SqlStatementSplitter.split("SELECT interval?;").completeStatements.map(_.statement) ==
        Seq("SELECT interval?")
      )
    }
  }

  test("optional references are not supported in statements that modify table data") {
    withOptionalReferences(enabled = true) {
      Seq(
        ("UPDATE t SET a = b? WHERE c = 1", "UPDATE", "b?"),
        ("UPDATE t SET a = 1 WHERE t.c? = 1", "UPDATE", "t.c?"),
        ("DELETE FROM t WHERE c? = 1", "DELETE", "c?"),
        ("DELETE FROM t WHERE c IN (SELECT d? FROM s)", "DELETE", "d?"),
        (
          "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN UPDATE SET t.b = s.b?",
          "MERGE",
          "s.b?"
        ),
        (
          "MERGE INTO t USING (SELECT a? FROM s) s ON t.a = s.a WHEN MATCHED THEN DELETE",
          "MERGE",
          "a?"
        ),
        ("INSERT INTO t REPLACE WHERE c? = 1 SELECT * FROM s", "INSERT", "c?"),
        ("INSERT INTO t AS t REPLACE ON t.c? = s.c SELECT * FROM s", "INSERT", "t.c?"),
        // The common table expressions of the statement.
        ("WITH s AS (SELECT c? FROM u) DELETE FROM t WHERE c IN (SELECT c FROM s)", "DELETE", "c?"),
        ("WITH s AS (SELECT c? FROM u) UPDATE t SET a = (SELECT max(c) FROM s)", "UPDATE", "c?"),
        (
          "WITH s AS (SELECT a? FROM u) MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE",
          "MERGE",
          "a?"
        )
      ).foreach {
        case (sqlText, statement, fragment) =>
          checkError(
            exception = intercept[ParseException](CatalystSqlParser.parsePlan(sqlText)),
            condition = "OPTIONAL_COLUMN_REFERENCE_NOT_SUPPORTED.IN_DML",
            sqlState = Some("0A000"),
            parameters = Map("context" -> statement),
            context = expectedContext(sqlText, fragment)
          )
      }
    }
  }

  test("optional named parameter markers are not supported in statements that modify data") {
    withOptionalReferences(enabled = true) {
      val sqlText = "UPDATE t SET a = :v? WHERE c = 1"
      Seq[() => Any](
        // Without parameter values, the marker is parsed as a named parameter.
        () => CatalystSqlParser.parsePlan(sqlText),
        // With parameter values, the marker is substituted before the statement is parsed.
        () => new SubstituteParamsParser().substitute(sqlText, Map("v" -> "1"))
      ).foreach { parse =>
        checkError(
          exception = intercept[ParseException](parse()),
          condition = "OPTIONAL_COLUMN_REFERENCE_NOT_SUPPORTED.IN_DML",
          sqlState = Some("0A000"),
          parameters = Map("context" -> "UPDATE"),
          context = expectedContext(sqlText, ":v?")
        )
      }
    }
  }

  test("optional named parameter markers are not supported in partition specifications") {
    withOptionalReferences(enabled = true) {
      Seq(
        "TRUNCATE TABLE t PARTITION (p = :v?)",
        "ALTER TABLE t DROP PARTITION (p = :v?)",
        "INSERT INTO t PARTITION (p = :v?) SELECT a FROM s"
      ).foreach { sqlText =>
        Seq[() => Any](
          () => CatalystSqlParser.parsePlan(sqlText),
          () => new SubstituteParamsParser().substitute(sqlText, Map("v" -> "1"))
        ).foreach { parse =>
          checkError(
            exception = intercept[ParseException](parse()),
            condition = "OPTIONAL_COLUMN_REFERENCE_NOT_SUPPORTED.IN_DML",
            sqlState = Some("0A000"),
            parameters = Map("context" -> "PARTITION"),
            context = expectedContext(sqlText, ":v?")
          )
        }
      }
    }
  }

  test("optional references are not supported in expressions that define table objects") {
    withOptionalReferences(enabled = true) {
      Seq(
        ("CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a? + 1))", "GENERATED ALWAYS AS"),
        ("CREATE TABLE t (a INT, b INT DEFAULT a?)", "DEFAULT"),
        ("ALTER TABLE t ALTER COLUMN b SET DEFAULT a?", "DEFAULT"),
        ("ALTER TABLE t ADD CONSTRAINT c CHECK (a? > 0)", "CHECK"),
        ("CREATE TABLE t (a INT, CONSTRAINT c CHECK (a? > 0))", "CHECK"),
        ("CREATE TABLE t (a INT CHECK (a? > 0))", "CHECK"),
        ("CREATE TABLE t (b INT) USING parquet OPTIONS (compression = a?)", "OPTIONS")
      ).foreach {
        case (sqlText, clause) =>
          checkError(
            exception = intercept[ParseException](CatalystSqlParser.parsePlan(sqlText)),
            condition = "OPTIONAL_COLUMN_REFERENCE_NOT_SUPPORTED.IN_DDL",
            sqlState = Some("0A000"),
            parameters = Map("context" -> clause),
            context = expectedContext(sqlText, "a?")
          )
      }
    }
  }

  test("optional references are allowed in queries that read data") {
    withOptionalReferences(enabled = true) {
      Seq(
        "SELECT a?, t.b?, s?.c FROM t",
        "INSERT INTO t SELECT a? FROM s",
        "INSERT INTO t VALUES (a?)",
        "INSERT INTO t REPLACE WHERE c = 1 SELECT a? FROM s",
        "WITH cte AS (SELECT a? FROM t) SELECT * FROM cte",
        "WITH cte AS (SELECT a? FROM s) INSERT INTO t SELECT * FROM cte",
        "SELECT * FROM VALUES (a?) AS v(c)",
        "CREATE TABLE t2 AS SELECT a? FROM t",
        "DECLARE OR REPLACE VARIABLE v INT DEFAULT a?"
      ).foreach { sqlText =>
        CatalystSqlParser.parsePlan(sqlText)
      }
    }
  }

  test("unbound optional named parameter markers are substituted with NULL") {
    withOptionalReferences(enabled = true) {
      val substitutor = new SubstituteParamsParser()
      def substitute(sqlText: String, namedParameters: Map[String, String]): String =
        substitutor.substitute(sqlText, namedParameters)._1

      assert(substitute("SELECT :x?, :y?", Map("x" -> "1")) == "SELECT 1, NULL")
      assert(substitute("SELECT :x ?, :x?", Map("x" -> "1")) == "SELECT 1, 1")
      assert(
        substitute("SELECT :x? WHERE :x? IS NULL", Map.empty) == "SELECT NULL WHERE NULL IS NULL"
      )
      assert(substitute("SELECT :x?::INT", Map("x" -> "'5'")) == "SELECT '5'::INT")
      // A keyword or an identifier right after the `?` stays separate from the value.
      assert(substitute("SELECT :x?AND true", Map("x" -> "false")) == "SELECT false AND true")
      assert(substitute("SELECT :x?AS c, :y?_c", Map("x" -> "1")) == "SELECT 1 AS c, NULL _c")

      val sqlText = "SELECT :y?, :y"
      checkError(
        exception = intercept[AnalysisException](substitute(sqlText, Map.empty)),
        condition = "UNBOUND_SQL_PARAMETER",
        parameters = Map("name" -> "y"),
        context = ExpectedContext(fragment = ":y", start = 12, stop = 13)
      )
    }
    // The name of a parameter can be a double-quoted identifier.
    withSQLConf(
      SQLConf.OPTIONAL_REFERENCES_ENABLED.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.DOUBLE_QUOTED_IDENTIFIERS.key -> "true") {
      val substituted = new SubstituteParamsParser().substitute("SELECT :\"x\"?", Map("x" -> "1"))
      assert(substituted._1 == "SELECT 1")
    }
  }

  test(
    "a ? after a named parameter marker is a positional parameter marker while optional " +
    "references are disabled"
  ) {
    withOptionalReferences(enabled = false) {
      // As before, the two markers are the parts of a string literal, which then is not valid.
      val parseException = intercept[ParseException](parseExpression(":p?"))
      assert(parseException.getCondition == "UNEXPECTED_USE_OF_PARAMETER_MARKER")
      val substitutionException = intercept[AnalysisException] {
        new SubstituteParamsParser().substitute("SELECT :x?", Map("x" -> "1"))
      }
      assert(substitutionException.getCondition == "INVALID_QUERY_MIXED_QUERY_PARAMETERS")
    }
  }
}
