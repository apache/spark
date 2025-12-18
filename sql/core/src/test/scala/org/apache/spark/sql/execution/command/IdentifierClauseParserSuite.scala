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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, ExpressionWithUnresolvedIdentifier, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction, UnresolvedInlineTable, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, LambdaFunction, Literal, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, OneRowRelation, Pivot, Project, SubqueryAlias, Unpivot}
import org.apache.spark.sql.catalyst.util.EvaluateUnresolvedInlineTable
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class IdentifierClauseParserSuite extends AnalysisTest {

  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  private def intercept(sqlCommand: String): ParseException = {
    intercept[ParseException](parsePlan(sqlCommand))
  }

  test("UNPIVOT column alias with IDENTIFIER()") {
    comparePlans(
      parsePlan(
        "SELECT * FROM unpivot_test UNPIVOT (val FOR col IN " +
          "(a AS IDENTIFIER('col_a'), b AS IDENTIFIER('col_b')))"),
      Unpivot(
        None,
        Some(Seq(Seq($"a"), Seq($"b"))),
        Some(Seq(Some("col_a"), Some("col_b"))),
        "col",
        Seq("val"),
        table("unpivot_test"))
        .where(coalesce($"val").isNotNull)
        .select(star())
    )
  }

  test("UNPIVOT multi-value column alias with IDENTIFIER()") {
    comparePlans(
      parsePlan(
        "SELECT * FROM unpivot_test UNPIVOT ((v1, v2) FOR col IN " +
          "((a, b) AS IDENTIFIER('cols_ab'), (b, c) AS IDENTIFIER('cols_bc')))"),
      Unpivot(
        None,
        Some(Seq(Seq($"a", $"b"), Seq($"b", $"c"))),
        Some(Seq(Some("cols_ab"), Some("cols_bc"))),
        "col",
        Seq("v1", "v2"),
        table("unpivot_test"))
        .where(coalesce($"v1", $"v2").isNotNull)
        .select(star())
    )
  }

  test("PIVOT column with IDENTIFIER()") {
    comparePlans(
      parsePlan(
        "SELECT * FROM pivot_test PIVOT (SUM(revenue) FOR IDENTIFIER('quarter') IN ('Q1', 'Q2'))"),
      Pivot(
        None,
        UnresolvedAttribute.quoted("quarter"),
        Seq(Literal("Q1"), Literal("Q2")),
        Seq(UnresolvedFunction("SUM", Seq($"revenue"), isDistinct = false)),
        table("pivot_test"))
        .select(star())
    )
  }

  test("PIVOT value alias with IDENTIFIER()") {
    comparePlans(
      parsePlan(
        "SELECT * FROM pivot_test PIVOT (SUM(revenue) AS IDENTIFIER('total') FOR quarter IN " +
          "('Q1' AS IDENTIFIER('first_quarter'), 'Q2' AS IDENTIFIER('second_quarter')))"),
      Pivot(
        None,
        $"quarter",
        Seq(
          Alias(Literal("Q1"), "first_quarter")(),
          Alias(Literal("Q2"), "second_quarter")()
        ),
        Seq(Alias(UnresolvedFunction("SUM", Seq($"revenue"), isDistinct = false), "total")()),
        table("pivot_test"))
        .select(star())
    )
  }

  test("Lambda variable name with IDENTIFIER()") {
    val lambdaVar = UnresolvedNamedLambdaVariable(Seq("x"))
    comparePlans(
      parsePlan("SELECT transform(array(1, 2, 3), IDENTIFIER('x') -> x + 1)"),
      OneRowRelation()
        .select(
          UnresolvedFunction(
            "transform",
            Seq(
              UnresolvedFunction(
                "array",
                Seq(Literal(1), Literal(2), Literal(3)),
                isDistinct = false),
              LambdaFunction(
                lambdaVar + Literal(1),
                Seq(lambdaVar)
              )
            ),
            isDistinct = false
          )
        )
    )
  }

  test("Struct field names with IDENTIFIER() in CAST") {
    val structType = StructType(Seq(
      StructField("field1", IntegerType),
      StructField("field2", StringType)
    ))
    comparePlans(
      parsePlan(
        "SELECT CAST(named_struct('field1', 1, 'field2', 'a') AS " +
          "STRUCT<IDENTIFIER('field1'): INT, IDENTIFIER('field2'): STRING>)"),
      OneRowRelation()
        .select(
          Cast(
            UnresolvedFunction(
              "named_struct",
              Seq(Literal("field1"), Literal(1), Literal("field2"), Literal("a")),
              isDistinct = false),
            structType
          )
        )
    )
  }

  test("Struct field access with IDENTIFIER()") {
    val plan = parsePlan("SELECT IDENTIFIER('data').IDENTIFIER('field1') FROM struct_field_test")
    val resolvedPlan = plan.transformAllExpressions {
      case e: ExpressionWithUnresolvedIdentifier =>
        e.exprBuilder(Seq(e.identifierExpr.eval().toString), e.otherExprs)
    }

    comparePlans(
      resolvedPlan,
      table("struct_field_test").select(UnresolvedExtractValue($"data", Literal("field1")))
    )
  }

  test("Struct field access with multiple IDENTIFIER() parts") {
    val plan = parsePlan("SELECT IDENTIFIER('a').IDENTIFIER('b').IDENTIFIER('c') FROM t")
    val resolvedPlan = plan.transformAllExpressions {
      case e: ExpressionWithUnresolvedIdentifier =>
        e.exprBuilder(Seq(e.identifierExpr.eval().toString), e.otherExprs)
    }

    comparePlans(
      resolvedPlan,
      table("t").select(
        UnresolvedExtractValue(
          UnresolvedExtractValue($"a", Literal("b")),
          Literal("c")
        )
      )
    )
  }

  test("Partition spec with IDENTIFIER() for partition column name") {
    val plan = parsePlan(
      "INSERT INTO partition_spec_test PARTITION (IDENTIFIER('c2') = 'value1') VALUES (1)")
      .asInstanceOf[InsertIntoStatement]
    val values = EvaluateUnresolvedInlineTable.evaluate(
      UnresolvedInlineTable(Seq("col1"), Seq(Seq(Literal(1)))))

    comparePlans(
      plan,
      InsertIntoStatement(
        plan.table,
        Map("c2" -> Some("value1")),
        Nil,
        values,
        overwrite = false,
        ifPartitionNotExists = false
      )
    )
  }

  test("Pipe operator alias with IDENTIFIER()") {
    val values = EvaluateUnresolvedInlineTable.evaluate(
      UnresolvedInlineTable(Seq("c1", "c2"), Seq(Seq(Literal(1), Literal(2)))))
    comparePlans(
      parsePlan(
        "SELECT * FROM VALUES(1, 2) AS T(c1, c2) |> AS IDENTIFIER('pipe_alias') |> SELECT c1, c2"),
      Project(
        Seq($"c1", $"c2"),
        SubqueryAlias(
          "pipe_alias",
          Project(
            Seq(UnresolvedStar(None)),
            SubqueryAlias("T", values)
          )
        )
      )
    )
  }

  test("Pipe operator alias with IDENTIFIER() - second variant") {
    val values = EvaluateUnresolvedInlineTable.evaluate(
      UnresolvedInlineTable(Seq("c1", "c2"), Seq(Seq(Literal(1), Literal(2)))))
    comparePlans(
      parsePlan(
        "SELECT c1, c2 FROM VALUES(1, 2) AS T(c1, c2) |> AS IDENTIFIER('my_result') |> SELECT *"),
      Project(
        Seq(UnresolvedStar(None)),
        SubqueryAlias(
          "my_result",
          Project(
            Seq($"c1", $"c2"),
            SubqueryAlias("T", values)
          )
        )
      )
    )
  }

  test("Resource type ADD is a keyword - should fail") {
    checkError(
      exception = intercept("ADD IDENTIFIER('file') '/tmp/test.txt'"),
      condition = "INVALID_SQL_SYNTAX.UNSUPPORTED_SQL_STATEMENT",
      parameters = Map("sqlText" -> "ADD IDENTIFIER('file') '/tmp/test.txt'"),
      context = ExpectedContext(
        fragment = "ADD IDENTIFIER('file') '/tmp/test.txt'",
        start = 0,
        stop = 37
      )
    )
  }

  test("Resource type LIST is a keyword - should fail") {
    checkError(
      exception = intercept("LIST IDENTIFIER('files')"),
      condition = "INVALID_SQL_SYNTAX.UNSUPPORTED_SQL_STATEMENT",
      parameters = Map("sqlText" -> "LIST IDENTIFIER('files')"),
      context = ExpectedContext(
        fragment = "LIST IDENTIFIER('files')",
        start = 0,
        stop = 23
      )
    )
  }

  test("CREATE FUNCTION USING resource type is a keyword - should fail") {
    checkError(
      exception = intercept(
        "CREATE FUNCTION keyword_test_func AS 'com.example.Test' " +
          "USING IDENTIFIER('jar') '/path/to.jar'"),
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'('", "hint" -> "")
    )
  }

  test("ANALYZE TABLE NOSCAN is a keyword - should fail") {
    checkError(
      exception = intercept(
        "ANALYZE TABLE analyze_keyword_test COMPUTE STATISTICS IDENTIFIER('noscan')"),
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'('", "hint" -> "")
    )
  }
}
