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
package org.apache.spark.sql.expressions

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAttribute, UnresolvedDataFrameStar, UnresolvedExtractValue, UnresolvedFunction, UnresolvedRegex, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, CaseWhen, Cast, CurrentRow, Descending, DropField, EvalMode, Expression, ExprId, FrameType, LambdaFunction, Literal, NullOrdering, NullsFirst, NullsLast, RangeFrame, RowFrame, SortDirection, SortOrder, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, UnresolvedNamedLambdaVariable, UnspecifiedFrame, UpdateFields, WindowExpression, WindowSpecDefinition, WithField}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.column
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DataType, DoubleType, IntegerType, LongType, ShortType, StringType}

/**
 * Test suite for [[column.ColumnNode]] to [[Expression]] conversions.
 */
class ColumnNodeToExpressionConverterSuite extends SparkFunSuite {
  private object Converter extends ColumnNodeToExpressionConverter {
    override val conf: SQLConf = new SQLConf
    override val parser: ParserInterface = new SparkSqlParser
  }

  private def testConversion(node: => column.ColumnNode, expected: Expression): Expression = {
    val myOrigin = Origin()
    CurrentOrigin.withOrigin(myOrigin) {
      val expression = normalizeExpression(Converter(node))
      assert(expression == normalizeExpression(expected))
      assert(expression.origin eq myOrigin)
      expression
    }
  }

  private def normalizeExpression(e: Expression): Expression = e.transform {
    case a: Alias =>
      a.copy()(exprId = ExprId(0), a.qualifier, a.explicitMetadata, a.nonInheritableMetadataKeys)
  }

  test("literal") {
    testConversion(column.Literal(1), Literal(1, IntegerType))
    testConversion(column.Literal("foo", Option(StringType)), Literal.create("foo", StringType))
  }

  test("attribute") {
    val expression1 = testConversion(column.UnresolvedAttribute("x"), UnresolvedAttribute("x"))
    assert(expression1.getTagValue(LogicalPlan.PLAN_ID_TAG).isEmpty)
    assert(expression1.getTagValue(LogicalPlan.IS_METADATA_COL).isEmpty)

    val expression2 = testConversion(
      column.UnresolvedAttribute("y", Option(44L), isMetadataColumn = true),
      UnresolvedAttribute("y"))
    assert(expression2.getTagValue(LogicalPlan.PLAN_ID_TAG).contains(44L))
    assert(expression2.getTagValue(LogicalPlan.IS_METADATA_COL).isDefined)
  }

  test("star") {
    testConversion(column.UnresolvedStar(None), UnresolvedStar(None))
    testConversion(
      column.UnresolvedStar(Option("x.y.z")),
      UnresolvedStar(Option(Seq("x", "y", "z"))))
    testConversion(
      column.UnresolvedStar(None, Option(10L)),
      UnresolvedDataFrameStar(10L))
  }

  test("regex") {
    testConversion(
      column.UnresolvedRegex("`(_1)?+.+`"),
      UnresolvedRegex("(_1)?+.+", None, caseSensitive = false))

    val expression = testConversion(
      column.UnresolvedRegex("a", planId = Option(11L)),
      UnresolvedAttribute("a"))
    assert(expression.getTagValue(LogicalPlan.PLAN_ID_TAG).contains(11L))
    assert(expression.getTagValue(LogicalPlan.IS_METADATA_COL).isEmpty)
  }

  test("function") {
    testConversion(
      column.UnresolvedFunction("+", Seq(column.UnresolvedAttribute("a"), column.Literal(1))),
      UnresolvedFunction(Seq("+"), Seq(UnresolvedAttribute("a"), Literal(1)), isDistinct = false))
    testConversion(
      column.UnresolvedFunction(
        "db1.myAgg",
        Seq(column.UnresolvedAttribute("a")),
        isDistinct = true,
        isUserDefinedFunction = true),
      UnresolvedFunction(
        Seq("db1", "myAgg"),
        Seq(UnresolvedAttribute("a")),
        isDistinct = true))
  }

  test("alias") {
    testConversion(
      column.Alias(column.Literal("qwe"), "newA" :: Nil),
      Alias(Literal("qwe"), "newA")())
    testConversion(
      column.Alias(column.UnresolvedAttribute("complex"), "newA" :: "newB" :: Nil),
      MultiAlias(UnresolvedAttribute("complex"), Seq("newA", "newB")))
  }

  private def testCast(
      dataType: DataType,
      colEvalMode: column.Cast.EvalMode.Value,
      catEvalMode: EvalMode.Value): Unit = {
    testConversion(
      column.Cast(column.UnresolvedAttribute("attr"), dataType, Option(colEvalMode)),
      Cast(UnresolvedAttribute("attr"), dataType, evalMode = catEvalMode))
  }

  test("cast") {
    testConversion(
      column.Cast(column.UnresolvedAttribute("str"), DoubleType),
      Cast(UnresolvedAttribute("str"), DoubleType))

    testCast(LongType, column.Cast.EvalMode.Legacy, EvalMode.LEGACY)
    testCast(BinaryType, column.Cast.EvalMode.Try, EvalMode.TRY)
    testCast(ShortType, column.Cast.EvalMode.Ansi, EvalMode.ANSI)
  }

  private def testSortOrder(
      colDirection: column.SortOrder.SortDirection.SortDirection,
      colNullOrdering: column.SortOrder.NullOrdering.NullOrdering,
      catDirection: SortDirection,
      catNullOrdering: NullOrdering): Unit = {
    testConversion(
      column.SortOrder(column.UnresolvedAttribute("unsorted"), colDirection, colNullOrdering),
      new SortOrder(UnresolvedAttribute("unsorted"), catDirection, catNullOrdering, Nil))
  }

  test("sortOrder") {
    testSortOrder(
      column.SortOrder.SortDirection.Ascending,
      column.SortOrder.NullOrdering.NullsFirst,
      Ascending,
      NullsFirst)
    testSortOrder(
      column.SortOrder.SortDirection.Ascending,
      column.SortOrder.NullOrdering.NullsLast,
      Ascending,
      NullsLast)
    testSortOrder(
      column.SortOrder.SortDirection.Descending,
      column.SortOrder.NullOrdering.NullsFirst,
      Descending,
      NullsFirst)
    testSortOrder(
      column.SortOrder.SortDirection.Descending,
      column.SortOrder.NullOrdering.NullsLast,
      Descending,
      NullsLast)
  }

  private def testWindowFrame(
      colFrameType: column.WindowFrame.FrameType.FrameType,
      colLower: column.WindowFrame.FrameBoundary,
      colUpper: column.WindowFrame.FrameBoundary,
      catFrameType: FrameType,
      catLower: Expression,
      catUpper: Expression): Unit = {
    testConversion(
      column.Window(
        column.UnresolvedFunction("sum", Seq(column.UnresolvedAttribute("a"))),
        column.WindowSpec(
          Seq(column.UnresolvedAttribute("b"), column.UnresolvedAttribute("c")),
          Seq(column.SortOrder(
            column.UnresolvedAttribute("d"),
            column.SortOrder.SortDirection.Descending,
            column.SortOrder.NullOrdering.NullsLast)),
          Option(column.WindowFrame(colFrameType, colLower, colUpper)))),
      WindowExpression(
        UnresolvedFunction("sum", Seq(UnresolvedAttribute("a")), isDistinct = false),
        WindowSpecDefinition(
          Seq(UnresolvedAttribute("b"), UnresolvedAttribute("c")),
          Seq(SortOrder(UnresolvedAttribute("d"), Descending, NullsLast, Nil)),
          SpecifiedWindowFrame(catFrameType, catLower, catUpper))))
  }

  test("window") {
    testConversion(
      column.Window(
        column.UnresolvedFunction("sum", Seq(column.UnresolvedAttribute("a"))),
        column.WindowSpec(
          Seq(column.UnresolvedAttribute("b"), column.UnresolvedAttribute("c")),
          Nil,
          None)),
      WindowExpression(
        UnresolvedFunction("sum", Seq(UnresolvedAttribute("a")), isDistinct = false),
        WindowSpecDefinition(
          Seq(UnresolvedAttribute("b"), UnresolvedAttribute("c")),
          Nil,
          UnspecifiedFrame)))
    testWindowFrame(
      column.WindowFrame.FrameType.Row,
      column.WindowFrame.Value(column.Literal(-10)),
      column.WindowFrame.Unbounded,
      RowFrame,
      Literal(-10),
      UnboundedFollowing)
    testWindowFrame(
      column.WindowFrame.FrameType.Range,
      column.WindowFrame.Unbounded,
      column.WindowFrame.CurrentRow,
      RangeFrame,
      UnboundedPreceding,
      CurrentRow)
  }

  test("lambda") {
    val colX = column.UnresolvedNamedLambdaVariable("x")
    val catX = UnresolvedNamedLambdaVariable(Seq("x"))
    testConversion(
      column.LambdaFunction(
        column.UnresolvedFunction("+", Seq(colX, column.UnresolvedAttribute("y"))),
        Seq(colX)),
      LambdaFunction(
        UnresolvedFunction("+", Seq(catX, UnresolvedAttribute("y")), isDistinct = false),
        Seq(catX)))
  }

  test("caseWhen") {
    testConversion(
      column.CaseWhenOtherwise(
        Seq(column.UnresolvedAttribute("c1") -> column.Literal("r1")),
        Option(column.Literal("fallback"))),
      CaseWhen(
        Seq(UnresolvedAttribute("c1") -> Literal("r1")),
        Option(Literal("fallback")))
    )
  }

  test("extract field") {
    testConversion(
      column.UnresolvedExtractValue(column.UnresolvedAttribute("struct"), column.Literal("cl_a")),
      UnresolvedExtractValue(UnresolvedAttribute("struct"), Literal("cl_a")))
  }

  test("update field") {
    testConversion(
      column.UpdateFields(
        column.UnresolvedAttribute("struct"),
        "col_b",
        Option(column.Literal("cl_a"))),
      UpdateFields(UnresolvedAttribute("struct"), Seq(WithField("col_b", Literal("cl_a")))))

    testConversion(
      column.UpdateFields(column.UnresolvedAttribute("struct"), "col_c", None),
      UpdateFields(UnresolvedAttribute("struct"), Seq(DropField("col_c"))))
  }

  test("extension") {
    testConversion(column.Extension(UnresolvedAttribute("bar")), UnresolvedAttribute("bar"))
  }

  test("unsupported") {
    intercept[SparkException](Converter(column.Extension("kaboom")))
  }
}
