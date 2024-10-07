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
package org.apache.spark.sql.internal

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.{analysis, expressions, InternalRow}
import org.apache.spark.sql.catalyst.encoders.{encoderFor, AgnosticEncoder}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.aggregate
import org.apache.spark.sql.expressions.{Aggregator, SparkUserDefinedFunction, UserDefinedAggregator}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test suite for [[ColumnNode]] to [[Expression]] conversions.
 */
class ColumnNodeToExpressionConverterSuite extends SparkFunSuite {
  private object Converter extends ColumnNodeToExpressionConverter {
    override val conf: SQLConf = new SQLConf
    override val parser: ParserInterface = new SparkSqlParser
  }

  private def testConversion(node: => ColumnNode, expected: Expression): Expression = {
    val myOrigin = Origin()
    CurrentOrigin.withOrigin(myOrigin) {
      val expression = normalizeExpression(Converter(node))
      assert(expression == normalizeExpression(expected))
      assert(expression.origin eq myOrigin)
      expression
    }
  }

  private def normalizeExpression(e: Expression): Expression = e.transform {
    case a: expressions.Alias =>
      a.copy()(exprId = ExprId(0), a.qualifier, a.explicitMetadata, a.nonInheritableMetadataKeys)
    case a: expressions.AttributeReference =>
      a.withExprId(ExprId(0))
    case d: analysis.UnresolvedDeserializer =>
      d.copy(inputAttributes = d.inputAttributes.map(_.withExprId(ExprId(0))))
    case a: expressions.aggregate.AggregateExpression =>
      a.copy(resultId = ExprId(0))
    case expressions.UnresolvedNamedLambdaVariable(Seq(name)) =>
      expressions.UnresolvedNamedLambdaVariable(name.takeWhile(_ != '_') :: Nil)
  }

  test("literal") {
    testConversion(Literal(1), expressions.Literal(1, IntegerType))
    testConversion(
      Literal("foo", Option(StringType)),
      expressions.Literal.create("foo", StringType))
    val dataType = new StructType()
      .add("_1", DoubleType)
      .add("_2", StringType)
      .add("_3", DoubleType)
      .add("_4", StringType)
    testConversion(
      Literal((12.0, "north", 60.0, "west"), Option(dataType)),
      expressions.Literal(
        InternalRow(12.0, UTF8String.fromString("north"), 60.0, UTF8String.fromString("west")),
        dataType))
  }

  test("attribute") {
    val expression1 = testConversion(UnresolvedAttribute("x"), analysis.UnresolvedAttribute("x"))
    assert(expression1.getTagValue(LogicalPlan.PLAN_ID_TAG).isEmpty)
    assert(expression1.getTagValue(LogicalPlan.IS_METADATA_COL).isEmpty)

    val expression2 = testConversion(
      UnresolvedAttribute("y" :: Nil, Option(44L), isMetadataColumn = true),
      analysis.UnresolvedAttribute("y"))
    assert(expression2.getTagValue(LogicalPlan.PLAN_ID_TAG).contains(44L))
    assert(expression2.getTagValue(LogicalPlan.IS_METADATA_COL).isDefined)
  }

  test("star") {
    testConversion(UnresolvedStar(None), analysis.UnresolvedStar(None))
    testConversion(
      UnresolvedStar(Option("x.y.z.*")),
      analysis.UnresolvedStar(Option(Seq("x", "y", "z"))))
    testConversion(
      UnresolvedStar(None, Option(10L)),
      analysis.UnresolvedDataFrameStar(10L))
  }

  test("regex") {
    testConversion(
      UnresolvedRegex("`(_1)?+.+`"),
      analysis.UnresolvedRegex("(_1)?+.+", None, caseSensitive = false))

    val expression = testConversion(
      UnresolvedRegex("a", planId = Option(11L)),
      analysis.UnresolvedAttribute("a"))
    assert(expression.getTagValue(LogicalPlan.PLAN_ID_TAG).contains(11L))
    assert(expression.getTagValue(LogicalPlan.IS_METADATA_COL).isEmpty)
  }

  test("function") {
    testConversion(
      UnresolvedFunction("+", Seq(UnresolvedAttribute("a"), Literal(1))),
      analysis.UnresolvedFunction(
        Seq("+"),
        Seq(analysis.UnresolvedAttribute("a"), expressions.Literal(1)),
        isDistinct = false))
    testConversion(
      UnresolvedFunction(
        "db1.myAgg",
        Seq(UnresolvedAttribute("a")),
        isDistinct = true,
        isUserDefinedFunction = true),
      analysis.UnresolvedFunction(
        Seq("db1", "myAgg"),
        Seq(analysis.UnresolvedAttribute("a")),
        isDistinct = true))
  }

  test("alias") {
    testConversion(
      Alias(Literal("qwe"), "newA" :: Nil),
      expressions.Alias(expressions.Literal("qwe"), "newA")(
        nonInheritableMetadataKeys = Seq(Dataset.DATASET_ID_KEY, Dataset.COL_POS_KEY)))
    val metadata = new MetadataBuilder().putLong("q", 10).build()
    testConversion(
      Alias(UnresolvedAttribute("a"), "b" :: Nil, Option(metadata)),
      expressions.Alias(analysis.UnresolvedAttribute("a"), "b")(
        explicitMetadata = Option(metadata)))
    testConversion(
      Alias(UnresolvedAttribute("complex"), "newA" :: "newB" :: Nil),
      analysis.MultiAlias(analysis.UnresolvedAttribute("complex"), Seq("newA", "newB")))
  }

  private def testCast(
      dataType: DataType,
      colEvalMode: Cast.EvalMode,
      catEvalMode: expressions.EvalMode.Value): Unit = {
    testConversion(
      Cast(UnresolvedAttribute("attr"), dataType, Option(colEvalMode)),
      expressions.Cast(analysis.UnresolvedAttribute("attr"), dataType, evalMode = catEvalMode))
  }

  test("cast") {
    testConversion(
      Cast(UnresolvedAttribute("str"), DoubleType),
      expressions.Cast(analysis.UnresolvedAttribute("str"), DoubleType))

    testCast(LongType, Cast.Legacy, expressions.EvalMode.LEGACY)
    testCast(BinaryType, Cast.Try, expressions.EvalMode.TRY)
    testCast(ShortType, Cast.Ansi, expressions.EvalMode.ANSI)
  }

  private def testSortOrder(
      colDirection: SortOrder.SortDirection,
      colNullOrdering: SortOrder.NullOrdering,
      catDirection: expressions.SortDirection,
      catNullOrdering: expressions.NullOrdering): Unit = {
    testConversion(
      SortOrder(UnresolvedAttribute("unsorted"), colDirection, colNullOrdering),
      new expressions.SortOrder(
        analysis.UnresolvedAttribute("unsorted"),
        catDirection,
        catNullOrdering,
        Nil))
  }

  test("sortOrder") {
    testSortOrder(
      SortOrder.Ascending,
      SortOrder.NullsFirst,
      expressions.Ascending,
      expressions.NullsFirst)
    testSortOrder(
      SortOrder.Ascending,
      SortOrder.NullsLast,
      expressions.Ascending,
      expressions.NullsLast)
    testSortOrder(
      SortOrder.Descending,
      SortOrder.NullsFirst,
      expressions.Descending,
      expressions.NullsFirst)
    testSortOrder(
      SortOrder.Descending,
      SortOrder.NullsLast,
      expressions.Descending,
      expressions.NullsLast)
  }

  private def testWindowFrame(
      colFrameType: WindowFrame.FrameType,
      colLower: WindowFrame.FrameBoundary,
      colUpper: WindowFrame.FrameBoundary,
      catFrameType: expressions.FrameType,
      catLower: Expression,
      catUpper: Expression): Unit = {
    testConversion(
      Window(
        UnresolvedFunction("sum", Seq(UnresolvedAttribute("a"))),
        WindowSpec(
          Seq(UnresolvedAttribute("b"), UnresolvedAttribute("c")),
          Seq(SortOrder(
            UnresolvedAttribute("d"),
            SortOrder.Descending,
            SortOrder.NullsLast)),
          Option(WindowFrame(colFrameType, colLower, colUpper)))),
      expressions.WindowExpression(
        analysis.UnresolvedFunction(
          "sum",
          Seq(analysis.UnresolvedAttribute("a")),
          isDistinct = false),
        expressions.WindowSpecDefinition(
          Seq(analysis.UnresolvedAttribute("b"), analysis.UnresolvedAttribute("c")),
          Seq(expressions.SortOrder(
            analysis.UnresolvedAttribute("d"),
            expressions.Descending,
            expressions.NullsLast,
            Nil)),
          expressions.SpecifiedWindowFrame(catFrameType, catLower, catUpper))))
  }

  test("window") {
    testConversion(
      Window(
        UnresolvedFunction("sum", Seq(UnresolvedAttribute("a"))),
        WindowSpec(
          Seq(UnresolvedAttribute("b"), UnresolvedAttribute("c")),
          Nil,
          None)),
      expressions.WindowExpression(
        analysis.UnresolvedFunction(
          "sum",
          Seq(analysis.UnresolvedAttribute("a")),
          isDistinct = false),
        expressions.WindowSpecDefinition(
          Seq(analysis.UnresolvedAttribute("b"), analysis.UnresolvedAttribute("c")),
          Nil,
          expressions.UnspecifiedFrame)))
    testWindowFrame(
      WindowFrame.Row,
      WindowFrame.Value(Literal(-10)),
      WindowFrame.UnboundedFollowing,
      expressions.RowFrame,
      expressions.Literal(-10),
      expressions.UnboundedFollowing)
    testWindowFrame(
      WindowFrame.Range,
      WindowFrame.UnboundedPreceding,
      WindowFrame.CurrentRow,
      expressions.RangeFrame,
      expressions.UnboundedPreceding,
      expressions.CurrentRow)
  }

  test("lambda") {
    val colX = UnresolvedNamedLambdaVariable("x")
    val catX = expressions.UnresolvedNamedLambdaVariable(Seq("x"))
    testConversion(
      LambdaFunction(UnresolvedFunction("+", Seq(colX, UnresolvedAttribute("y"))), Seq(colX)),
      expressions.LambdaFunction(
        analysis.UnresolvedFunction(
          "+",
          Seq(catX, analysis.UnresolvedAttribute("y")),
          isDistinct = false),
        Seq(catX)))
  }

  test("sql") {
    // Direct comparison because Origin is a bit messed up.
    assert(Converter(SqlExpression("1 + 1")) == Converter.parser.parseExpression("1 + 1"))
  }

  test("caseWhen") {
    testConversion(
      CaseWhenOtherwise(
        Seq(UnresolvedAttribute("c1") -> Literal("r1")),
        Option(Literal("fallback"))),
      expressions.CaseWhen(
        Seq(analysis.UnresolvedAttribute("c1") -> expressions.Literal("r1")),
        Option(expressions.Literal("fallback")))
    )
  }

  test("extract field") {
    testConversion(
      UnresolvedExtractValue(UnresolvedAttribute("struct"), Literal("cl_a")),
      analysis.UnresolvedExtractValue(
        analysis.UnresolvedAttribute("struct"),
        expressions.Literal("cl_a")))
  }

  test("update field") {
    testConversion(
      UpdateFields(UnresolvedAttribute("struct"), "col_b", Option(Literal("cl_a"))),
      expressions.UpdateFields(
        analysis.UnresolvedAttribute("struct"),
        Seq(expressions.WithField("col_b", expressions.Literal("cl_a")))))

    testConversion(
      UpdateFields(UnresolvedAttribute("struct"), "col_c", None),
      expressions.UpdateFields(
        analysis.UnresolvedAttribute("struct"),
        Seq(expressions.DropField("col_c"))))
  }

  private def toAny(a: AgnosticEncoder[_]): AgnosticEncoder[Any] =
    a.asInstanceOf[AgnosticEncoder[Any]]

  test("udf") {
    val int2LongSum = new TypedSumLong[Int]((i: Int) => i.toLong)
    val bufferEncoder = encoderFor(int2LongSum.bufferEncoder)
    val outputEncoder = encoderFor(int2LongSum.outputEncoder)
    val bufferAttrs = bufferEncoder.namedExpressions.map {
      _.toAttribute.asInstanceOf[expressions.AttributeReference]
    }

    // Aggregator applied on the entire Dataset.
    testConversion(
      InvokeInlineUserDefinedFunction(int2LongSum, Nil),
      aggregate.SimpleTypedAggregateExpression(
        aggregator = int2LongSum.asInstanceOf[Aggregator[Any, Any, Any]],
        inputDeserializer = None,
        inputClass = None,
        inputSchema = None,
        bufferSerializer = bufferEncoder.namedExpressions,
        aggBufferAttributes = bufferAttrs,
        bufferDeserializer = analysis.UnresolvedDeserializer(
          bufferEncoder.deserializer,
          bufferAttrs),
        outputSerializer = outputEncoder.serializer,
        outputExternalType = LongType,
        dataType = LongType,
        nullable = false
      ).toAggregateExpression())

    // Aggregator applied on an input.
    testConversion(
      InvokeInlineUserDefinedFunction(
        UserDefinedAggregator(
          aggregator = int2LongSum,
          inputEncoder = PrimitiveIntEncoder,
          nullable = false,
          givenName = Option("int2LongSum")),
        UnresolvedAttribute("i_col") :: Nil),
      aggregate.ScalaAggregator(
        children = analysis.UnresolvedAttribute("i_col") :: Nil,
        agg = int2LongSum,
        inputEncoder = encoderFor(PrimitiveIntEncoder),
        bufferEncoder = encoderFor(PrimitiveLongEncoder),
        nullable = false,
        aggregatorName = Option("int2LongSum")).toAggregateExpression())

    // Regular function
    val concat = (a: String, b: String) => a + b
    testConversion(
      InvokeInlineUserDefinedFunction(
        SparkUserDefinedFunction(
          f = concat,
          inputEncoders = None :: Option(toAny(StringEncoder)) :: Nil,
          outputEncoder = Option(toAny(StringEncoder)),
          dataType = StringType,
          nullable = false,
          deterministic = false),
        Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"))),
      expressions.ScalaUDF(
        function = concat,
        dataType = StringType,
        children = Seq(analysis.UnresolvedAttribute("a"), analysis.UnresolvedAttribute("b")),
        inputEncoders = Seq(None, Option(encoderFor(StringEncoder))),
        outputEncoder = Option(encoderFor(StringEncoder)),
        udfName = None,
        nullable = false,
        udfDeterministic = false))
  }

  test("extension") {
    testConversion(
      ExpressionColumnNode(analysis.UnresolvedAttribute("bar")),
      analysis.UnresolvedAttribute("bar"))
  }

  test("unsupported") {
    intercept[SparkException](Converter(Nope()))
  }
}

private[internal] case class Nope(override val origin: Origin = CurrentOrigin.get)
  extends ColumnNode {
  override private[internal] def normalize(): Nope = this
  override def sql: String = "nope"
}
