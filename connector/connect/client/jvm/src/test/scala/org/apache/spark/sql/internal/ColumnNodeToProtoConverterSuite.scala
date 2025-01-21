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

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Expression.Window.WindowFrame.FrameBoundary
import org.apache.spark.sql.{Column, Encoder}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{PrimitiveIntEncoder, PrimitiveLongEncoder}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, ProtoDataTypes}
import org.apache.spark.sql.expressions.{Aggregator, SparkUserDefinedFunction, UserDefinedAggregator}
import org.apache.spark.sql.test.ConnectFunSuite
import org.apache.spark.sql.types.{BinaryType, DataType, DoubleType, LongType, MetadataBuilder, ShortType, StringType, StructType}

/**
 * Test suite for [[ColumnNode]] to [[proto.Expression]] conversions.
 */
class ColumnNodeToProtoConverterSuite extends ConnectFunSuite {
  private def testConversion(
      node: => ColumnNode,
      expected: proto.Expression): proto.Expression = {
    val expression = ColumnNodeToProtoConverter(node)
    assert(expression == expected)
    expression
  }

  private def expr(f: proto.Expression.Builder => Unit): proto.Expression = {
    val builder = proto.Expression.newBuilder()
    f(builder)
    builder.build()
  }

  private def attribute(name: String): proto.Expression =
    expr(_.getUnresolvedAttributeBuilder.setUnparsedIdentifier(name))

  private def structField(
      name: String,
      dataType: proto.DataType,
      nullable: Boolean = true): proto.DataType.StructField = {
    proto.DataType.StructField
      .newBuilder()
      .setName(name)
      .setDataType(dataType)
      .setNullable(nullable)
      .build()
  }

  test("literal") {
    testConversion(Literal(1), expr(_.getLiteralBuilder.setInteger(1).build()))
    testConversion(
      Literal("foo", Option(StringType)),
      expr(_.getLiteralBuilder.setString("foo").build()))
    val dataType = new StructType()
      .add("_1", DoubleType)
      .add("_2", StringType)
      .add("_3", DoubleType)
      .add("_4", StringType)
    val stringTypeWithCollation = proto.DataType
      .newBuilder()
      .setString(proto.DataType.String.newBuilder().setCollation("UTF8_BINARY"))
      .build()
    testConversion(
      Literal((12.0, "north", 60.0, "west"), Option(dataType)),
      expr { b =>
        val builder = b.getLiteralBuilder.getStructBuilder
        builder.getStructTypeBuilder.getStructBuilder
          .addFields(structField("_1", ProtoDataTypes.DoubleType))
          .addFields(structField("_2", stringTypeWithCollation))
          .addFields(structField("_3", ProtoDataTypes.DoubleType))
          .addFields(structField("_4", stringTypeWithCollation))
        builder.addElements(proto.Expression.Literal.newBuilder().setDouble(12.0))
        builder.addElements(proto.Expression.Literal.newBuilder().setString("north"))
        builder.addElements(proto.Expression.Literal.newBuilder().setDouble(60.0))
        builder.addElements(proto.Expression.Literal.newBuilder().setString("west"))
      })
  }

  test("attribute") {
    testConversion(UnresolvedAttribute("x"), attribute("x"))
    testConversion(
      UnresolvedAttribute("y", Option(44L), isMetadataColumn = true),
      expr(
        _.getUnresolvedAttributeBuilder
          .setUnparsedIdentifier("y")
          .setPlanId(44L)
          .setIsMetadataColumn(true)))
  }

  test("star") {
    testConversion(UnresolvedStar(None), expr(_.getUnresolvedStarBuilder))
    testConversion(
      UnresolvedStar(Option("x.y.z.*")),
      expr(_.getUnresolvedStarBuilder.setUnparsedTarget("x.y.z.*")))
    testConversion(
      UnresolvedStar(None, Option(10L)),
      expr(_.getUnresolvedStarBuilder.setPlanId(10L)))
  }

  test("regex") {
    testConversion(
      UnresolvedRegex("`(_1)?+.+`"),
      expr(_.getUnresolvedRegexBuilder.setColName("`(_1)?+.+`")))
    testConversion(
      UnresolvedRegex("a", planId = Option(11L)),
      expr(_.getUnresolvedRegexBuilder.setColName("a").setPlanId(11L)))
  }

  test("function") {
    testConversion(
      UnresolvedFunction("+", Seq(UnresolvedAttribute("a"), Literal(1))),
      expr(
        _.getUnresolvedFunctionBuilder
          .setFunctionName("+")
          .setIsDistinct(false)
          .addArguments(attribute("a"))
          .addArguments(expr(_.getLiteralBuilder.setInteger(1)))
          .setIsInternal(false)))
    testConversion(
      UnresolvedFunction(
        "db1.myAgg",
        Seq(UnresolvedAttribute("a")),
        isDistinct = true,
        isUserDefinedFunction = true,
        isInternal = true),
      expr(
        _.getUnresolvedFunctionBuilder
          .setFunctionName("db1.myAgg")
          .setIsDistinct(true)
          .setIsUserDefinedFunction(true)
          .addArguments(attribute("a"))
          .setIsInternal(true)))
  }

  test("alias") {
    testConversion(
      Alias(Literal("qwe"), "newA" :: Nil),
      expr(
        _.getAliasBuilder
          .setExpr(expr(_.getLiteralBuilder.setString("qwe")))
          .addName("newA")))
    val metadata = new MetadataBuilder().putLong("q", 10).build()
    testConversion(
      Alias(UnresolvedAttribute("a"), "b" :: Nil, Option(metadata)),
      expr(
        _.getAliasBuilder
          .setExpr(attribute("a"))
          .addName("b")
          .setMetadata("""{"q":10}""")))
    testConversion(
      Alias(UnresolvedAttribute("complex"), "newA" :: "newB" :: Nil),
      expr(
        _.getAliasBuilder
          .setExpr(attribute("complex"))
          .addName("newA")
          .addName("newB")))
  }

  private def testCast(
      dataType: DataType,
      colEvalMode: Cast.EvalMode,
      catEvalMode: proto.Expression.Cast.EvalMode): Unit = {
    testConversion(
      Cast(UnresolvedAttribute("attr"), dataType, Option(colEvalMode)),
      expr(
        _.getCastBuilder
          .setExpr(attribute("attr"))
          .setType(DataTypeProtoConverter.toConnectProtoType(dataType))
          .setEvalMode(catEvalMode)))
  }

  test("cast") {
    testConversion(
      Cast(UnresolvedAttribute("str"), DoubleType),
      expr(
        _.getCastBuilder
          .setExpr(attribute("str"))
          .setType(ProtoDataTypes.DoubleType)))

    testCast(LongType, Cast.Legacy, proto.Expression.Cast.EvalMode.EVAL_MODE_LEGACY)
    testCast(BinaryType, Cast.Try, proto.Expression.Cast.EvalMode.EVAL_MODE_TRY)
    testCast(ShortType, Cast.Ansi, proto.Expression.Cast.EvalMode.EVAL_MODE_ANSI)
  }

  private def testSortOrder(
      colDirection: SortOrder.SortDirection,
      colNullOrdering: SortOrder.NullOrdering,
      catDirection: proto.Expression.SortOrder.SortDirection,
      catNullOrdering: proto.Expression.SortOrder.NullOrdering): Unit = {
    testConversion(
      SortOrder(UnresolvedAttribute("unsorted"), colDirection, colNullOrdering),
      expr(
        _.getSortOrderBuilder
          .setChild(attribute("unsorted"))
          .setNullOrdering(catNullOrdering)
          .setDirection(catDirection)))
  }

  test("sortOrder") {
    testSortOrder(
      SortOrder.Ascending,
      SortOrder.NullsFirst,
      proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
      proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST)
    testSortOrder(
      SortOrder.Ascending,
      SortOrder.NullsLast,
      proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
      proto.Expression.SortOrder.NullOrdering.SORT_NULLS_LAST)
    testSortOrder(
      SortOrder.Descending,
      SortOrder.NullsFirst,
      proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING,
      proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST)
    testSortOrder(
      SortOrder.Descending,
      SortOrder.NullsLast,
      proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING,
      proto.Expression.SortOrder.NullOrdering.SORT_NULLS_LAST)
  }

  private def testWindowFrame(
      colFrameType: WindowFrame.FrameType,
      colLower: WindowFrame.FrameBoundary,
      colUpper: WindowFrame.FrameBoundary,
      catFrameType: proto.Expression.Window.WindowFrame.FrameType,
      catLower: proto.Expression.Window.WindowFrame.FrameBoundary,
      catUpper: proto.Expression.Window.WindowFrame.FrameBoundary): Unit = {
    testConversion(
      Window(
        UnresolvedFunction("sum", Seq(UnresolvedAttribute("a"))),
        WindowSpec(
          Seq(UnresolvedAttribute("b"), UnresolvedAttribute("c")),
          Seq(SortOrder(UnresolvedAttribute("d"), SortOrder.Descending, SortOrder.NullsLast)),
          Option(WindowFrame(colFrameType, colLower, colUpper)))),
      expr(
        _.getWindowBuilder
          .setWindowFunction(
            expr(
              _.getUnresolvedFunctionBuilder
                .setFunctionName("sum")
                .setIsDistinct(false)
                .addArguments(attribute("a"))
                .setIsInternal(false)))
          .addPartitionSpec(attribute("b"))
          .addPartitionSpec(attribute("c"))
          .addOrderSpec(proto.Expression.SortOrder
            .newBuilder()
            .setChild(attribute("d"))
            .setDirection(proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING)
            .setNullOrdering(proto.Expression.SortOrder.NullOrdering.SORT_NULLS_LAST))
          .getFrameSpecBuilder
          .setFrameType(catFrameType)
          .setLower(catLower)
          .setUpper(catUpper)))
  }

  test("window") {
    testConversion(
      Window(
        UnresolvedFunction("sum", Seq(UnresolvedAttribute("a"))),
        WindowSpec(Seq(UnresolvedAttribute("b"), UnresolvedAttribute("c")), Nil, None)),
      expr(
        _.getWindowBuilder
          .setWindowFunction(
            expr(
              _.getUnresolvedFunctionBuilder
                .setFunctionName("sum")
                .setIsDistinct(false)
                .addArguments(attribute("a"))
                .setIsInternal(false)))
          .addPartitionSpec(attribute("b"))
          .addPartitionSpec(attribute("c"))))
    testWindowFrame(
      WindowFrame.Row,
      WindowFrame.Value(Literal(-10)),
      WindowFrame.UnboundedFollowing,
      proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW,
      FrameBoundary.newBuilder().setValue(expr(_.getLiteralBuilder.setInteger(-10))).build(),
      FrameBoundary.newBuilder().setUnbounded(true).build())
    testWindowFrame(
      WindowFrame.Range,
      WindowFrame.UnboundedPreceding,
      WindowFrame.CurrentRow,
      proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE,
      FrameBoundary.newBuilder().setUnbounded(true).build(),
      FrameBoundary.newBuilder().setCurrentRow(true).build())
  }

  test("lambda") {
    val colX = UnresolvedNamedLambdaVariable("x")
    val catX = proto.Expression.UnresolvedNamedLambdaVariable
      .newBuilder()
      .addNameParts(colX.name)
      .build()
    testConversion(
      LambdaFunction(UnresolvedFunction("+", Seq(colX, UnresolvedAttribute("y"))), Seq(colX)),
      expr(
        _.getLambdaFunctionBuilder
          .setFunction(
            expr(
              _.getUnresolvedFunctionBuilder
                .setFunctionName("+")
                .addArguments(expr(_.setUnresolvedNamedLambdaVariable(catX)))
                .addArguments(attribute("y"))
                .setIsInternal(false)))
          .addArguments(catX)))
  }

  test("sql") {
    testConversion(
      SqlExpression("1 + 1"),
      expr(_.getExpressionStringBuilder.setExpression("1 + 1")))
  }

  test("caseWhen") {
    testConversion(
      CaseWhenOtherwise(
        Seq(UnresolvedAttribute("c1") -> Literal("r1")),
        Option(Literal("fallback"))),
      expr(
        _.getUnresolvedFunctionBuilder
          .setFunctionName("when")
          .addArguments(attribute("c1"))
          .addArguments(expr(_.getLiteralBuilder.setString("r1")))
          .addArguments(expr(_.getLiteralBuilder.setString("fallback")))
          .setIsInternal(false)))
  }

  test("extract field") {
    testConversion(
      UnresolvedExtractValue(UnresolvedAttribute("struct"), Literal("cl_a")),
      expr(
        _.getUnresolvedExtractValueBuilder
          .setChild(attribute("struct"))
          .setExtraction(expr(_.getLiteralBuilder.setString("cl_a")))))
  }

  test("update field") {
    testConversion(
      UpdateFields(UnresolvedAttribute("struct"), "col_b", Option(Literal("cl_a"))),
      expr(
        _.getUpdateFieldsBuilder
          .setStructExpression(attribute("struct"))
          .setFieldName("col_b")
          .setValueExpression(expr(_.getLiteralBuilder.setString("cl_a")))))

    testConversion(
      UpdateFields(UnresolvedAttribute("struct"), "col_c", None),
      expr(
        _.getUpdateFieldsBuilder
          .setStructExpression(attribute("struct"))
          .setFieldName("col_c")))
  }

  test("udf") {
    val fn = (i: Int) => i + 1
    val udf = SparkUserDefinedFunction(fn, PrimitiveIntEncoder :: Nil, PrimitiveIntEncoder)
    val named = udf.withName("boo").asNondeterministic()
    testConversion(
      InvokeInlineUserDefinedFunction(named, Seq(UnresolvedAttribute("a"))),
      expr(
        _.getCommonInlineUserDefinedFunctionBuilder
          .setFunctionName("boo")
          .setDeterministic(false)
          .addArguments(attribute("a"))
          .getScalarScalaUdfBuilder
          .setPayload(
            UdfToProtoUtils.toUdfPacketBytes(fn, PrimitiveIntEncoder :: Nil, PrimitiveIntEncoder))
          .addInputTypes(ProtoDataTypes.IntegerType)
          .setOutputType(ProtoDataTypes.IntegerType)
          .setNullable(false)
          .setAggregate(false)))

    val aggregator = new Aggregator[Long, Long, Long] {
      override def zero: Long = 0
      override def reduce(b: Long, a: Long): Long = a + b
      override def merge(b1: Long, b2: Long): Long = b1 + b2
      override def finish(reduction: Long): Long = reduction
      override def bufferEncoder: Encoder[Long] = PrimitiveLongEncoder
      override def outputEncoder: Encoder[Long] = PrimitiveLongEncoder
    }
    val uda = UserDefinedAggregator(aggregator, PrimitiveLongEncoder)
      .withName("lsum")
      .asNonNullable()
    testConversion(
      InvokeInlineUserDefinedFunction(uda, Seq(UnresolvedAttribute(("a")))),
      expr(
        _.getCommonInlineUserDefinedFunctionBuilder
          .setFunctionName("lsum")
          .setDeterministic(true)
          .addArguments(attribute("a"))
          .getScalarScalaUdfBuilder
          .setPayload(UdfToProtoUtils
            .toUdfPacketBytes(aggregator, PrimitiveLongEncoder :: Nil, PrimitiveLongEncoder))
          .addInputTypes(ProtoDataTypes.LongType)
          .setOutputType(ProtoDataTypes.LongType)
          .setNullable(false)
          .setAggregate(true)))

    val invokeColumn = Column(InvokeInlineUserDefinedFunction(aggregator, Nil))
    val result = ColumnNodeToProtoConverter.toTypedExpr(invokeColumn, PrimitiveLongEncoder)
    val expected = expr { builder =>
      builder.getTypedAggregateExpressionBuilder.getScalarScalaUdfBuilder
        .setPayload(UdfToProtoUtils
          .toUdfPacketBytes(aggregator, PrimitiveLongEncoder :: Nil, PrimitiveLongEncoder))
        .addInputTypes(ProtoDataTypes.LongType)
        .setOutputType(ProtoDataTypes.LongType)
        .setNullable(true)
        .setAggregate(true)
      val origin = builder.getCommonBuilder.getOriginBuilder.getJvmOriginBuilder
      invokeColumn.node.origin.stackTrace.map {
        _.foreach { element =>
          origin.addStackTrace(
            proto.StackTraceElement
              .newBuilder()
              .setClassLoaderName(element.getClassLoaderName)
              .setDeclaringClass(element.getClassName)
              .setMethodName(element.getMethodName)
              .setFileName(element.getFileName)
              .setLineNumber(element.getLineNumber))
        }
      }
    }
    assert(result == expected)
  }

  test("extension") {
    val e = attribute("name")
    testConversion(ProtoColumnNode(e), e)
  }

  test("unsupported") {
    intercept[SparkException](ColumnNodeToProtoConverter(Nope()))
  }

  test("origin") {
    val origin = Origin(
      line = Some(1),
      sqlText = Some("lol"),
      stackTrace = Some(Array(new StackTraceElement("a", "b", "c", 9))))
    testConversion(
      SqlExpression("1 + 1", origin),
      expr { builder =>
        builder.getExpressionStringBuilder.setExpression("1 + 1")
        builder.getCommonBuilder.getOriginBuilder.getJvmOriginBuilder
          .setLine(1)
          .setSqlText("lol")
          .addStackTrace(
            proto.StackTraceElement
              .newBuilder()
              .setDeclaringClass("a")
              .setMethodName("b")
              .setFileName("c")
              .setLineNumber(9))
      })
  }
}

private[internal] case class Nope(override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override def sql: String = "nope"
  override private[internal] def children: Seq[ColumnNodeLike] = Seq.empty
}
