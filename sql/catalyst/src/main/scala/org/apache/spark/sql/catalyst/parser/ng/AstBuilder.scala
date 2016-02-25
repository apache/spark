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
package org.apache.spark.sql.catalyst.parser.ng

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ng.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreeNode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class AstBuilder extends SqlBaseBaseVisitor[AnyRef] {
  import AstBuilder._

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    typedVisit(ctx.statement)
  }

  override def visitSingleExpression(ctx: SingleExpressionContext): Expression = withOrigin(ctx) {
    typedVisit(ctx.namedExpression)
  }

  /* --------------------------------------------------------------------------------------------
   * Plan parsing
   * -------------------------------------------------------------------------------------------- */
  private def query(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  /* --------------------------------------------------------------------------------------------
   * Expression parsing
   * -------------------------------------------------------------------------------------------- */
  private def expression(tree: ParserRuleContext): Expression = typedVisit(tree)

  private def invertIfNotDefined(expression: Expression, not: TerminalNode): Expression = {
    if (not != null) {
      Not(expression)
    } else {
      expression
    }
  }


  override def visitNamedExpression(ctx: NamedExpressionContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    if (ctx.identifier != null) {
      Alias(e, cleanIdentifier(ctx.identifier.getText))()
    } else {
      e
    }
  }

  override def visitLogicalBinary(ctx: LogicalBinaryContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    ctx.operator.getType match {
      case SqlBaseParser.AND =>
        And(left, right)
      case SqlBaseParser.OR =>
        Or(left, right)
    }
  }

  override def visitLogicalNot(ctx: LogicalNotContext): Expression = withOrigin(ctx) {
    Not(expression(ctx.booleanExpression()))
  }

  override def visitExists(ctx: ExistsContext): Expression = notImplemented(ctx)

  override def visitComparison(ctx: ComparisonContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.value)
    val right = expression(ctx.right)
    val operator = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    operator.getSymbol.getType match {
      case SqlBaseParser.EQ =>
        EqualTo(left, right)
      case SqlBaseParser.NSEQ =>
        EqualNullSafe(left, right)
      case SqlBaseParser.NEQ =>
        Not(EqualTo(left, right))
      case SqlBaseParser.LT =>
        LessThan(left, right)
      case SqlBaseParser.LTE =>
        LessThanOrEqual(left, right)
      case SqlBaseParser.GT =>
        GreaterThan(left, right)
      case SqlBaseParser.GTE =>
        GreaterThanOrEqual(left, right)
    }
  }

  override def visitBetween(ctx: BetweenContext): Expression = withOrigin(ctx) {
    val value = expression(ctx.value)
    val between = And(
      GreaterThanOrEqual(value, expression(ctx.lower)),
      LessThanOrEqual(value, expression(ctx.upper)))
    invertIfNotDefined(between, ctx.NOT)
  }

  override def visitInList(ctx: InListContext): Expression = withOrigin(ctx) {
    val in = In(expression(ctx.value), ctx.expression().asScala.map(expression))
    invertIfNotDefined(in, ctx.NOT)
  }

  override def visitInSubquery(ctx: InSubqueryContext): Expression = notImplemented(ctx)

  override def visitLike(ctx: LikeContext): Expression = {
    val left = expression(ctx.value)
    val right = expression(ctx.pattern)
    val like = ctx.like.getType match {
      case SqlBaseParser.LIKE =>
        Like(left, right)
      case SqlBaseParser.RLIKE =>
        RLike(left, right)
    }
    invertIfNotDefined(like, ctx.NOT)
  }

  override def visitNullPredicate(ctx: NullPredicateContext): Expression = withOrigin(ctx) {
    val value = expression(ctx.value)
    if (ctx.NOT != null) {
      IsNotNull(value)
    } else {
      IsNull(value)
    }
  }

  override def visitArithmeticBinary(ctx: ArithmeticBinaryContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    ctx.operator.getType match {
      case SqlBaseParser.ASTERISK =>
        Multiply(left, right)
      case SqlBaseParser.SLASH =>
        Divide(left, right)
      case SqlBaseParser.PERCENT =>
        Remainder(left, right)
      case SqlBaseParser.DIV =>
        Cast(Divide(left, right), LongType)
      case SqlBaseParser.PLUS =>
        Add(left, right)
      case SqlBaseParser.MINUS =>
        Subtract(left, right)
      case SqlBaseParser.AMPERSAND =>
        BitwiseAnd(left, right)
      case SqlBaseParser.HAT =>
        BitwiseXor(left, right)
      case SqlBaseParser.PIPE =>
        BitwiseXor(left, right)
    }
  }

  override def visitArithmeticUnary(ctx: ArithmeticUnaryContext): Expression = withOrigin(ctx) {
    val value = expression(ctx.valueExpression)
    ctx.operator.getType match {
      case SqlBaseParser.PLUS =>
        value
      case SqlBaseParser.MINUS =>
        UnaryMinus(value)
      case SqlBaseParser.TILDE =>
        BitwiseNot(value)
    }
  }

  override def visitCast(ctx: CastContext): Expression = withOrigin(ctx) {
    Cast(expression(ctx.expression), typedVisit(ctx.dataType))
  }

  override def visitPrimitiveDatatype(ctx: PrimitiveDatatypeContext): DataType = withOrigin(ctx) {
    (ctx.identifier.getText.toLowerCase, ctx.typeParameter().asScala.toList) match {
      case ("boolean", Nil) => BooleanType
      case ("tinyint" | "byte", Nil) => ByteType
      case ("smallint" | "short", Nil) => ShortType
      case ("int" | "integer", Nil) => IntegerType
      case ("bigint" | "long", Nil) => LongType
      case ("float", Nil) => FloatType
      case ("double", Nil) => DoubleType
      case ("date", Nil) => DateType
      case ("timestamp", Nil) => TimestampType
      case ("char" | "varchar" | "string", Nil) => StringType
      case ("decimal", Nil) => DecimalType.USER_DEFAULT
      case ("decimal", precision :: Nil) => DecimalType(precision.getText.toInt, 0)
      case ("decimal", precision :: scale :: Nil) =>
        DecimalType(precision.getText.toInt, scale.getText.toInt)
      case _ => notImplemented(ctx)
    }
  }

  override def visitComplexDataType(ctx: ComplexDataTypeContext): DataType = withOrigin(ctx) {
    ctx.complex.getType match {
      case SqlBaseParser.ARRAY =>
        ArrayType(typedVisit(ctx.dataType(0)))
      case SqlBaseParser.MAP =>
        MapType(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)))
      case SqlBaseParser.STRUCT =>
        val fields = ctx.colType().asScala.map { col =>
          // Add the comment to the metadata.
          val builder = new MetadataBuilder
          if (col.STRING != null) {
            builder.putString("comment", unquote(col.STRING.getText))
          }

          StructField(
            cleanIdentifier(col.identifier.getText),
            typedVisit(col.dataType),
            nullable = true,
            builder.build())
        }
        StructType(fields)
    }
  }

  override def visitFunctionCall(ctx: FunctionCallContext): Expression = withOrigin(ctx) {
    val function = UnresolvedFunction(
      ctx.qualifiedName().getText,
      ctx.expression().asScala.map(expression),
      ctx.setQuantifier().DISTINCT() != null)

    // Check if the function is evaluated in a windowed context.
    ctx.over match {
      case spec: WindowRefContext =>
        UnresolvedWindowExpression(function, visitWindowRef(spec))
      case spec: WindowDefContext =>
        WindowExpression(function, visitWindowDef(spec))
      case _ => function
    }
  }

  override def visitWindowRef(ctx: WindowRefContext): WindowSpecReference = withOrigin(ctx) {
    WindowSpecReference(ctx.identifier.getText)
  }

  override def visitWindowDef(ctx: WindowDefContext): WindowSpecDefinition = withOrigin(ctx) {
    val spec = ctx.windowSpec

    // PARTITION BY ... ORDER BY ...
    val partition = spec.partition.asScala.map(expression)
    val order = spec.sortItem.asScala.map(visitSortItem)

    // RANGE/ROWS BETWEEN ...
    val frameSpecOption = Option(spec.windowFrame).map { frame =>
      val frameType = frame.frameType.getType match {
        case SqlBaseParser.RANGE => RangeFrame
        case SqlBaseParser.ROWS => RowFrame
      }

      SpecifiedWindowFrame(
        frameType,
        visitFrameBound(frame.start),
        Option(frame.end).map(visitFrameBound).getOrElse(CurrentRow))
    }

    WindowSpecDefinition(
      partition,
      order,
      frameSpecOption.getOrElse(UnspecifiedFrame))
  }

  override def visitFrameBound(ctx: FrameBoundContext): FrameBoundary = withOrigin(ctx) {
    // We currently only allow foldable integers.
    def value: Int = {
      val e = expression(ctx.expression)
      assert(e.foldable && e.dataType == IntegerType,
        "Frame bound value must be a constant integer.")
      e.eval().asInstanceOf[Int]
    }

    // Create the FrameBoundary
    ctx.boundType.getType match {
      case SqlBaseParser.PRECEDING if ctx.UNBOUNDED != null =>
        UnboundedPreceding
      case SqlBaseParser.PRECEDING =>
        ValuePreceding(value)
      case SqlBaseParser.CURRENT =>
        CurrentRow
      case SqlBaseParser.FOLLOWING if ctx.UNBOUNDED != null =>
        UnboundedFollowing
      case SqlBaseParser.FOLLOWING =>
        ValueFollowing(value)
    }
  }


  override def visitRowConstructor(ctx: RowConstructorContext): Expression = withOrigin(ctx) {
    CreateStruct(ctx.expression().asScala.map(expression))
  }

  override def visitArrayConstructor(ctx: ArrayConstructorContext): Expression = withOrigin(ctx) {
    CreateArray(ctx.expression().asScala.map(expression))
  }

  override def visitSubqueryExpression(
      ctx: SubqueryExpressionContext): Expression = withOrigin(ctx) {
    ScalarSubquery(query(ctx.query))
  }

  override def visitSimpleCase(ctx: SimpleCaseContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.valueExpression)
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (EqualTo(e, expression(wCtx.condition)), expression(wCtx.result))
    }
    CaseWhen(branches, Option(ctx.elseExpression).map(expression))
  }

  override def visitSearchedCase(ctx: SearchedCaseContext): Expression = withOrigin(ctx) {
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (expression(wCtx.condition), expression(wCtx.result))
    }
    CaseWhen(branches, Option(ctx.elseExpression).map(expression))
  }

  override def visitDereference(ctx: DereferenceContext): Expression = withOrigin(ctx) {
    val attr = cleanIdentifier(ctx.fieldName.getText)
    expression(ctx.base) match {
      case UnresolvedAttribute(nameParts) =>
        UnresolvedAttribute(nameParts :+ attr)
      case e =>
        UnresolvedExtractValue(e, Literal(attr))
    }
  }

  override def visitColumnReference(ctx: ColumnReferenceContext): Expression = withOrigin(ctx) {
    UnresolvedAttribute.quoted(cleanIdentifier(ctx.getText))
  }

  override def visitSubscript(ctx: SubscriptContext): Expression = withOrigin(ctx) {
    UnresolvedExtractValue(expression(ctx.value), expression(ctx.index))
  }

  override def visitStar(ctx: StarContext): Expression = withOrigin(ctx) {
    val target = Option(ctx.identifier()).map(_.asScala.map {
      lCtx => cleanIdentifier(lCtx.getText)
    })
    UnresolvedStar(target)
  }

  override def visitSortItem(ctx: SortItemContext): SortOrder = withOrigin(ctx) {
    if (ctx.DESC != null) {
      SortOrder(expression(ctx.expression), Descending)
    } else {
      SortOrder(expression(ctx.expression), Ascending)
    }
  }

  override def visitNullLiteral(ctx: NullLiteralContext): Literal = withOrigin(ctx) {
    Literal(null)
  }

  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal = withOrigin(ctx) {
   Literal(ctx.getText.toBoolean)
  }

  override def visitIntegerLiteral(ctx: IntegerLiteralContext): TreeNode[_] = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue())
      case v if v.isValidLong =>
        Literal(v.longValue())
      case v => Literal(v.underlying())
    }
  }

  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = withOrigin(ctx) {
    Literal(ctx.getText.toByte)
  }

  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = withOrigin(ctx) {
    Literal(ctx.getText.toShort)
  }

  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = withOrigin(ctx) {
    Literal(ctx.getText.toLong)
  }

  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = withOrigin(ctx) {
    Literal(ctx.getText.toDouble)
  }

  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal(UTF8String.fromString(unquote(ctx.getText)))
  }

  override def visitDtsIntervalLiteral(ctx: DtsIntervalLiteralContext): Literal = withOrigin(ctx) {
   Literal(CalendarInterval.fromDayTimeString(unquote(ctx.value.getText)))
  }

  override def visitYtmIntervalLiteral(ctx: YtmIntervalLiteralContext): Literal = withOrigin(ctx) {
    Literal(CalendarInterval.fromYearMonthString(unquote(ctx.value.getText)))
  }

  override def visitComposedIntervalLiteral(
      ctx: ComposedIntervalLiteralContext): Literal = withOrigin(ctx) {
    val intervals = ctx.intervalField().asScala.map { pCtx =>
      CalendarInterval.fromSingleUnitString(pCtx.unit.getText, unquote(pCtx.value.getText))
    }
    assert(intervals.nonEmpty, "Interval should contain at least one or more value and unit pairs")
    Literal(intervals.reduce(_.add(_)))
  }
}

private[spark] object AstBuilder {

  def cleanIdentifier(raw: String): String = {
    var cleaned = raw
    val last = raw.length - 1
    if (last >= 1) {
      if (raw(0) == '`' && raw(last) == '`') {
        cleaned = raw.substring(1, last).replace("``", "`")
      }
    }
    cleaned
  }

  def unquote(raw: String): String = {
    var unquoted = raw
    val last = raw.length - 1
    if (last >= 1) {
      unquoted = raw(0) match {
        case '\'' if raw(last) == '\'' =>
          raw.substring(1, last).replace("''", "'")
        case '"' if raw(last) == '"' =>
          raw.substring(1, last).replace("\"\"", "\"")
        case _ =>
          raw
      }
    }
    unquoted
  }

  def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    val token = ctx.getStart
    CurrentOrigin.setPosition(token.getLine, token.getCharPositionInLine)
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  def notImplemented(ctx: ParserRuleContext): Nothing = {
    throw new UnsupportedOperationException(ctx.toString)
  }
}