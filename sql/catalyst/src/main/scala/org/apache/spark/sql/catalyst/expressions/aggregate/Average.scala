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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, FunctionRegistry, TypeCheckResult}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.{SQLQueryContext, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{AVERAGE, TreePattern}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the mean calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (3) AS tab(col);
       2.0
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (NULL) AS tab(col);
       1.5
  """,
  group = "agg_funcs",
  since = "1.0.0")
case class Average(
    child: Expression,
    evalMode: EvalMode.Value = EvalMode.fromSQLConf(SQLConf.get))
  extends DeclarativeAggregate
  with ImplicitCastInputTypes
  with SupportQueryContext
  with UnaryLike[Expression] {

  def this(child: Expression) = this(child, EvalMode.fromSQLConf(SQLConf.get))

  override def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("avg")

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(NumericType, YearMonthIntervalType, DayTimeIntervalType))

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForAnsiIntervalOrNumericType(child)

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  final override val nodePatterns: Seq[TreePattern] = Seq(AVERAGE)

  protected lazy val resultType = child.dataType match {
    case DecimalType.Fixed(p, s) =>
      DecimalType.bounded(p + 4, s + 4)
    case _: YearMonthIntervalType => YearMonthIntervalType()
    case _: DayTimeIntervalType => DayTimeIntervalType()
    case _ => DoubleType
  }

  lazy val sumDataType = child.dataType match {
    case _ @ DecimalType.Fixed(p, s) => DecimalType.bounded(p + 10, s)
    case _: YearMonthIntervalType => YearMonthIntervalType()
    case _: DayTimeIntervalType => DayTimeIntervalType()
    case _ => DoubleType
  }

  lazy val sum = AttributeReference("sum", sumDataType)()
  lazy val count = AttributeReference("count", LongType)()

  protected def add(left: Expression, right: Expression): Expression = left.dataType match {
    case _: DecimalType => DecimalAddNoOverflowCheck(left, right, left.dataType)
    case _ => Add(left, right, evalMode)
  }

  override lazy val aggBufferAttributes = sum :: count :: Nil

  override lazy val initialValues = Seq(
    /* sum = */ Literal.default(sumDataType),
    /* count = */ Literal(0L)
  )

  override lazy val mergeExpressions = Seq(
    /* sum = */ add(sum.left, sum.right),
    /* count = */ count.left + count.right
  )

  // If all input are nulls, count will be 0 and we will get null after the division.
  // We can't directly use `/` as it throws an exception under ansi mode.
  override lazy val evaluateExpression = child.dataType match {
    case _: DecimalType =>
      If(EqualTo(count, Literal(0L)),
        Literal(null, resultType),
        DecimalDivideWithOverflowCheck(
          sum,
          count.cast(DecimalType.LongDecimal),
          resultType.asInstanceOf[DecimalType],
          getContextOrNull(),
          evalMode != EvalMode.ANSI))
    case _: YearMonthIntervalType =>
      If(EqualTo(count, Literal(0L)),
        Literal(null, YearMonthIntervalType()), DivideYMInterval(sum, count))
    case _: DayTimeIntervalType =>
      If(EqualTo(count, Literal(0L)),
        Literal(null, DayTimeIntervalType()), DivideDTInterval(sum, count))
    case _ =>
      Divide(sum.cast(resultType), count.cast(resultType), EvalMode.LEGACY)
  }

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* sum = */
    add(
      sum,
      coalesce(child.cast(sumDataType), Literal.default(sumDataType))),
    /* count = */ If(child.isNull, count, count + 1L)
  )

  // The flag `useAnsiAdd` won't be shown in the `toString` or `toAggString` methods
  override def flatArguments: Iterator[Any] = Iterator(child)

  override protected def withNewChildInternal(newChild: Expression): Average =
    copy(child = newChild)

  override def initQueryContext(): Option[SQLQueryContext] = if (evalMode == EvalMode.ANSI) {
    Some(origin.context)
  } else {
    None
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the mean calculated from values of a group and the result is null on overflow.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (3) AS tab(col);
       2.0
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (NULL) AS tab(col);
       1.5
      > SELECT _FUNC_(col) FROM VALUES (interval '2147483647 months'), (interval '1 months') AS tab(col);
       NULL
  """,
  group = "agg_funcs",
  since = "3.3.0")
// scalastyle:on line.size.limit
object TryAverageExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 1) {
      Average(expressions.head, EvalMode.TRY)
    } else {
      throw QueryCompilationErrors.invalidFunctionArgumentNumberError(Seq(1, 2), funcName, numArgs)
    }
  }
}
