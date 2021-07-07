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

import org.apache.spark.sql.catalyst.analysis.{DecimalPrecision, FunctionRegistry, TypeCheckResult}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TreePattern.{AVERAGE, TreePattern}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.TypeUtils
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
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends DeclarativeAggregate
  with ImplicitCastInputTypes
  with UnaryLike[Expression] {

  def this(child: Expression) = this(child, failOnError = SQLConf.get.ansiEnabled)

  override def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("avg")

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(NumericType, YearMonthIntervalType, DayTimeIntervalType))

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForAnsiIntervalOrNumericType(child.dataType, "average")

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  final override val nodePatterns: Seq[TreePattern] = Seq(AVERAGE)

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(p, s) =>
      DecimalType.bounded(p + 4, s + 4)
    case _: YearMonthIntervalType => YearMonthIntervalType()
    case _: DayTimeIntervalType => DayTimeIntervalType()
    case _ => DoubleType
  }

  private lazy val sumDataType = child.dataType match {
    case _ @ DecimalType.Fixed(p, s) => DecimalType.bounded(p + 10, s)
    case _: YearMonthIntervalType => YearMonthIntervalType()
    case _: DayTimeIntervalType => DayTimeIntervalType()
    case _ => DoubleType
  }

  private lazy val sum = AttributeReference("sum", sumDataType)()
  private lazy val count = AttributeReference("count", LongType)()

  override lazy val aggBufferAttributes = sum :: count :: Nil

  override lazy val initialValues = Seq(
    /* sum = */ Literal.default(sumDataType),
    /* count = */ Literal(0L)
  )

  override lazy val mergeExpressions = Seq(
    /* sum = */ sum.left + sum.right,
    /* count = */ count.left + count.right
  )

  // If all input are nulls, count will be 0 and we will get null after the division.
  // We can't directly use `/` as it throws an exception under ansi mode.
  override lazy val evaluateExpression = child.dataType match {
    case d: DecimalType =>
      DecimalPrecision.decimalAndDecimal()(
        Divide(
          CheckOverflowInSum(sum, d, !failOnError),
          count.cast(DecimalType.LongDecimal), failOnError = false)).cast(resultType)
    case _: YearMonthIntervalType =>
      If(EqualTo(count, Literal(0L)),
        Literal(null, YearMonthIntervalType()), DivideYMInterval(sum, count))
    case _: DayTimeIntervalType =>
      If(EqualTo(count, Literal(0L)),
        Literal(null, DayTimeIntervalType()), DivideDTInterval(sum, count))
    case _ =>
      Divide(sum.cast(resultType), count.cast(resultType), failOnError = false)
  }

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* sum = */
    Add(
      sum,
      coalesce(child.cast(sumDataType), Literal.default(sumDataType))),
    /* count = */ If(child.isNull, count, count + 1L)
  )

  override protected def withNewChildInternal(newChild: Expression): Average =
    copy(child = newChild)

  // The flag `failOnError` won't be shown in the `toString` or `toAggString` methods
  override def flatArguments: Iterator[Any] = Iterator(child)
}
