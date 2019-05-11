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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

/**
 * The shared abstract superclass for `MaxBy` and `MinBy` SQL aggregate functions.
 */
abstract class MaxMinBy extends DeclarativeAggregate {

  def valueExpr: Expression
  def orderingExpr: Expression

  protected def funcName: String
  // The predicate compares two ordering values.
  protected def predicate(oldExpr: Expression, newExpr: Expression): Expression
  // The arithmetic expression returns greatest/least value of all parameters.
  // Used to pick up updated ordering value.
  protected def orderingUpdater(oldExpr: Expression, newExpr: Expression): Expression

  override def children: Seq[Expression] = valueExpr :: orderingExpr :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = valueExpr.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(orderingExpr.dataType, s"function $funcName")

  private lazy val ordering = AttributeReference("ordering", orderingExpr.dataType)()
  private lazy val value = AttributeReference("value", valueExpr.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = value :: ordering :: Nil

  private lazy val nullValue = Literal.create(null, valueExpr.dataType)
  private lazy val nullOrdering = Literal.create(null, orderingExpr.dataType)

  override lazy val initialValues: Seq[Literal] = Seq(
    /* value = */ nullValue,
    /* ordering = */ nullOrdering
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* value = */
    CaseWhen(
      (ordering.isNull && orderingExpr.isNull, nullValue) ::
        (ordering.isNull, valueExpr) ::
        (orderingExpr.isNull, value) :: Nil,
      If(predicate(ordering, orderingExpr), value, valueExpr)
    ),
    /* ordering = */ orderingUpdater(ordering, orderingExpr)
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    /* value = */
    CaseWhen(
      (ordering.left.isNull && ordering.right.isNull, nullValue) ::
        (ordering.left.isNull, value.right) ::
        (ordering.right.isNull, value.left) :: Nil,
      If(predicate(ordering.left, ordering.right), value.left, value.right)
    ),
    /* ordering = */ orderingUpdater(ordering.left, ordering.right)
  )

  override lazy val evaluateExpression: AttributeReference = value
}

@ExpressionDescription(
  usage = "_FUNC_(x, y) - Returns the value of `x` associated with the maximum value of `y`.",
  examples = """
    Examples:
      > SELECT _FUNC_(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', 20)) AS tab(x, y);
       b
  """,
  since = "3.0")
case class MaxBy(valueExpr: Expression, orderingExpr: Expression) extends MaxMinBy {
  override protected def funcName: String = "max_by"

  override protected def predicate(oldExpr: Expression, newExpr: Expression): Expression =
    GreaterThan(oldExpr, newExpr)

  override protected def orderingUpdater(oldExpr: Expression, newExpr: Expression): Expression =
    greatest(oldExpr, newExpr)
}

@ExpressionDescription(
  usage = "_FUNC_(x, y) - Returns the value of `x` associated with the minimum value of `y`.",
  examples = """
    Examples:
      > SELECT _FUNC_(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', 20)) AS tab(x, y);
       a
  """,
  since = "3.0")
case class MinBy(valueExpr: Expression, orderingExpr: Expression) extends MaxMinBy {
  override protected def funcName: String = "min_by"

  override protected def predicate(oldExpr: Expression, newExpr: Expression): Expression =
    LessThan(oldExpr, newExpr)

  override protected def orderingUpdater(oldExpr: Expression, newExpr: Expression): Expression =
    least(oldExpr, newExpr)
}
