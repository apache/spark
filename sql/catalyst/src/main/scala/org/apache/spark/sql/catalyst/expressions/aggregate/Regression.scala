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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionDescription, If, ImplicitCastInputTypes, Literal, Pow}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType}

abstract class Regression(val left: Expression, val right: Expression)
  extends DeclarativeAggregate with ImplicitCastInputTypes with BinaryLike[Expression] {

  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

  protected val count = AttributeReference("count", DoubleType, nullable = false)()
  protected val meanX = AttributeReference("meanX", DoubleType, nullable = false)()
  protected val meanY = AttributeReference("meanY", DoubleType, nullable = false)()
  protected val c2 = AttributeReference("c2", DoubleType, nullable = false)()
  protected val m2X = AttributeReference("m2X", DoubleType, nullable = false)()

  override val aggBufferAttributes: Seq[AttributeReference] = Seq(count, meanX, meanY, c2, m2X)

  override val initialValues: Seq[Expression] = Array.fill(5)(Literal(0.0))

  override lazy val updateExpressions: Seq[Expression] = {
    val newCount = count + 1.0
    val oldMeanX = meanX
    val newMeanX = oldMeanX + (right - oldMeanX) / newCount
    val oldMeanY = meanY
    val newMeanY = oldMeanY + (left - oldMeanY) / newCount
    val newC2 = c2 + (right - oldMeanX) * (left - newMeanY)
    val newM2X = m2X + (right - oldMeanX) * (right - newMeanX)

    val isNull = left.isNull || right.isNull
    Seq(
      If(isNull, count, newCount),
      If(isNull, meanX, newMeanX),
      If(isNull, meanY, newMeanY),
      If(isNull, c2, newC2),
      If(isNull, m2X, newM2X)
    )
  }

  override val mergeExpressions: Seq[Expression] = {
    val count1 = count.left
    val count2 = count.right
    val newCount = count1 + count2
    val newM2X =
      m2X.left + m2X.right + count1 * count2 * Pow(meanX.left - meanX.right, 2) / newCount
    val deltaX = meanX.right - meanX.left
    val deltaY = meanY.right - meanY.left
    val newC2 = c2.left + c2.right + deltaX * deltaY * count1 * count2 / newCount
    val newMeanX = meanX.left + deltaX * count2 / newCount
    val newMeanY = meanY.left + deltaY * count2 / newCount

    Seq(newCount, newMeanX, newMeanY, newC2, newM2X)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr1, expr2) - Returns the slope of the linear regression line for non-null pairs in a group.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(c1, c2) FROM VALUES (1,1), (2,2), (3,3) AS tab(c1, c2);
       1.0
  """,
  group = "agg_funcs",
  since = "3.3.0")
case class RegrSlope(
    override val left: Expression,
    override val right: Expression) extends Regression(left, right) {

  override val evaluateExpression: Expression = {
    c2 / m2X
  }
  override def prettyName: String = "regr_slope"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrSlope =
    copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr1, expr2) - Returns the intercept of the univariate linear regression line
                           for non-null pairs in a group.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(c1, c2) FROM VALUES (1,1), (2,2), (3,3) AS tab(c1, c2);
       0.0
  """,
  group = "agg_funcs",
  since = "3.3.0")
case class RegrIntercept(
    override val left: Expression,
    override val right: Expression) extends Regression(left, right) {

  override val evaluateExpression: Expression = {
    val slope = c2 / m2X
    meanY - slope * meanX
  }
  override def prettyName: String = "regr_intercept"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrIntercept =
    copy(left = newLeft, right = newRight)
}
