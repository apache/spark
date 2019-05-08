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

@ExpressionDescription(
  usage = "_FUNC_(x, y) - Returns the value of `x` associated with the maximum value of `y`.",
  examples = """
    Examples:
      > SELECT _FUNC_(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', 20)) AS tab(x, y);
       b
  """,
  since = "3.0")
case class MaxBy(valueExpr: Expression, maxExpr: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = valueExpr :: maxExpr :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = valueExpr.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(maxExpr.dataType, "function max_by")

  private lazy val max = AttributeReference("max", maxExpr.dataType)()
  private lazy val value = AttributeReference("value", valueExpr.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = value :: max :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* value = */ Literal.create(null, valueExpr.dataType),
    /* max = */ Literal.create(null, maxExpr.dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* value = */
    CaseWhen(
      (And(IsNull(max), IsNull(maxExpr)), Literal.create(null, valueExpr.dataType)) ::
        (IsNull(max), valueExpr) ::
        (IsNull(maxExpr), value) :: Nil,
      If(GreaterThan(max, maxExpr), value, valueExpr)
    ),
    /* max = */ greatest(max, maxExpr)
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* value = */
      CaseWhen(
        (And(IsNull(max.left), IsNull(max.right)), Literal.create(null, valueExpr.dataType)) ::
          (IsNull(max.left), value.right) ::
          (IsNull(max.right), value.left) :: Nil,
        If(GreaterThan(max.left, max.right), value.left, value.right)
      ),
      /* max = */ greatest(max.left, max.right)
    )
  }

  override lazy val evaluateExpression: AttributeReference = value
}
