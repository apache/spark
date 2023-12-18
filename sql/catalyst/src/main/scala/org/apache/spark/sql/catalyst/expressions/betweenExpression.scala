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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.{BooleanType, DataType}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "BETWEEN(projection, lower, upper) - Returns true if `projection` > `lower` and `projection` <  `upper`.",
  examples = """
    Examples:
      > SELECT _FUNC_(0.5, 0, 1);
        TRUE
  """,
  since = "2.0.0",
  group = "conditional_funcs")
case class BetweenExpr(proj: Expression, lower: Expression, upper: Expression)
  extends RuntimeReplaceable with ComplexTypeMergingExpression {
  override lazy val replacement: Expression = {
    val commonExpr = CommonExpressionDef(proj)
    val ref = new CommonExpressionRef(commonExpr)
    With(And(
      GreaterThanOrEqual(ref, lower),
      LessThanOrEqual(ref, upper)),
      Seq(commonExpr))
  }

  override def dataType: DataType = BooleanType

  override def prettyName: String = "between"

  override def children: Seq[Expression] = Seq(proj, lower, upper)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(proj = newChildren(0), lower = newChildren(1), upper = newChildren(2))
}
