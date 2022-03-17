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

import org.apache.spark.sql.catalyst.expressions.{And, Expression, ExpressionDescription, If, ImplicitCastInputTypes, IsNotNull, Literal, RuntimeReplaceableAggregate}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, NumericType}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the number of non-null number pairs in a group.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       4
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       3
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       2
  """,
  group = "agg_funcs",
  since = "3.3.0")
case class RegrCount(left: Expression, right: Expression)
  extends AggregateFunction
  with RuntimeReplaceableAggregate
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {
  override lazy val replacement: Expression = Count(Seq(left, right))
  override def nodeName: String = "regr_count"
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, NumericType)
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrCount =
    this.copy(left = newLeft, right = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the average of the independent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.",
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       2.75
      > SELECT _FUNC_(y, x) FROM VALUES (1, null) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (null, 1) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       3.0
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       3.0
  """,
  group = "agg_funcs",
  since = "3.3.0")
// scalastyle:on line.size.limit
case class RegrAvgX(
    left: Expression,
    right: Expression)
  extends AggregateFunction
  with RuntimeReplaceableAggregate
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {
  override lazy val replacement: Expression =
    Average(If(And(IsNotNull(left), IsNotNull(right)), right, Literal.create(null, right.dataType)))
  override def nodeName: String = "regr_avgx"
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, NumericType)
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrAvgX =
    this.copy(left = newLeft, right = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(y, x) - Returns the average of the dependent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.",
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       1.75
      > SELECT _FUNC_(y, x) FROM VALUES (1, null) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (null, 1) AS tab(y, x);
       NULL
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       1.6666666666666667
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       1.5
  """,
  group = "agg_funcs",
  since = "3.3.0")
// scalastyle:on line.size.limit
case class RegrAvgY(
    left: Expression,
    right: Expression)
  extends AggregateFunction
  with RuntimeReplaceableAggregate
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {
  override lazy val replacement: Expression =
    Average(If(And(IsNotNull(left), IsNotNull(right)), left, Literal.create(null, left.dataType)))
  override def nodeName: String = "regr_avgy"
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, NumericType)
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrAvgY =
    this.copy(left = newLeft, right = newRight)
}
