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

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, UnevaluableAggregate}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.trees.TreePattern.{REGR_AGG, TreePattern}
import org.apache.spark.sql.types.{AbstractDataType, DataType, DecimalType, DoubleType, LongType, NumericType}

trait RegressionAggregate
  extends UnevaluableAggregate with ImplicitCastInputTypes with BinaryLike[Expression]{

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, NumericType)

  final override val nodePatterns: Seq[TreePattern] = Seq(REGR_AGG)
}

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
case class RegrCount(left: Expression, right: Expression) extends RegressionAggregate {

  override def prettyName: String = "regr_count"

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrCount =
    this.copy(left = newLeft, right = newRight)
}

trait RegrAvg extends RegressionAggregate {

  def avgInputExpression: Expression

  override def dataType: DataType = avgInputExpression.dataType match {
    case DecimalType.Fixed(p, s) =>
      DecimalType.bounded(p + 4, s + 4)
    case _ => DoubleType
  }
}

@ExpressionDescription(
  usage = """
     _FUNC_(expr) - Returns the average of the independent variable for non-null pairs in a group.
                    right is the independent variable and left is the dependent variable.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       2.75
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       3.0
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       3.0
  """,
  group = "agg_funcs",
  since = "3.3.0")
case class RegrAvgX(left: Expression, right: Expression) extends RegrAvg {

  override def prettyName: String = "regr_avgx"

  override def avgInputExpression: Expression = right

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrAvgX =
    copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = """
     _FUNC_(expr) - Returns the average of the independent variable for non-null pairs in a group.
                    right is the independent variable and left is the dependent variable.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       1.75
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       1.6666666666666667
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       1.5
  """,
  group = "agg_funcs",
  since = "3.3.0")
case class RegrAvgY(left: Expression, right: Expression) extends RegrAvg {

  override def prettyName: String = "regr_avgy"

  override def avgInputExpression: Expression = left

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrAvgY =
    copy(left = newLeft, right = newRight)
}
