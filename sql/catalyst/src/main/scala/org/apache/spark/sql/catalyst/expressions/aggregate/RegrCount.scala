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
import org.apache.spark.sql.catalyst.trees.TreePattern.{REGR_COUNT, TreePattern}
import org.apache.spark.sql.types.{AbstractDataType, DataType, LongType, NumericType}

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
  extends UnevaluableAggregate with ImplicitCastInputTypes with BinaryLike[Expression] {

  override def prettyName: String = "regr_count"

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, NumericType)

  final override val nodePatterns: Seq[TreePattern] = Seq(REGR_COUNT)

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrCount =
    this.copy(left = newLeft, right = newRight)
}
