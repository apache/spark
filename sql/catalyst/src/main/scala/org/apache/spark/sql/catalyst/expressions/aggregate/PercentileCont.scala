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

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, RuntimeReplaceableAggregate}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.AbstractDataType

/**
 * Return a percentile value based on a continuous distribution of
 * the input column (specified in ORDER BY clause).
 */
@ExpressionDescription(
  usage = """
    _FUNC_(col, percentage) - Returns a percentile value based on a continuous distribution of
     numeric or ansi interval column `col` at the given percentage. The value of percentage must be
     between 0.0 and 1.0.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, 0.3) FROM VALUES (0), (10) AS tab(col);
       3.0
      > SELECT _FUNC_(col, 0.5) FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS tab(col);
       5.0
  """,
  group = "agg_funcs",
  since = "3.3.0")
case class PercentileCont(left: Expression, right: Expression)
  extends AggregateFunction
  with RuntimeReplaceableAggregate
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {
  private lazy val percentile = new Percentile(left, right)
  override def replacement: Expression = percentile
  override def nodeName: String = "percentile_cont"
  override def inputTypes: Seq[AbstractDataType] = percentile.inputTypes
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): PercentileCont =
    this.copy(left = newLeft, right = newRight)
}
