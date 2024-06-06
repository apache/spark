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

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types._

/**
 * Returns the first value of `child` for a group of rows. If the first value of `child`
 * is `null`, it returns `null` (respecting nulls). Even if [[AnyValue]] is used on an already
 * sorted column, if we do partial aggregation and final aggregation (when mergeExpression
 * is used) its result will not be deterministic (unless the input table is sorted and has
 * a single partition, and we use a single reducer to do the aggregation.).
 * Interchangeable with [[First]].
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, isIgnoreNull]) - Returns some value of `expr` for a group of rows.
      If `isIgnoreNull` is true, returns only non-null values.""",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (10), (5), (20) AS tab(col);
       10
      > SELECT _FUNC_(col) FROM VALUES (NULL), (5), (20) AS tab(col);
       NULL
      > SELECT _FUNC_(col, true) FROM VALUES (NULL), (5), (20) AS tab(col);
       5
  """,
  note = """
    The function is non-deterministic.
  """,
  group = "agg_funcs",
  since = "3.4.0")
case class AnyValue(child: Expression, ignoreNulls: Boolean)
  extends AggregateFunction with ExpectsInputTypes with RuntimeReplaceableAggregate
    with UnaryLike[Expression] {
  override lazy val replacement: Expression = First(child, ignoreNulls)

  def this(child: Expression) = this(child, false)

  def this(child: Expression, ignoreNullsExpr: Expression) = {
    this(child, FirstLast.validateIgnoreNullExpr(ignoreNullsExpr, "any_value"))
  }

  override protected def withNewChildInternal(newChild: Expression): AnyValue =
    copy(child = newChild)
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("any_value")
}
