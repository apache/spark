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
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, UnevaluableAggregateFunc}
import org.apache.spark.sql.catalyst.trees.TreePattern.{MEASURE, TreePattern}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, DataType}

// This function serves as an annotation to tell the analyzer to calculate
// the measures defined in metric views. It cannot be evaluated in execution phase
// and instead it'll be replaced to the actual aggregate functions defined by
// the measure (as input argument).
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - this function is used and can only be used to calculate a measure defined in a metric view.",
  examples = """
    Examples:
      > SELECT dimension_col, _FUNC_(measure_col)
        FROM test_metric_view
        GROUP BY dimension_col;
      dim_1, 100
      dim_2, 200
  """,
  group = "agg_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class Measure(child: Expression)
  extends UnevaluableAggregateFunc with ExpectsInputTypes
    with UnaryLike[Expression] {

  override protected def withNewChildInternal(newChild: Expression): Measure =
    copy(child = newChild)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def dataType: DataType = child.dataType

  override def prettyName: String = getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("measure")

  override def nullable: Boolean = child.nullable

  override val nodePatterns: Seq[TreePattern] = Seq(MEASURE)
}
