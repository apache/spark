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
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, UnevaluableAggregate}
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType, LongType}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the number of `TRUE` values for the expression.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col % 2 = 0) FROM VALUES (NULL), (0), (1), (2), (3) AS tab(col);
       2
      > SELECT _FUNC_(col IS NULL) FROM VALUES (NULL), (0), (1), (2), (3) AS tab(col);
       1
  """,
  group = "agg_funcs",
  since = "3.0.0")
case class CountIf(predicate: Expression) extends UnevaluableAggregate with ImplicitCastInputTypes {
  override def prettyName: String = "count_if"

  override def children: Seq[Expression] = Seq(predicate)

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override def inputTypes: Seq[AbstractDataType] = Seq(BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = predicate.dataType match {
    case BooleanType =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure(
        s"function $prettyName requires boolean type, not ${predicate.dataType.catalogString}"
      )
  }
}
