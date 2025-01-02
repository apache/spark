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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Ascending, Expression, FunctionTableSubqueryArgumentExpression, SortOrder}

class TableArg(
    private[sql] val expression: FunctionTableSubqueryArgumentExpression,
    private val sparkSession: SparkSession,
    private val isPartitioned: Boolean = false)
  extends TableValuedFunctionArgument {
  import sparkSession.toRichColumn

  @scala.annotation.varargs
  def partitionBy(cols: Column*): TableArg = {
    if (isPartitioned) {
      throw new IllegalArgumentException(
        "partitionBy() can only be specified once."
      )
    }
    if (expression.withSinglePartition) {
      throw new IllegalArgumentException(
        "Cannot call partitionBy() after withSinglePartition() has been called."
      )
    }
    val partitionByExpressions = cols.map(_.expr)
    new TableArg(
      expression.copy(
        partitionByExpressions = partitionByExpressions),
      sparkSession,
      isPartitioned = true
    )
  }

  @scala.annotation.varargs
  def orderBy(cols: Column*): TableArg = {
    // Validate that partitionBy has been called before orderBy
    if (expression.partitionByExpressions.isEmpty) {
      throw new IllegalArgumentException(
        "Please call partitionBy() before orderBy()."
      )
    }
    val orderByExpressions = cols.map { col =>
      col.expr match {
        case sortOrder: SortOrder => sortOrder
        case expr: Expression => SortOrder(expr, Ascending)
      }
    }
    new TableArg(
      expression.copy(orderByExpressions = orderByExpressions),
      sparkSession,
      isPartitioned)
  }

  def withSinglePartition(): TableArg = {
    if (expression.partitionByExpressions.nonEmpty) {
      throw new IllegalArgumentException(
        "Cannot call withSinglePartition() after partitionBy() has been called."
      )
    }
    new TableArg(
      expression.copy(withSinglePartition = true),
      sparkSession)
  }
}
