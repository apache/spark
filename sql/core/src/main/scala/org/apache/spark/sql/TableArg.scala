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
    sparkSession: classic.SparkSession)
  extends TableValuedFunctionArgument {
  import sparkSession.toRichColumn

  private def isPartitioned: Boolean =
    expression.partitionByExpressions.nonEmpty || expression.withSinglePartition

  @scala.annotation.varargs
  def partitionBy(cols: Column*): TableArg = {
    if (isPartitioned) {
      throw new IllegalArgumentException(
        "Cannot call partitionBy() after partitionBy() or withSinglePartition() has been called."
      )
    }
    val partitionByExpressions = cols.map(_.expr)
    new TableArg(
      expression.copy(
        partitionByExpressions = partitionByExpressions),
        sparkSession)
  }

  @scala.annotation.varargs
  def orderBy(cols: Column*): TableArg = {
    if (!isPartitioned) {
      throw new IllegalArgumentException(
        "Please call partitionBy() or withSinglePartition() before orderBy()."
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
      sparkSession)
  }

  def withSinglePartition(): TableArg = {
    if (isPartitioned) {
      throw new IllegalArgumentException(
        "Cannot call withSinglePartition() after partitionBy() or " +
          "withSinglePartition() has been called."
      )
    }
    new TableArg(
      expression.copy(withSinglePartition = true),
      sparkSession)
  }
}
