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
    val expression: FunctionTableSubqueryArgumentExpression,
    private val sparkSession: SparkSession
)  extends UDTFArgument {
  import sparkSession.RichColumn

  def partitionBy(cols: Seq[Column]): TableArg = {
    if (expression.withSinglePartition) {
      throw new IllegalArgumentException(
        "Cannot call partitionBy() after withSinglePartition() has been called."
      )
    }
    val partitionByExpressions = cols.map(_.expr)
    new TableArg(
      expression.copy(
        partitionByExpressions = partitionByExpressions, withSinglePartition = false),
      sparkSession
    )
  }

  def orderBy(cols: Seq[Column]): TableArg = {
    val orderByExpressions = cols.map { col =>
      col.expr match {
        case sortOrder: SortOrder => sortOrder
        case expr: Expression => SortOrder(expr, Ascending)
        case other =>
          throw new IllegalArgumentException(
            s"Unsupported expression type in orderBy: ${other.getClass.getName}"
          )
      }
    }
    new TableArg(expression.copy(orderByExpressions = orderByExpressions),
      sparkSession)
  }

  def withSinglePartition(): TableArg = {
    if (expression.partitionByExpressions.nonEmpty) {
      throw new IllegalArgumentException(
        "Cannot call withSinglePartition() after partitionBy() has been called."
      )
    }
    new TableArg(
      expression.copy(partitionByExpressions = Seq.empty, withSinglePartition = true),
      sparkSession)
  }
}
