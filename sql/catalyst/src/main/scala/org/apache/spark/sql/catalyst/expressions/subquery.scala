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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.DataType

/**
  * A interface for subquery that is used in expressions.
  */
trait SubqueryExpression extends LeafExpression {
  def query: LogicalPlan
  def withNewPlan(plan: LogicalPlan): SubqueryExpression
}

/**
  * A subquery that will return only one row and one column.
  */
case class ScalarSubquery(query: LogicalPlan) extends SubqueryExpression with CodegenFallback {

  override lazy val resolved: Boolean = query.resolved

  override def dataType: DataType = query.schema.fields.head.dataType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (query.schema.length != 1) {
      TypeCheckResult.TypeCheckFailure("Scalar subquery can only have 1 column, but got " +
        query.schema.length.toString)
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  // It can not be evaluated by optimizer.
  override def foldable: Boolean = false
  override def nullable: Boolean = true

  override def withNewPlan(plan: LogicalPlan): ScalarSubquery = ScalarSubquery(plan)

  // TODO: support sql()

  // the first column in first row from `query`.
  private var result: Any = null

  def updateResult(v: Any): Unit = {
    result = v
  }

  override def eval(input: InternalRow): Any = result
}
