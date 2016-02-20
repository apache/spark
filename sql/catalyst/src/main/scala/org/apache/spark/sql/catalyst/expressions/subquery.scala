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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.types.DataType

/**
 * An interface for subquery that is used in expressions.
 */
abstract class SubqueryExpression extends LeafExpression{

  /**
   * The logical plan of the query.
   */
  def query: LogicalPlan

  /**
   * The underline plan for the query, could be logical plan or physical plan.
   *
   * This is used to generate tree string.
   */
  def plan: QueryPlan[_]

  /**
   * Updates the query with new logical plan.
   */
  def withNewPlan(plan: LogicalPlan): SubqueryExpression
}

/**
 * A subquery that will return only one row and one column.
 *
 * This is not evaluable, it should be converted into SparkScalaSubquery.
 */
case class ScalarSubquery(
    query: LogicalPlan,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression with Unevaluable {

  override def plan: LogicalPlan = Subquery(toString, query)

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

  override def foldable: Boolean = false
  override def nullable: Boolean = true

  override def withNewPlan(plan: LogicalPlan): ScalarSubquery = ScalarSubquery(plan, exprId)

  override def toString: String = s"subquery#${exprId.id}"

  // TODO: support sql()
}
