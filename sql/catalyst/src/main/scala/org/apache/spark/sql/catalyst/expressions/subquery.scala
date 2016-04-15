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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.types._

/**
 * An interface for subquery that is used in expressions.
 */
abstract class SubqueryExpression extends Expression {

  /**
   * The logical plan of the query.
   */
  def query: LogicalPlan

  /**
   * Either a logical plan or a physical plan. The generated tree string (explain output) uses this
   * field to explain the subquery.
   */
  def plan: QueryPlan[_]

  /**
   * Updates the query with new logical plan.
   */
  def withNewPlan(plan: LogicalPlan): SubqueryExpression
}

/**
 * A subquery that will return only one row and one column. This will be converted into a physical
 * scalar subquery during planning.
 *
 * Note: `exprId` is used to have unique name in explain string output.
 */
case class ScalarSubquery(
    query: LogicalPlan,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression with Unevaluable {

  override def plan: LogicalPlan = SubqueryAlias(toString, query)

  override lazy val resolved: Boolean = query.resolved

  override def dataType: DataType = query.schema.fields.head.dataType

  override def children: Seq[Expression] = Nil

  override def checkInputDataTypes(): TypeCheckResult = {
    if (query.schema.length != 1) {
      TypeCheckResult.TypeCheckFailure("Scalar subquery must return only one column, but got " +
        query.schema.length.toString)
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def foldable: Boolean = false
  override def nullable: Boolean = true

  override def withNewPlan(plan: LogicalPlan): ScalarSubquery = ScalarSubquery(plan, exprId)

  override def toString: String = s"subquery#${exprId.id}"
}

/**
 * Base interface for (potentially) correlated subquery expressions.
 */
abstract class CorrelatedSubqueryExpression extends SubqueryExpression {
  override def plan: LogicalPlan = SubqueryAlias(toString, query)
}

/**
 * The [[InSubQuery]] allows us to use a subquery as the (single) argument in an [[In]] predicate.
 * Such predicates are typically used to filter query results. For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   a.id IN (SELECT  id
 *                    FROM    b)
 * }}}
 *
 * Currently we only allow [[InSubQuery]] expressions within a Filter plan (i.e. WHERE or a HAVING
 * clause). This will be rewritten into a left semi/anti join during analysis.
 */
case class InSubQuery(value: Expression, query: LogicalPlan)
  extends CorrelatedSubqueryExpression with Unevaluable  {
  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = value :: Nil
  override def nullable: Boolean = true
  override def withNewPlan(plan: LogicalPlan): InSubQuery = InSubQuery(value, plan)
}

/**
 * The [[Exists]] expression checks if a row exists in a subquery given some correlated condition.
 * This is typically used to filter query results. For example (SQL):
 * {{{5
 *   SELECT  *
 *   FROM    a
 *   WHERE   EXISTS (SELECT  *
 *                   FROM    b
 *                   WHERE   b.id = a.id)
 * }}}
 *
 * Currently we only allow [[Exists]] expressions within a Filter plan (i.e. WHERE or a HAVING
 * clause). This will be rewritten into a left semi/anti join during analysis.
 */
case class Exists(query: LogicalPlan)
  extends CorrelatedSubqueryExpression with Unevaluable with Predicate {
  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): Exists = Exists(plan)
}
