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
 * A predicate subquery checks the existence of a value in a sub-query. We currently only allow
 * [[PredicateSubquery]] expressions within a Filter plan (i.e. WHERE or a HAVING clause). This will
 * be rewritten into a left semi/anti join during analysis.
 */
abstract class PredicateSubquery extends SubqueryExpression with Unevaluable with Predicate {
  override def nullable: Boolean = false
}

object PredicateSubquery {
  def hasPredicateSubquery(e: Expression): Boolean = {
    e.find(_.isInstanceOf[PredicateSubquery]).isDefined
  }
}

/**
 * The [[InSubQuery]] predicate checks the existence of a value in a sub-query. For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   a.id IN (SELECT  id
 *                    FROM    b)
 * }}}
 */
case class InSubQuery(
    value: Expression,
    query: LogicalPlan,
    exprId: ExprId = NamedExpression.newExprId) extends PredicateSubquery {
  override def children: Seq[Expression] = value :: Nil
  override lazy val resolved: Boolean = value.resolved && query.resolved
  override def withNewPlan(plan: LogicalPlan): InSubQuery = InSubQuery(value, plan, exprId)
  override def plan: LogicalPlan = SubqueryAlias(s"subquery#${exprId.id}", query)

  /**
   * The unwrapped value side expressions.
   */
  lazy val expressions: Seq[Expression] = value match {
    case CreateStruct(cols) => cols
    case col => Seq(col)
  }

  /**
   * Check if the number of columns and the data types on both sides match.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    // Check the number of arguments.
    if (expressions.length != query.output.length) {
      return TypeCheckResult.TypeCheckFailure(
        s"The number of fields in the value (${expressions.length}) does not match with " +
          s"the number of columns in the subquery (${query.output.length})")
    }

    // Check the argument types.
    expressions.zip(query.output).zipWithIndex.foreach {
      case ((e, a), i) if e.dataType != a.dataType =>
        return TypeCheckResult.TypeCheckFailure(
          s"The data type of value[$i] (${e.dataType}) does not match " +
            s"subquery column '${a.name}' (${a.dataType}).")
      case _ =>
    }

    TypeCheckResult.TypeCheckSuccess
  }

  override def toString: String = s"$value IN subquery#${exprId.id}"
}

/**
 * The [[Exists]] expression checks if a row exists in a subquery given some correlated condition.
 * For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   EXISTS (SELECT  *
 *                   FROM    b
 *                   WHERE   b.id = a.id)
 * }}}
 */
case class Exists(
    query: LogicalPlan,
    exprId: ExprId = NamedExpression.newExprId) extends PredicateSubquery {
  override def children: Seq[Expression] = Nil
  override def withNewPlan(plan: LogicalPlan): Exists = Exists(plan, exprId)
  override def plan: LogicalPlan = SubqueryAlias(toString, query)
  override def toString: String = s"exists#${exprId.id}"
}
