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
  /**  The id of the subquery expression. */
  def exprId: ExprId

  /** The logical plan of the query. */
  def query: LogicalPlan

  /**
   * Either a logical plan or a physical plan. The generated tree string (explain output) uses this
   * field to explain the subquery.
   */
  def plan: QueryPlan[_]

  /** Updates the query with new logical plan. */
  def withNewPlan(plan: LogicalPlan): SubqueryExpression

  protected def conditionString: String = children.mkString("[", " && ", "]")
}

object SubqueryExpression {
  def hasCorrelatedSubquery(e: Expression): Boolean = {
    e.find {
      case e: SubqueryExpression if e.children.nonEmpty => true
      case _ => false
    }.isDefined
  }
}

/**
 * A subquery that will return only one row and one column. This will be converted into a physical
 * scalar subquery during planning.
 *
 * Note: `exprId` is used to have a unique name in explain string output.
 */
case class ScalarSubquery(
    query: LogicalPlan,
    children: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression with Unevaluable {
  override lazy val resolved: Boolean = childrenResolved && query.resolved
  override lazy val references: AttributeSet = {
    if (query.resolved) super.references -- query.outputSet
    else super.references
  }
  override def dataType: DataType = query.schema.fields.head.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def plan: LogicalPlan = SubqueryAlias(toString, query)
  override def withNewPlan(plan: LogicalPlan): ScalarSubquery = copy(query = plan)
  override def toString: String = s"scalar-subquery#${exprId.id} $conditionString"
}

object ScalarSubquery {
  def hasCorrelatedScalarSubquery(e: Expression): Boolean = {
    e.find {
      case e: ScalarSubquery if e.children.nonEmpty => true
      case _ => false
    }.isDefined
  }
}

/**
 * A predicate subquery checks the existence of a value in a sub-query. We currently only allow
 * [[PredicateSubquery]] expressions within a Filter plan (i.e. WHERE or a HAVING clause). This will
 * be rewritten into a left semi/anti join during analysis.
 */
case class PredicateSubquery(
    query: LogicalPlan,
    children: Seq[Expression] = Seq.empty,
    nullAware: Boolean = false,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression with Predicate with Unevaluable {
  override lazy val resolved = childrenResolved && query.resolved
  override lazy val references: AttributeSet = super.references -- query.outputSet
  override def nullable: Boolean = nullAware
  override def plan: LogicalPlan = SubqueryAlias(toString, query)
  override def withNewPlan(plan: LogicalPlan): PredicateSubquery = copy(query = plan)
  override def toString: String = s"predicate-subquery#${exprId.id} $conditionString"
}

object PredicateSubquery {
  def hasPredicateSubquery(e: Expression): Boolean = {
    e.find {
      case _: PredicateSubquery | _: ListQuery | _: Exists => true
      case _ => false
    }.isDefined
  }

  /**
   * Returns whether there are any null-aware predicate subqueries inside Not. If not, we could
   * turn the null-aware predicate into not-null-aware predicate.
   */
  def hasNullAwarePredicateWithinNot(e: Expression): Boolean = {
    e.find{ x =>
      x.isInstanceOf[Not] && e.find {
        case p: PredicateSubquery => p.nullAware
        case _ => false
      }.isDefined
    }.isDefined
  }
}

/**
 * A [[ListQuery]] expression defines the query which we want to search in an IN subquery
 * expression. It should and can only be used in conjunction with a IN expression.
 *
 * For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   a.id IN (SELECT  id
 *                    FROM    b)
 * }}}
 */
case class ListQuery(query: LogicalPlan, exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression with Unevaluable {
  override lazy val resolved = false
  override def children: Seq[Expression] = Seq.empty
  override def dataType: DataType = ArrayType(NullType)
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): ListQuery = copy(query = plan)
  override def plan: LogicalPlan = SubqueryAlias(toString, query)
  override def toString: String = s"list#${exprId.id}"
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
case class Exists(query: LogicalPlan, exprId: ExprId = NamedExpression.newExprId)
    extends SubqueryExpression with Predicate with Unevaluable {
  override lazy val resolved = false
  override def children: Seq[Expression] = Seq.empty
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): Exists = copy(query = plan)
  override def plan: LogicalPlan = SubqueryAlias(toString, query)
  override def toString: String = s"exists#${exprId.id}"
}
