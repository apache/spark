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

import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.types._

/**
 * An interface for expressions that contain a [[QueryPlan]].
 */
abstract class PlanExpression[T <: QueryPlan[_]] extends Expression {
  /**  The id of the subquery expression. */
  def exprId: ExprId

  /** The plan being wrapped in the query. */
  def plan: T

  /** Updates the expression with a new plan. */
  def withNewPlan(plan: T): PlanExpression[T]

  protected def conditionString: String = children.mkString("[", " && ", "]")
}

/**
 * A base interface for expressions that contain a [[LogicalPlan]].
 */
abstract class SubqueryExpression extends PlanExpression[LogicalPlan] {
  override def withNewPlan(plan: LogicalPlan): SubqueryExpression
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
    plan: LogicalPlan,
    children: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression with Unevaluable {
  override lazy val resolved: Boolean = childrenResolved && plan.resolved
  override lazy val references: AttributeSet = {
    if (plan.resolved) super.references -- plan.outputSet
    else super.references
  }
  override def dataType: DataType = plan.schema.fields.head.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def withNewPlan(plan: LogicalPlan): ScalarSubquery = copy(plan = plan)
  override def toString: String = s"scalar-subquery#${exprId.id} $conditionString"
  override def sql: String = s"(${plan.sql})"
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
    plan: LogicalPlan,
    children: Seq[Expression] = Seq.empty,
    nullAware: Boolean = false,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression with Predicate with Unevaluable {
  override lazy val resolved = childrenResolved && plan.resolved
  override lazy val references: AttributeSet = super.references -- plan.outputSet
  override def nullable: Boolean = nullAware
  override def withNewPlan(plan: LogicalPlan): PredicateSubquery = copy(plan = plan)
  override def semanticEquals(o: Expression): Boolean = o match {
    case p: PredicateSubquery =>
      plan.sameResult(p.plan) && nullAware == p.nullAware &&
        children.length == p.children.length &&
        children.zip(p.children).forall(p => p._1.semanticEquals(p._2))
    case _ => false
  }
  override def toString: String = s"predicate-subquery#${exprId.id} $conditionString"
  override def sql: String = {
    if (nullAware) {
      val (in, correlated) = children.partition(_.isInstanceOf[EqualTo])
      val (outer, inner) = in.zipWithIndex.map {
        case (EqualTo(l, r), i) if plan.outputSet.intersect(r.references).nonEmpty =>
          (l, Alias(r, s"_c$i")())
        case (EqualTo(r, l), i) =>
          (l, Alias(r, s"_c$i")())
      }.unzip
      val filtered = if (correlated.nonEmpty) {
        Filter(children.reduce(And), plan)
      } else {
        plan
      }
      val value = outer match {
        case Seq(expr) => expr
        case exprs => CreateStruct(exprs)
      }
      children.head.map(_.sql).mkString(" AND ")
      s"${value.sql} IN (${filtered.sql})"
    } else {
      val conditionSQL = children.map(_.sql).mkString(" AND ")
      val subquery = if (conditionSQL.isEmpty) {
        plan.sql
      } else {
        s"${plan.sql} ${if (plan.sql.contains("WHERE")) "AND" else "WHERE"} $conditionSQL"
      }
      s"EXISTS ($subquery)"
    }
  }
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
 * expression. It should and can only be used in conjunction with an IN expression.
 *
 * For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   a.id IN (SELECT  id
 *                    FROM    b)
 * }}}
 */
case class ListQuery(plan: LogicalPlan, exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression with Unevaluable {
  override lazy val resolved = false
  override def children: Seq[Expression] = Seq.empty
  override def dataType: DataType = ArrayType(NullType)
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): ListQuery = copy(plan = plan)
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
case class Exists(plan: LogicalPlan, exprId: ExprId = NamedExpression.newExprId)
    extends SubqueryExpression with Predicate with Unevaluable {
  override lazy val resolved = false
  override def children: Seq[Expression] = Seq.empty
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): Exists = copy(plan = plan)
  override def toString: String = s"exists#${exprId.id}"
}
