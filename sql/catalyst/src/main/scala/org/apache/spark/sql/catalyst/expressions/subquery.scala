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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
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
abstract class SubqueryExpression(
    plan: LogicalPlan,
    children: Seq[Expression],
    exprId: ExprId) extends PlanExpression[LogicalPlan] {
  override lazy val resolved: Boolean = childrenResolved && plan.resolved
  override lazy val references: AttributeSet =
    if (plan.resolved) super.references -- plan.outputSet else super.references
  override def withNewPlan(plan: LogicalPlan): SubqueryExpression
  override def semanticEquals(o: Expression): Boolean = o match {
    case p: SubqueryExpression =>
      this.getClass.getName.equals(p.getClass.getName) && plan.sameResult(p.plan) &&
        children.length == p.children.length &&
        children.zip(p.children).forall(p => p._1.semanticEquals(p._2))
    case _ => false
  }
}

object SubqueryExpression {
  /**
   * Returns true when an expression contains an IN or correlated EXISTS subquery
   * and false otherwise.
   */
  def hasInOrCorrelatedExistsSubquery(e: Expression): Boolean = {
    e.find {
      case _: ListQuery => true
      case _: Exists if e.children.nonEmpty => true
      case _ => false
    }.isDefined
  }

  /**
   * Returns true when an expression contains a subquery that has outer reference(s). The outer
   * reference attributes are kept as children of subquery expression by
   * [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveSubquery]]
   */
  def hasCorrelatedSubquery(e: Expression): Boolean = {
    e.find {
      case s: SubqueryExpression => s.children.nonEmpty
      case _ => false
    }.isDefined
  }

  /**
   * Returns true when an expression contains a subquery
   */
  def hasSubquery(e: Expression): Boolean = {
    e.find {
      case _: SubqueryExpression => true
      case _ => false
    }.isDefined
  }
}

object SubExprUtils extends PredicateHelper {
  /**
   * Returns true when an expression contains correlated predicates i.e outer references and
   * returns false otherwise.
   */
  def containsOuter(e: Expression): Boolean = {
    e.find(_.isInstanceOf[OuterReference]).isDefined
  }

  /**
   * Returns an expression after removing the OuterReference shell.
   */
  def stripOuterReference(e: Expression): Expression = e.transform { case OuterReference(r) => r }

  /**
   * Returns the list of expressions after removing the OuterReference shell from each of
   * the expression.
   */
  def stripOuterReferences(e: Seq[Expression]): Seq[Expression] = e.map(stripOuterReference)

  /**
   * Returns the logical plan after removing the OuterReference shell from all the expressions
   * of the input logical plan.
   */
  def stripOuterReferences(p: LogicalPlan): LogicalPlan = {
    p.transformAllExpressions {
      case OuterReference(a) => a
    }
  }

  /**
   * Given a logical plan, returns TRUE if it has an outer reference and false otherwise.
   */
  def hasOuterReferences(plan: LogicalPlan): Boolean = {
    plan.find {
      case f: Filter => containsOuter(f.condition)
      case other => false
    }.isDefined
  }

  /**
   * Given a list of expressions, returns the expressions which have outer references. Aggregate
   * expressions are treated in a special way. If the children of aggregate expression contains an
   * outer reference, then the entire aggregate expression is marked as an outer reference.
   * Example (SQL):
   * {{{
   *   SELECT a FROM l GROUP by 1 HAVING EXISTS (SELECT 1 FROM r WHERE d < min(b))
   * }}}
   * In the above case, we want to mark the entire min(b) as an outer reference
   * OuterReference(min(b)) instead of min(OuterReference(b)).
   * TODO: Currently we don't allow deep correlation. Also, we don't allow mixing of
   * outer references and local references under an aggregate expression.
   * For example (SQL):
   * {{{
   *   SELECT .. FROM p1
   *   WHERE EXISTS (SELECT ...
   *                 FROM p2
   *                 WHERE EXISTS (SELECT ...
   *                               FROM sq
   *                               WHERE min(p1.a + p2.b) = sq.c))
   *
   *   SELECT .. FROM p1
   *   WHERE EXISTS (SELECT ...
   *                 FROM p2
   *                 WHERE EXISTS (SELECT ...
   *                               FROM sq
   *                               WHERE min(p1.a) + max(p2.b) = sq.c))
   *
   *   SELECT .. FROM p1
   *   WHERE EXISTS (SELECT ...
   *                 FROM p2
   *                 WHERE EXISTS (SELECT ...
   *                               FROM sq
   *                               WHERE min(p1.a + sq.c) > 1))
   * }}}
   * The code below needs to change when we support the above cases.
   */
  def getOuterReferences(conditions: Seq[Expression]): Seq[Expression] = {
    val outerExpressions = ArrayBuffer.empty[Expression]
    conditions foreach { expr =>
      expr transformDown {
        case a: AggregateExpression if a.collectLeaves.forall(_.isInstanceOf[OuterReference]) =>
          val newExpr = stripOuterReference(a)
          outerExpressions += newExpr
          newExpr
        case OuterReference(e) =>
          outerExpressions += e
          e
      }
    }
    outerExpressions.toSeq
  }

  /**
   * Returns all the expressions that have outer references from a logical plan. Currently only
   * Filter operator can host outer references.
   */
  def getOuterReferences(plan: LogicalPlan): Seq[Expression] = {
    val conditions = plan.collect { case Filter(cond, _) => cond }
    getOuterReferences(conditions)
  }

  /**
   * Returns the correlated predicates from a logical plan. The OuterReference wrapper
   * is removed before returning the predicate to the caller.
   */
  def getCorrelatedPredicates(plan: LogicalPlan): Seq[Expression] = {
    val conditions = plan.collect { case Filter(cond, _) => cond }
    conditions.flatMap { e =>
      val (correlated, _) = splitConjunctivePredicates(e).partition(containsOuter)
      stripOuterReferences(correlated) match {
        case Nil => None
        case xs => xs
      }
    }
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
  extends SubqueryExpression(plan, children, exprId) with Unevaluable {
  override def dataType: DataType = {
    assert(plan.schema.fields.nonEmpty, "Scalar subquery should have only one column")
    plan.schema.fields.head.dataType
  }
  override def nullable: Boolean = true
  override def withNewPlan(plan: LogicalPlan): ScalarSubquery = copy(plan = plan)
  override def toString: String = s"scalar-subquery#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    ScalarSubquery(
      plan.canonicalized,
      children.map(_.canonicalized),
      ExprId(0))
  }
}

object ScalarSubquery {
  def hasCorrelatedScalarSubquery(e: Expression): Boolean = {
    e.find {
      case s: ScalarSubquery => s.children.nonEmpty
      case _ => false
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
case class ListQuery(
    plan: LogicalPlan,
    children: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId,
    childOutputs: Seq[Attribute] = Seq.empty)
  extends SubqueryExpression(plan, children, exprId) with Unevaluable {
  override def dataType: DataType = if (childOutputs.length > 1) {
    childOutputs.toStructType
  } else {
    childOutputs.head.dataType
  }
  override lazy val resolved: Boolean = childrenResolved && plan.resolved && childOutputs.nonEmpty
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): ListQuery = copy(plan = plan)
  override def toString: String = s"list#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    ListQuery(
      plan.canonicalized,
      children.map(_.canonicalized),
      ExprId(0),
      childOutputs.map(_.canonicalized.asInstanceOf[Attribute]))
  }
}

/**
 * The [[Exists]] expression checks if a row exists in a subquery given some correlated condition
 * or some uncorrelated condition.
 *
 * 1. correlated condition:
 *
 * For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   EXISTS (SELECT  *
 *                   FROM    b
 *                   WHERE   b.id = a.id)
 * }}}
 *
 * 2. uncorrelated condition example:
 *
 * For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   EXISTS (SELECT  *
 *                   FROM    b
 *                   WHERE   b.id > 10)
 * }}}
 */
case class Exists(
    plan: LogicalPlan,
    children: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression(plan, children, exprId) with Predicate with Unevaluable {
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): Exists = copy(plan = plan)
  override def toString: String = s"exists#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    Exists(
      plan.canonicalized,
      children.map(_.canonicalized),
      ExprId(0))
  }
}
