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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.BitSet

/**
 * An interface for expressions that contain a [[QueryPlan]].
 */
abstract class PlanExpression[T <: QueryPlan[_]] extends Expression {

  // Override `treePatternBits` to propagate bits for its internal plan.
  override lazy val treePatternBits: BitSet = {
    val bits: BitSet = getDefaultTreePatternBits
    // Propagate its query plan's pattern bits
    bits.union(plan.treePatternBits)
    bits
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(PLAN_EXPRESSION) ++ nodePatternsInternal()

  override lazy val deterministic: Boolean = children.forall(_.deterministic) &&
    plan.deterministic

  // Subclasses can override this function to provide more TreePatterns.
  def nodePatternsInternal(): Seq[TreePattern] = Seq()

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
 *
 * @param plan: the subquery plan
 * @param outerAttrs: the outer references in the subquery plan
 * @param exprId: ID of the expression
 * @param joinCond: the join conditions with the outer query. It contains both inner and outer
 *                  query references.
 * @param hint: An optional hint for this subquery that will be passed to the join formed from
 *              this subquery.
 */
abstract class SubqueryExpression(
    plan: LogicalPlan,
    outerAttrs: Seq[Expression],
    exprId: ExprId,
    joinCond: Seq[Expression],
    hint: Option[HintInfo]) extends PlanExpression[LogicalPlan] {
  override lazy val resolved: Boolean = childrenResolved && plan.resolved
  override lazy val references: AttributeSet =
    AttributeSet.fromAttributeSets(outerAttrs.map(_.references))
  override def children: Seq[Expression] = outerAttrs ++ joinCond
  override def withNewPlan(plan: LogicalPlan): SubqueryExpression
  def withNewOuterAttrs(outerAttrs: Seq[Expression]): SubqueryExpression
  def isCorrelated: Boolean = outerAttrs.nonEmpty
  def hint: Option[HintInfo]
  def withNewHint(hint: Option[HintInfo]): SubqueryExpression
}

object SubqueryExpression {
  /**
   * Returns true when an expression contains an IN or correlated EXISTS subquery
   * and false otherwise.
   */
  def hasInOrCorrelatedExistsSubquery(e: Expression): Boolean = {
    e.exists {
      case _: ListQuery => true
      case ex: Exists => ex.isCorrelated
      case _ => false
    }
  }

  /**
   * Returns true when an expression contains a subquery that has outer reference(s). The outer
   * reference attributes are kept as children of subquery expression by
   * [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveSubquery]]
   */
  def hasCorrelatedSubquery(e: Expression): Boolean = {
    e.exists {
      case s: SubqueryExpression => s.isCorrelated
      case _ => false
    }
  }

  /**
   * Returns true when an expression contains a subquery
   */
  def hasSubquery(e: Expression): Boolean = {
    e.exists {
      case _: SubqueryExpression => true
      case _ => false
    }
  }
}

object SubExprUtils extends PredicateHelper {
  /**
   * Returns true when an expression contains correlated predicates i.e outer references and
   * returns false otherwise.
   */
  def containsOuter(e: Expression): Boolean = {
    e.exists(_.isInstanceOf[OuterReference])
  }

  /**
   * Returns an expression after removing the OuterReference shell.
   */
  def stripOuterReference[E <: Expression](e: E): E = {
    e.transform { case OuterReference(r) => r }.asInstanceOf[E]
  }

  /**
   * Returns the list of expressions after removing the OuterReference shell from each of
   * the expression.
   */
  def stripOuterReferences[E <: Expression](e: Seq[E]): Seq[E] = e.map(stripOuterReference)

  /**
   * Returns the logical plan after removing the OuterReference shell from all the expressions
   * of the input logical plan.
   */
  def stripOuterReferences(p: LogicalPlan): LogicalPlan = {
    p.transformAllExpressionsWithPruning(_.containsPattern(OUTER_REFERENCE)) {
      case OuterReference(a) => a
    }
  }

  /**
   * Wrap attributes in the expression with [[OuterReference]]s.
   */
  def wrapOuterReference[E <: Expression](e: E): E = {
    e.transform { case a: Attribute => OuterReference(a) }.asInstanceOf[E]
  }

  /**
   * Given a logical plan, returns TRUE if it has an outer reference and false otherwise.
   */
  def hasOuterReferences(plan: LogicalPlan): Boolean = {
    plan.exists(_.expressions.exists(containsOuter))
  }

  /**
   * Given an expression, returns the expressions which have outer references. Aggregate
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
  def getOuterReferences(expr: Expression): Seq[Expression] = {
    val outerExpressions = ArrayBuffer.empty[Expression]
    def collectOutRefs(input: Expression): Unit = input match {
      case a: AggregateExpression if containsOuter(a) =>
        if (a.references.nonEmpty) {
          throw QueryCompilationErrors.mixedRefsInAggFunc(a.sql, a.origin)
        } else {
          // Collect and update the sub-tree so that outer references inside this aggregate
          // expression will not be collected. For example: min(outer(a)) -> min(a).
          val newExpr = stripOuterReference(a)
          outerExpressions += newExpr
        }
      case OuterReference(e) => outerExpressions += e
      case _ => input.children.foreach(collectOutRefs)
    }
    collectOutRefs(expr)
    outerExpressions.toSeq
  }

  /**
   * Returns all the expressions that have outer references from a logical plan. Currently only
   * Filter operator can host outer references.
   */
  def getOuterReferences(plan: LogicalPlan): Seq[Expression] = {
    plan.flatMap(_.expressions.flatMap(getOuterReferences))
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

  /**
   * Matches an equality 'expr = func(outer)', where 'func(outer)' depends on outer rows or
   * is a constant.
   * A scalar subquery is allowed to group-by on 'expr', as they are guaranteed to have exactly
   * one value for every outer row.
   * Positive examples:
   *   - x + 1 = outer(a)
   *   - cast(x as date) = outer(b)
   *   - y + z = 100
   *   - y / 10 = outer(b) + outer(c)
   * In all of these examples, the left side of the equality will be returned.
   *
   * Negative examples:
   *    - x < outer(b)
   *    - x = y
   * In all of these examples, None will be returned.
   * @param expr
   * @return
   */
  private def getEquivalentToOuter(expr: Expression): Option[Expression] = {
    val allowConstants =
      SQLConf.get.getConf(SQLConf.SCALAR_SUBQUERY_ALLOW_GROUP_BY_COLUMN_EQUAL_TO_CONSTANT)

    expr match {
      case EqualTo(left, x)
        if ((allowConstants || containsOuter(x)) &&
          !x.exists(_.isInstanceOf[Attribute])) => Some(left)
      case EqualTo(x, right)
        if ((allowConstants || containsOuter(x)) &&
          !x.exists(_.isInstanceOf[Attribute])) => Some(right)
      case _ => None
    }
  }

  /**
   * Returns the inner query expressions that are guaranteed to have a single value for each
   * outer row. Therefore, a scalar subquery is allowed to group-by on these expressions.
   * We can derive these from correlated equality predicates, though we need to take care about
   * propagating this through operators like OUTER JOIN or UNION.
   *
   * Positive examples:
   * - x = outer(a) AND y = outer(b)
   * - x = 1
   * - x = outer(a) + 1
   * - cast(x as date) = current_date() + outer(b)
   *
   * Negative examples:
   * - x <= outer(a)
   * - x + y = outer(a)
   * - x = outer(a) OR y = outer(b)
   * - y + outer(b) = 1 (this and similar expressions could be supported, but very carefully)
   * - An equality under the right side of a LEFT OUTER JOIN, e.g.
   *   select *, (select count(*) from y left join
   *     (select * from z where z1 = x1) sub on y2 = z2 group by z1) from x;
   * - An equality under UNION e.g.
   *   select *, (select count(*) from
   *     (select * from y where y1 = x1 union all select * from y) group by y1) from x;
   */
  def getCorrelatedEquivalentInnerExpressions(plan: LogicalPlan): ExpressionSet = {
    plan match {
      case Filter(cond, child) =>
        val equivalentExprs = ExpressionSet(splitConjunctivePredicates(cond)
          .filter(
            SQLConf.get.getConf(SQLConf.SCALAR_SUBQUERY_ALLOW_GROUP_BY_COLUMN_EQUAL_TO_CONSTANT)
            || containsOuter(_))
          .flatMap(getEquivalentToOuter))
        equivalentExprs ++ getCorrelatedEquivalentInnerExpressions(child)

      case Join(left, right, joinType, _, _) =>
         joinType match {
          case _: InnerLike =>
            ExpressionSet(plan.children.flatMap(
              child => getCorrelatedEquivalentInnerExpressions(child)))
          case LeftOuter => getCorrelatedEquivalentInnerExpressions(left)
          case RightOuter => getCorrelatedEquivalentInnerExpressions(right)
          case FullOuter => ExpressionSet().empty
          case LeftSemi => getCorrelatedEquivalentInnerExpressions(left)
          case LeftAnti => getCorrelatedEquivalentInnerExpressions(left)
          case _ => ExpressionSet().empty
        }

      case _: Union => ExpressionSet().empty
      case Except(left, _, _) => getCorrelatedEquivalentInnerExpressions(left)

      case
        _: Aggregate |
        _: Distinct |
        _: Intersect |
        _: GlobalLimit |
        _: LocalLimit |
        _: Offset |
        _: Project |
        _: Repartition |
        _: RepartitionByExpression |
        _: RebalancePartitions |
        _: Sample |
        _: Sort |
        _: Window |
        _: Tail |
        _: WithCTE |
        _: Range |
        _: SubqueryAlias =>
        ExpressionSet(plan.children.flatMap(child =>
          getCorrelatedEquivalentInnerExpressions(child)))

      case _ => ExpressionSet().empty
    }
  }
}

/**
 * A subquery that will return only one row and one column. This will be converted into a physical
 * scalar subquery during planning.
 *
 * Note: `exprId` is used to have a unique name in explain string output.
 *
 * `mayHaveCountBug` is whether it's possible for the subquery to evaluate to non-null on
 * empty input (zero tuples). It is false if the subquery has a GROUP BY clause, because in that
 * case the subquery yields no row at all on empty input to the GROUP BY, which evaluates to NULL.
 * It is set in PullupCorrelatedPredicates to true/false, before it is set its value is None.
 * See constructLeftJoins in RewriteCorrelatedScalarSubquery for more details.
 *
 * 'needSingleJoin' is set to true if we can't guarantee that the correlated scalar subquery
 * returns at most 1 row. For such subqueries we use a modification of an outer join called
 * LeftSingle join. This value is set in PullupCorrelatedPredicates and used in
 * RewriteCorrelatedScalarSubquery.
 */
case class ScalarSubquery(
    plan: LogicalPlan,
    outerAttrs: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId,
    joinCond: Seq[Expression] = Seq.empty,
    hint: Option[HintInfo] = None,
    mayHaveCountBug: Option[Boolean] = None,
    needSingleJoin: Option[Boolean] = None)
  extends SubqueryExpression(plan, outerAttrs, exprId, joinCond, hint) with Unevaluable {
  override def dataType: DataType = {
    if (!plan.schema.fields.nonEmpty) {
      throw QueryCompilationErrors.subqueryReturnMoreThanOneColumn(plan.schema.fields.length,
        origin)
    }
    plan.schema.fields.head.dataType
  }
  override def nullable: Boolean = true
  override def withNewPlan(plan: LogicalPlan): ScalarSubquery = copy(plan = plan)
  override def withNewOuterAttrs(outerAttrs: Seq[Expression]): ScalarSubquery = copy(
    outerAttrs = outerAttrs)
  override def withNewHint(hint: Option[HintInfo]): ScalarSubquery = copy(hint = hint)
  override def toString: String = s"scalar-subquery#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    ScalarSubquery(
      plan.canonicalized,
      outerAttrs.map(_.canonicalized),
      ExprId(0),
      joinCond.map(_.canonicalized))
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ScalarSubquery =
    copy(
      outerAttrs = newChildren.take(outerAttrs.size),
      joinCond = newChildren.drop(outerAttrs.size))

  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(SCALAR_SUBQUERY)
}

object ScalarSubquery {
  def hasCorrelatedScalarSubquery(e: Expression): Boolean = {
    e.exists {
      case s: ScalarSubquery => s.isCorrelated
      case _ => false
    }
  }
}

/**
 * A subquery that can return multiple rows and columns. This should be rewritten as a join
 * with the outer query during the optimization phase.
 *
 * Note: `exprId` is used to have a unique name in explain string output.
 */
case class LateralSubquery(
    plan: LogicalPlan,
    outerAttrs: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId,
    joinCond: Seq[Expression] = Seq.empty,
    hint: Option[HintInfo] = None)
  extends SubqueryExpression(plan, outerAttrs, exprId, joinCond, hint) with Unevaluable {
  override def dataType: DataType = plan.output.toStructType
  override def nullable: Boolean = true
  override def withNewPlan(plan: LogicalPlan): LateralSubquery = copy(plan = plan)
  override def withNewOuterAttrs(outerAttrs: Seq[Expression]): LateralSubquery = copy(
    outerAttrs = outerAttrs)
  override def withNewHint(hint: Option[HintInfo]): LateralSubquery = copy(hint = hint)
  override def toString: String = s"lateral-subquery#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    LateralSubquery(
      plan.canonicalized,
      outerAttrs.map(_.canonicalized),
      ExprId(0),
      joinCond.map(_.canonicalized))
  }

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): LateralSubquery =
    copy(
      outerAttrs = newChildren.take(outerAttrs.size),
      joinCond = newChildren.drop(outerAttrs.size))

  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(LATERAL_SUBQUERY)
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
    outerAttrs: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId,
    // The plan of list query may have more columns after de-correlation, and we need to track the
    // number of the columns of the original plan, to report the data type properly.
    numCols: Int = -1,
    joinCond: Seq[Expression] = Seq.empty,
    hint: Option[HintInfo] = None)
  extends SubqueryExpression(plan, outerAttrs, exprId, joinCond, hint) with Unevaluable {
  def childOutputs: Seq[Attribute] = plan.output.take(numCols)
  override def dataType: DataType = if (numCols > 1) {
    childOutputs.toStructType
  } else {
    plan.output.head.dataType
  }
  override lazy val resolved: Boolean = childrenResolved && plan.resolved && numCols != -1
  override def nullable: Boolean = {
    // ListQuery can't be executed alone so its nullability is not defined.
    // Consider using ListQuery.childOutputs.exists(_.nullable)
    if (!SQLConf.get.getConf(SQLConf.LEGACY_IN_SUBQUERY_NULLABILITY)) {
      assert(false, "ListQuery nullability is not defined. To restore the legacy behavior before " +
        s"Spark 3.5.0, set ${SQLConf.LEGACY_IN_SUBQUERY_NULLABILITY.key}=true")
    }
    false
  }
  override def withNewPlan(plan: LogicalPlan): ListQuery = copy(plan = plan)
  override def withNewOuterAttrs(outerAttrs: Seq[Expression]): ListQuery = copy(
    outerAttrs = outerAttrs)
  override def withNewHint(hint: Option[HintInfo]): ListQuery = copy(hint = hint)
  override def toString: String = s"list#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    ListQuery(
      plan.canonicalized,
      outerAttrs.map(_.canonicalized),
      ExprId(0),
      numCols,
      joinCond.map(_.canonicalized))
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ListQuery =
    copy(
      outerAttrs = newChildren.take(outerAttrs.size),
      joinCond = newChildren.drop(outerAttrs.size))

  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(LIST_SUBQUERY)
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
    outerAttrs: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId,
    joinCond: Seq[Expression] = Seq.empty,
    hint: Option[HintInfo] = None)
  extends SubqueryExpression(plan, outerAttrs, exprId, joinCond, hint)
  with Predicate
  with Unevaluable {
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): Exists = copy(plan = plan)
  override def withNewOuterAttrs(outerAttrs: Seq[Expression]): Exists = copy(
    outerAttrs = outerAttrs)
  override def withNewHint(hint: Option[HintInfo]): Exists = copy(hint = hint)
  override def toString: String = s"exists#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    Exists(
      plan.canonicalized,
      outerAttrs.map(_.canonicalized),
      ExprId(0),
      joinCond.map(_.canonicalized))
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Exists =
    copy(
      outerAttrs = newChildren.take(outerAttrs.size),
      joinCond = newChildren.drop(outerAttrs.size))

  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(EXISTS_SUBQUERY)
}
