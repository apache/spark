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

package org.apache.spark.sql.catalyst.planning

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.IntegerType

/**
 * A pattern that matches any number of project or filter operations on top of another relational
 * operator.  All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator.
 * [[org.apache.spark.sql.catalyst.expressions.Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
object PhysicalOperation extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _) = collectProjectsAndFilters(plan)
    Some((fields.getOrElse(child.output), filters, child))
  }

  /**
   * Collects all deterministic projects and filters, in-lining/substituting aliases if necessary.
   * Here are two examples for alias in-lining/substitution.
   * Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
   */
  private def collectProjectsAndFilters(plan: LogicalPlan):
      (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, Map[Attribute, Expression]) =
    plan match {
      case Project(fields, child) if fields.forall(_.deterministic) =>
        val (_, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields))

      case Filter(condition, child) if condition.deterministic =>
        val (fields, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)

      case BroadcastHint(child) =>
        collectProjectsAndFilters(child)

      case other =>
        (None, Nil, other, Map.empty)
    }

  private def collectAliases(fields: Seq[Expression]): Map[Attribute, Expression] = fields.collect {
    case a @ Alias(child, _) => a.toAttribute -> child
  }.toMap

  private def substitute(aliases: Map[Attribute, Expression])(expr: Expression): Expression = {
    expr.transform {
      case a @ Alias(ref: AttributeReference, name) =>
        aliases.get(ref)
          .map(Alias(_, name)(a.exprId, a.qualifier, isGenerated = a.isGenerated))
          .getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a)
          .map(Alias(_, a.name)(a.exprId, a.qualifier, isGenerated = a.isGenerated)).getOrElse(a)
    }
  }
}

/**
 * A pattern that finds joins with equality conditions that can be evaluated using equi-join.
 *
 * Null-safe equality will be transformed into equality as joining key (replace null with default
 * value).
 */
object ExtractEquiJoinKeys extends Logging with PredicateHelper {
  /** (joinType, leftKeys, rightKeys, condition, leftChild, rightChild) */
  type ReturnType =
    (JoinType, Seq[Expression], Seq[Expression], Option[Expression], LogicalPlan, LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case join @ Join(left, right, joinType, condition) =>
      logDebug(s"Considering join on: $condition")
      // Find equi-join predicates that can be evaluated before the join, and thus can be used
      // as join keys.
      val predicates = condition.map(splitConjunctivePredicates).getOrElse(Nil)
      val joinKeys = predicates.flatMap {
        case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => Some((l, r))
        case EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => Some((r, l))
        // Replace null with default value for joining key, then those rows with null in it could
        // be joined together
        case EqualNullSafe(l, r) if canEvaluate(l, left) && canEvaluate(r, right) =>
          Some((Coalesce(Seq(l, Literal.default(l.dataType))),
            Coalesce(Seq(r, Literal.default(r.dataType)))))
        case EqualNullSafe(l, r) if canEvaluate(l, right) && canEvaluate(r, left) =>
          Some((Coalesce(Seq(r, Literal.default(r.dataType))),
            Coalesce(Seq(l, Literal.default(l.dataType)))))
        case other => None
      }
      val otherPredicates = predicates.filterNot {
        case EqualTo(l, r) =>
          canEvaluate(l, left) && canEvaluate(r, right) ||
            canEvaluate(l, right) && canEvaluate(r, left)
        case other => false
      }

      if (joinKeys.nonEmpty) {
        val (leftKeys, rightKeys) = joinKeys.unzip
        logDebug(s"leftKeys:$leftKeys | rightKeys:$rightKeys")
        Some((joinType, leftKeys, rightKeys, otherPredicates.reduceOption(And), left, right))
      } else {
        None
      }
    case _ => None
  }
}

/**
 * A pattern that collects the filter and inner joins.
 *
 *          Filter
 *            |
 *        inner Join
 *          /    \            ---->      (Seq(plan0, plan1, plan2), conditions)
 *      Filter   plan2
 *        |
 *  inner join
 *      /    \
 *   plan0    plan1
 *
 * Note: This pattern currently only works for left-deep trees.
 */
object ExtractFiltersAndInnerJoins extends PredicateHelper {

  /**
   * Flatten all inner joins, which are next to each other.
   * Return a list of logical plans to be joined with a boolean for each plan indicating if it
   * was involved in an explicit cross join. Also returns the entire list of join conditions for
   * the left-deep tree.
   */
  def flattenJoin(plan: LogicalPlan, parentJoinType: InnerLike = Inner)
      : (Seq[(LogicalPlan, InnerLike)], Seq[Expression]) = plan match {
    case Join(left, right, joinType: InnerLike, cond) =>
      val (plans, conditions) = flattenJoin(left, joinType)
      (plans ++ Seq((right, joinType)), conditions ++ cond.toSeq)

    case Filter(filterCondition, j @ Join(left, right, _: InnerLike, joinCondition)) =>
      val (plans, conditions) = flattenJoin(j)
      (plans, conditions ++ splitConjunctivePredicates(filterCondition))

    case _ => (Seq((plan, parentJoinType)), Seq())
  }

  def unapply(plan: LogicalPlan): Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]
      = plan match {
    case f @ Filter(filterCondition, j @ Join(_, _, joinType: InnerLike, _)) =>
      Some(flattenJoin(f))
    case j @ Join(_, _, joinType, _) =>
      Some(flattenJoin(j))
    case _ => None
  }
}


/**
 * A pattern that collects all adjacent unions and returns their children as a Seq.
 * If the top union is wrapped in a [[Distinct]], then the [[Distinct]] in the adjacent unions, if
 * any, will be eliminated.
 */
object Unions {
  def unapply(plan: LogicalPlan): Option[(Seq[LogicalPlan], Boolean)] = plan match {
    case u: Union =>
      Some(collectUnionChildren(mutable.Stack(u), Seq.empty[LogicalPlan], false), false)
    case Distinct(u: Union) =>
      Some(collectUnionChildren(mutable.Stack(u), Seq.empty[LogicalPlan], true), true)
    case _ => None
  }

  // Doing a depth-first tree traversal to combine all the union children.
  @tailrec
  private def collectUnionChildren(
      plans: mutable.Stack[LogicalPlan],
      children: Seq[LogicalPlan],
      dedupDistinct: Boolean): Seq[LogicalPlan] = {
    if (plans.isEmpty) children
    else {
      plans.pop match {
        case Distinct(u @ Union(grandchildren)) if dedupDistinct =>
          grandchildren.reverseMap(plans.push(_))
          collectUnionChildren(plans, children, dedupDistinct)
        case Union(grandchildren) =>
          grandchildren.reverseMap(plans.push(_))
          collectUnionChildren(plans, children, dedupDistinct)
        case other => collectUnionChildren(plans, children :+ other, dedupDistinct)
      }
    }
  }
}

/**
 * An extractor used when planning the physical execution of an aggregation. Compared with a logical
 * aggregation, the following transformations are performed:
 *  - Unnamed grouping expressions are named so that they can be referred to across phases of
 *    aggregation
 *  - Aggregations that appear multiple times are deduplicated.
 *  - The computation of the aggregations themselves is separated from the final result. For
 *    example, the `count` in `count + 1` will be split into an [[AggregateExpression]] and a final
 *    computation that computes `count.resultAttribute + 1`.
 */
object PhysicalAggregation {
  // groupingExpressions, aggregateExpressions, resultExpressions, child
  type ReturnType =
    (Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression], LogicalPlan)

  def unapply(a: Any): Option[ReturnType] = a match {
    case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
      // A single aggregate expression might appear multiple times in resultExpressions.
      // In order to avoid evaluating an individual aggregate function multiple times, we'll
      // build a set of the distinct aggregate expressions and build a function which can
      // be used to re-write expressions so that they reference the single copy of the
      // aggregate function which actually gets computed.
      val aggregateExpressions = resultExpressions.flatMap { expr =>
        expr.collect {
          case agg: AggregateExpression => agg
        }
      }.distinct

      val namedGroupingExpressions = groupingExpressions.map {
        case ne: NamedExpression => ne -> ne
        // If the expression is not a NamedExpressions, we add an alias.
        // So, when we generate the result of the operator, the Aggregate Operator
        // can directly get the Seq of attributes representing the grouping expressions.
        case other =>
          val withAlias = Alias(other, other.toString)()
          other -> withAlias
      }
      val groupExpressionMap = namedGroupingExpressions.toMap

      // The original `resultExpressions` are a set of expressions which may reference
      // aggregate expressions, grouping column values, and constants. When aggregate operator
      // emits output rows, we will use `resultExpressions` to generate an output projection
      // which takes the grouping columns and final aggregate result buffer as input.
      // Thus, we must re-write the result expressions so that their attributes match up with
      // the attributes of the final result projection's input row:
      val rewrittenResultExpressions = resultExpressions.map { expr =>
        expr.transformDown {
          case ae: AggregateExpression =>
            // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
            // so replace each aggregate expression by its corresponding attribute in the set:
            ae.resultAttribute
          case expression =>
            // Since we're using `namedGroupingAttributes` to extract the grouping key
            // columns, we need to replace grouping key expressions with their corresponding
            // attributes. We do not rely on the equality check at here since attributes may
            // differ cosmetically. Instead, we use semanticEquals.
            groupExpressionMap.collectFirst {
              case (expr, ne) if expr semanticEquals expression => ne.toAttribute
            }.getOrElse(expression)
        }.asInstanceOf[NamedExpression]
      }

      Some((
        namedGroupingExpressions.map(_._2),
        aggregateExpressions,
        rewrittenResultExpressions,
        child))

    case _ => None
  }
}
