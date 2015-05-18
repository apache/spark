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

import org.apache.spark.sql.catalyst.expressions.aggregate2.AggregateExpression2

import scala.annotation.tailrec

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * A pattern that matches any number of filter operations on top of another relational operator.
 * Adjacent filter operators are collected and their conditions are broken up and returned as a
 * sequence of conjunctive predicates.
 *
 * @return A tuple containing a sequence of conjunctive predicates that should be used to filter the
 *         output and a relational operator.
 */
object FilteredOperation extends PredicateHelper {
  type ReturnType = (Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = Some(collectFilters(Nil, plan))

  @tailrec
  private def collectFilters(filters: Seq[Expression], plan: LogicalPlan): ReturnType = plan match {
    case Filter(condition, child) =>
      collectFilters(filters ++ splitConjunctivePredicates(condition), child)
    case other => (filters, other)
  }
}

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
   * Collects projects and filters, in-lining/substituting aliases if necessary.  Here are two
   * examples for alias in-lining/substitution.  Before:
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
  def collectProjectsAndFilters(plan: LogicalPlan):
      (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, Map[Attribute, Expression]) =
    plan match {
      case Project(fields, child) =>
        val (_, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields))

      case Filter(condition, child) =>
        val (fields, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)

      case other =>
        (None, Nil, other, Map.empty)
    }

  def collectAliases(fields: Seq[Expression]): Map[Attribute, Expression] = fields.collect {
    case a @ Alias(child, _) => a.toAttribute -> child
  }.toMap

  def substitute(aliases: Map[Attribute, Expression])(expr: Expression): Expression = {
    expr.transform {
      case a @ Alias(ref: AttributeReference, name) =>
        aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
    }
  }
}

/**
 *
 * TODO: This is a temporal solution to substitute the expression tree from
 * AggregateExpression with aggregate2.AggregateExpression2, and will be
 * removed once the aggregate2.AggregateExpression2 is stable enough.
 */
class AggregateExpressionSubsitution {
  def subsitute(aggr: AggregateExpression): AggregateExpression2 = aggr match {
    case Min(child) => aggregate2.Min(child)
    case Max(child) => aggregate2.Max(child)
    case Count(child) => aggregate2.Count(child)
    case CountDistinct(children) => aggregate2.CountDistinct(children)
      // TODO: we don't support approximate in aggregate2 yet.
    case ApproxCountDistinct(child, sd) => aggregate2.CountDistinct(child :: Nil)
    case Average(child) => aggregate2.Average(child)
    case Sum(child) => aggregate2.Sum(child)
    case SumDistinct(child) => aggregate2.Sum(child, true)
    case First(child) => aggregate2.First(child)
    case Last(child) => aggregate2.Last(child)
  }
}

// TODO: Will be removed once aggregate2.AggregateExpression2 is stable enough
object AggregateExpressionSubsitution extends AggregateExpressionSubsitution

/**
 * Matches a logical aggregation that can be performed on distributed data in two steps.  The first
 * operates on the data in each partition performing partial aggregation for each group.  The second
 * occurs after the shuffle and completes the aggregation.
 *
 * This pattern will only match if all aggregate expressions can be computed partially and will
 * return the rewritten aggregation expressions for both phases.
 *
 * The returned values for this match are as follows:
 *  - Grouping expression (transformed to NamedExpression) list .
 *  - Aggregate Expressions extract from the projection.
 *  - Rewritten Projection for post shuffled.
 *  - Original Projection.
 *  - Child logical plan.
 */
object PartialAggregation2 {
  type ReturnType =
    (Seq[NamedExpression],
     Seq[aggregate2.AggregateExpression2],
     Seq[NamedExpression],
     Seq[NamedExpression],
     LogicalPlan)

  def unapply(plan: LogicalPlan)
  : Option[ReturnType] = plan match {
    case logical.Aggregate(groupingExpressions, projection, child) =>
      // Collect all aggregate expressions that can be computed partially.
      val allAggregates = projection.flatMap(_ collect {
        case a: aggregate2.AggregateExpression2 => a
      })

      // We need to pass all grouping expressions though so the grouping can happen a second
      // time. However some of them might be unnamed so we alias them allowing them to be
      // referenced in the second aggregation.
      val namedGroupingExpressions = groupingExpressions.map {
        case n: NamedExpression => (n, n)
        case other => (other, Alias(other, "PartialGroup")())
      }
      val substitutions = namedGroupingExpressions.toMap

      // Replace aggregations with a new expression that computes the result from the already
      // computed partial evaluations and grouping values.
      val rewrittenProjection = projection.map(_.transformUp {
        case e: Expression if substitutions.contains(e) =>
          substitutions(e).toAttribute
        case e: AggregateExpression2 => e.transformChildrenDown {
          // replace the child expression of the aggregate expression, with
          // Literal, as in PostShuffle, we don't need children expression any
          // more(Only aggregate buffer required), otherwise, it will
          // cause the attribute not binding exceptions.
          case expr => MutableLiteral(null, expr.dataType, expr.nullable)
        }
        case e: Expression =>
          // Should trim aliases around `GetField`s. These aliases are introduced while
          // resolving struct field accesses, because `GetField` is not a `NamedExpression`.
          // (Should we just turn `GetField` into a `NamedExpression`?)
          substitutions
            .get(e.transform { case Alias(g: ExtractValue, _) => g })
            .map(_.toAttribute)
            .getOrElse(e)
      }).asInstanceOf[Seq[NamedExpression]]

      Some(
        (namedGroupingExpressions.map(_._2),
         allAggregates,
         rewrittenProjection,
         projection,
         child))
    case _ => None
  }
}

/**
 * Matches a logical aggregation that can be performed on distributed data in two steps.  The first
 * operates on the data in each partition performing partial aggregation for each group.  The second
 * occurs after the shuffle and completes the aggregation.
 *
 * This pattern will only match if all aggregate expressions can be computed partially and will
 * return the rewritten aggregation expressions for both phases.
 *
 * The returned values for this match are as follows:
 *  - Grouping attributes for the final aggregation.
 *  - Aggregates for the final aggregation.
 *  - Grouping expressions for the partial aggregation.
 *  - Partial aggregate expressions.
 *  - Input to the aggregation.
 */
object PartialAggregation {
  type ReturnType =
    (Seq[Attribute], Seq[NamedExpression], Seq[Expression], Seq[NamedExpression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case logical.Aggregate(groupingExpressions, aggregateExpressions, child)
      if (aggregateExpressions.flatMap(_.collect {
        case a: aggregate2.AggregateExpression2 => a
      })).length == 0 =>
      // Collect all aggregate expressions.
      val allAggregates =
        aggregateExpressions.flatMap(_ collect { case a: AggregateExpression => a})
      // Collect all aggregate expressions that can be computed partially.
      val partialAggregates =
        aggregateExpressions.flatMap(_ collect { case p: PartialAggregate => p})

      // Only do partial aggregation if supported by all aggregate expressions.
      if (allAggregates.size == partialAggregates.size) {
        // Create a map of expressions to their partial evaluations for all aggregate expressions.
        val partialEvaluations: Map[TreeNodeRef, SplitEvaluation] =
          partialAggregates.map(a => (new TreeNodeRef(a), a.asPartial)).toMap

        // We need to pass all grouping expressions though so the grouping can happen a second
        // time. However some of them might be unnamed so we alias them allowing them to be
        // referenced in the second aggregation.
        val namedGroupingExpressions: Map[Expression, NamedExpression] =
          groupingExpressions.filter(!_.isInstanceOf[Literal]).map {
            case n: NamedExpression => (n, n)
            case other => (other, Alias(other, "PartialGroup")())
          }.toMap

        // Replace aggregations with a new expression that computes the result from the already
        // computed partial evaluations and grouping values.
        val rewrittenAggregateExpressions = aggregateExpressions.map(_.transformUp {
          case e: Expression if partialEvaluations.contains(new TreeNodeRef(e)) =>
            partialEvaluations(new TreeNodeRef(e)).finalEvaluation

          case e: Expression =>
            // Should trim aliases around `GetField`s. These aliases are introduced while
            // resolving struct field accesses, because `GetField` is not a `NamedExpression`.
            // (Should we just turn `GetField` into a `NamedExpression`?)
            val trimmed = e.transform { case Alias(g: ExtractValue, _) => g }
            namedGroupingExpressions
              .find { case (k, v) => k semanticEquals trimmed }
              .map(_._2.toAttribute)
              .getOrElse(e)
        }).asInstanceOf[Seq[NamedExpression]]

        val partialComputation =
          (namedGroupingExpressions.values ++
            partialEvaluations.values.flatMap(_.partialEvaluations)).toSeq

        val namedGroupingAttributes = namedGroupingExpressions.values.map(_.toAttribute).toSeq

        Some(
          (namedGroupingAttributes,
           rewrittenAggregateExpressions,
           groupingExpressions,
           partialComputation,
           child))
      } else {
        None
      }
    case _ => None
  }
}


/**
 * A pattern that finds joins with equality conditions that can be evaluated using equi-join.
 */
object ExtractEquiJoinKeys extends Logging with PredicateHelper {
  /** (joinType, rightKeys, leftKeys, condition, leftChild, rightChild) */
  type ReturnType =
    (JoinType, Seq[Expression], Seq[Expression], Option[Expression], LogicalPlan, LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case join @ Join(left, right, joinType, condition) =>
      logDebug(s"Considering join on: $condition")
      // Find equi-join predicates that can be evaluated before the join, and thus can be used
      // as join keys.
      val (joinPredicates, otherPredicates) =
        condition.map(splitConjunctivePredicates).getOrElse(Nil).partition {
          case EqualTo(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
            (canEvaluate(l, right) && canEvaluate(r, left)) => true
          case _ => false
        }

      val joinKeys = joinPredicates.map {
        case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => (l, r)
        case EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => (r, l)
      }
      val leftKeys = joinKeys.map(_._1)
      val rightKeys = joinKeys.map(_._2)

      if (joinKeys.nonEmpty) {
        logDebug(s"leftKeys:$leftKeys | rightKeys:$rightKeys")
        Some((joinType, leftKeys, rightKeys, otherPredicates.reduceOption(And), left, right))
      } else {
        None
      }
    case _ => None
  }
}

/**
 * A pattern that collects all adjacent unions and returns their children as a Seq.
 */
object Unions {
  def unapply(plan: LogicalPlan): Option[Seq[LogicalPlan]] = plan match {
    case u: Union => Some(collectUnionChildren(u))
    case _ => None
  }

  private def collectUnionChildren(plan: LogicalPlan): Seq[LogicalPlan] = plan match {
    case Union(l, r) => collectUnionChildren(l) ++ collectUnionChildren(r)
    case other => other :: Nil
  }
}
