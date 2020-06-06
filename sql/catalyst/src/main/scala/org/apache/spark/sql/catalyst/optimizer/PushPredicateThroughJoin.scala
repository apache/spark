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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{And, AttributeSet, Expression, Not, Or, PredicateHelper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

trait PushPredicateThroughJoinBase extends Rule[LogicalPlan] with PredicateHelper {
  protected def enablePushingExtraPredicates: Boolean
  /**
   * Splits join condition expressions or filter predicates (on a given join's output) into three
   * categories based on the attributes required to evaluate them. Note that we explicitly exclude
   * non-deterministic (i.e., stateful) condition expressions in canEvaluateInLeft or
   * canEvaluateInRight to prevent pushing these predicates on either side of the join.
   *
   * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
   */
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (pushDownCandidates, nonDeterministic) = condition.partition(_.deterministic)
    val (leftEvaluateCondition, rest) =
      pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(expr => expr.references.subsetOf(right.outputSet))

    // For the predicates in `commonCondition`, it is still possible to find sub-predicates which
    // are able to be pushed down.
    val leftExtraCondition = if (enablePushingExtraPredicates) {
      commonCondition.flatMap(convertibleFilter(_, left.outputSet, canPartialPushDown = true))
    } else {
      Seq.empty
    }
    val rightExtraCondition = if (enablePushingExtraPredicates) {
      commonCondition.flatMap(convertibleFilter(_, right.outputSet, canPartialPushDown = true))
    } else {
      Seq.empty
    }

    // To avoid expanding the join condition into conjunctive normal form and making the size
    // of codegen much larger, `commonCondition` will be kept as original form in the new join
    // condition.
    (leftEvaluateCondition ++ leftExtraCondition, rightEvaluateCondition ++ rightExtraCondition,
      commonCondition ++ nonDeterministic)
  }

  private def convertibleFilter(
    condition: Expression,
    outputSet: AttributeSet,
    canPartialPushDown: Boolean): Option[Expression] = condition match {
    // At here, it is not safe to just convert one side and remove the other side
    // if we do not understand what the parent filters are.
    //
    // Here is an example used to explain the reason.
    // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
    // convert b in ('1'). If we only convert a = 2, we will end up with a filter
    // NOT(a = 2), which will generate wrong results.
    //
    // Pushing one side of AND down is only safe to do at the top level or in the child
    // AND before hitting NOT or OR conditions, and in this case, the unsupported predicate
    // can be safely removed.
    case And(left, right) =>
      val leftResultOptional = convertibleFilter(left, outputSet, canPartialPushDown)
      val rightResultOptional = convertibleFilter(right, outputSet, canPartialPushDown)
      (leftResultOptional, rightResultOptional) match {
        case (Some(leftResult), Some(rightResult)) => Some(And(leftResult, rightResult))
        case (Some(leftResult), None) if canPartialPushDown => Some(leftResult)
        case (None, Some(rightResult)) if canPartialPushDown => Some(rightResult)
        case _ => None
      }

    // The Or predicate is convertible when both of its children can be pushed down.
    // That is to say, if one/both of the children can be partially pushed down, the Or
    // predicate can be partially pushed down as well.
    //
    // Here is an example used to explain the reason.
    // Let's say we have
    // (a1 AND a2) OR (b1 AND b2),
    // a1 and b1 is convertible, while a2 and b2 is not.
    // The predicate can be converted as
    // (a1 OR b1) AND (a1 OR b2) AND (a2 OR b1) AND (a2 OR b2)
    // As per the logical in And predicate, we can push down (a1 OR b1).
    case Or(left, right) =>
      for {
        lhs <- convertibleFilter(left, outputSet, canPartialPushDown)
        rhs <- convertibleFilter(right, outputSet, canPartialPushDown)
      } yield Or(lhs, rhs)

    case Not(pred) =>
      val childResultOptional = convertibleFilter(pred, outputSet, canPartialPushDown = false)
      childResultOptional.map(Not)

    case other =>
      if (other.references.subsetOf(outputSet)) {
        Some(other)
      } else {
        None
      }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally

  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
    // push the where condition down into join filter
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition, hint)) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right)
      joinType match {
        case _: InnerLike =>
          // push down the single side `where` condition into respective sides
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val (newJoinConditions, others) =
            commonFilterCondition.partition(canEvaluateWithinJoin)
          val newJoinCond = if (enablePushingExtraPredicates) {
            joinCondition
          } else {
            (newJoinConditions ++ joinCondition).reduceLeftOption(And)
          }

          val join = Join(newLeft, newRight, joinType, newJoinCond, hint)
          if (others.nonEmpty) {
            Filter(others.reduceLeft(And), join)
          } else {
            join
          }
        case RightOuter =>
          // push down the right side only `where` condition
          val newLeft = left
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond, hint)

          (leftFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case LeftOuter | LeftExistence(_) =>
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond, hint)

          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case FullOuter => f // DO Nothing for Full Outer Join
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }

    // push down the join filter into sub query scanning if applicable
    case j @ Join(left, right, joinType, joinCondition, hint) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      joinType match {
        case _: InnerLike | LeftSemi =>
          // push down the single side only join filter for both sides sub queries
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = if (enablePushingExtraPredicates) {
            joinCondition
          } else {
            commonJoinCondition.reduceLeftOption(And)
          }

          Join(newLeft, newRight, joinType, newJoinCond, hint)
        case RightOuter =>
          // push down the left side only join filter for left side sub query
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = if (enablePushingExtraPredicates) {
            joinCondition
          } else {
            (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)
          }

          Join(newLeft, newRight, RightOuter, newJoinCond, hint)
        case LeftOuter | LeftAnti | ExistenceJoin(_) =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = if (enablePushingExtraPredicates) {
            joinCondition
          } else {
            (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)
          }

          Join(newLeft, newRight, joinType, newJoinCond, hint)
        case FullOuter => j
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }
  }
}

/**
 * Pushes down [[Filter]] operators where the `condition` can be
 * evaluated using only the attributes of the left or right side of a join.  Other
 * [[Filter]] conditions are moved into the `condition` of the [[Join]].
 *
 * And also pushes down the join filter, where the `condition` can be evaluated using only the
 * attributes of the left or right side of sub query when applicable.
 *
 * Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more details
 */
object PushPredicateThroughJoin extends PushPredicateThroughJoinBase {
  override def enablePushingExtraPredicates: Boolean = false
}

/**
 * Pushes down [[Filter]] operators where the `condition` or subset of `condition` can be
 * evaluated using only the attributes of the left or right side of a join.  Other
 * [[Filter]] conditions are moved into the `condition` of the [[Join]].
 *
 * And also pushes down the join filter, where the `condition` can be evaluated using only the
 * attributes of the left or right side of sub query when applicable.
 *
 * Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more details.
 *
 * Note: the rule is supposed to be executed once for one cerntain plan, otherwise the extra
 *       sub-predicates will be pushed down multiple times.
 */
object PushExtraPredicateThroughJoin extends PushPredicateThroughJoinBase {
  override def enablePushingExtraPredicates: Boolean = true
}
