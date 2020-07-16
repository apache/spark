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

import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

/**
 * Try converting join condition to conjunctive normal form expression so that more predicates may
 * be able to be pushed down.
 * To avoid expanding the join condition, the join condition will be kept in the original form even
 * when predicate pushdown happens.
 */
object PushExtraPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {

  private val processedJoinConditionTag = TreeNodeTag[Expression]("processedJoinCondition")

  private def canPushThrough(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftSemi | RightOuter | LeftOuter | LeftAnti | ExistenceJoin(_) => true
    case _ => false
  }

  /**
   * Splits join condition expressions or filter predicates (on a given join's output) into three
   * categories based on the attributes required to evaluate them. Note that we explicitly exclude
   * non-deterministic (i.e., stateful) condition expressions in canEvaluateInLeft or
   * canEvaluateInRight to prevent pushing these predicates on either side of the join.
   *
   * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
   */
  protected def extractConvertibleFilters(
    condition: Seq[Expression],
    left: LogicalPlan,
    right: LogicalPlan): (Seq[Expression], Seq[Expression], Seq[Expression]) = {
    val (pushDownCandidates, nonDeterministic) = condition.partition(_.deterministic)
    val (leftEvaluateCondition, rest) =
      pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(expr => expr.references.subsetOf(right.outputSet))

    // For the predicates in `commonCondition`, it is still possible to find sub-predicates which
    // are able to be pushed down.
    val leftExtraCondition =
      commonCondition.flatMap(convertibleFilter(_, left.outputSet))

    val rightExtraCondition =
      commonCondition.flatMap(convertibleFilter(_, right.outputSet))

    // To avoid expanding the join condition into conjunctive normal form and making the size
    // of codegen much larger, `commonCondition` will be kept as original form in the new join
    // condition.
    (leftEvaluateCondition ++ leftExtraCondition, rightEvaluateCondition ++ rightExtraCondition,
      commonCondition ++ nonDeterministic)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case j @ Join(left, right, joinType, Some(joinCondition), hint)
        if canPushThrough(joinType) =>
      val filtersOfBothSide = splitConjunctivePredicates(joinCondition).filter { f =>
        f.deterministic && f.references.nonEmpty &&
          !f.references.subsetOf(left.outputSet) && !f.references.subsetOf(right.outputSet)
      }
      val leftExtraCondition =
      filtersOfBothSide.flatMap(convertibleFilter(_, left.outputSet))

      val rightExtraCondition =
        filtersOfBothSide.flatMap(convertibleFilter(_, right.outputSet))

      val alreadyProcessed = j.getTagValue(processedJoinConditionTag).exists { condition =>
        condition.semanticEquals(joinCondition)
      }

      if ((leftExtraCondition.isEmpty && rightExtraCondition.isEmpty) || alreadyProcessed) {
        j
      } else {
        lazy val newLeft =
          leftExtraCondition.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
        lazy val newRight =
          rightExtraCondition.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)

        val newJoin = joinType match {
          case _: InnerLike | LeftSemi =>
            Join(newLeft, newRight, joinType, Some(joinCondition), hint)
          case RightOuter =>
            Join(newLeft, right, RightOuter, Some(joinCondition), hint)
          case LeftOuter | LeftAnti | ExistenceJoin(_) =>
            Join(left, newRight, joinType, Some(joinCondition), hint)
          case other =>
            throw new IllegalStateException(s"Unexpected join type: $other")
        }
        newJoin.setTagValue(processedJoinConditionTag, joinCondition)
        newJoin
    }
  }
}
