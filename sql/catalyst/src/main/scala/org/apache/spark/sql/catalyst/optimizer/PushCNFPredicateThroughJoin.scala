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

import org.apache.spark.sql.catalyst.expressions.{And, PredicateHelper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Try converting join condition to conjunctive normal form expression so that more predicates may
 * be able to be pushed down.
 * To avoid expanding the join condition, the join condition will be kept in the original form even
 * when predicate pushdown happens.
 */
object PushCNFPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {

  private def canPushThrough(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftSemi | RightOuter | LeftOuter | LeftAnti | ExistenceJoin(_) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case j @ Join(left, right, joinType, Some(joinCondition), hint)
        if canPushThrough(joinType) =>
      val predicates = CNFWithGroupExpressionsByQualifier(joinCondition)
      if (predicates.isEmpty) {
        j
      } else {
        val pushDownCandidates = predicates.filter(_.deterministic)
        lazy val leftFilterConditions =
          pushDownCandidates.filter(_.references.subsetOf(left.outputSet))
        lazy val rightFilterConditions =
          pushDownCandidates.filter(_.references.subsetOf(right.outputSet))

        lazy val newLeft =
          leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
        lazy val newRight =
          rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)

        joinType match {
          case _: InnerLike | LeftSemi =>
            Join(newLeft, newRight, joinType, Some(joinCondition), hint)
          case RightOuter =>
            Join(newLeft, right, RightOuter, Some(joinCondition), hint)
          case LeftOuter | LeftAnti | ExistenceJoin(_) =>
            Join(left, newRight, joinType, Some(joinCondition), hint)
          case other =>
            throw new IllegalStateException(s"Unexpected join type: $other")
        }
      }
  }
}
