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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{JOIN, UNION}
import org.apache.spark.sql.internal.SQLConf

/**
 * Pushes down `Join` through `Union` when the right side of the join is small enough
 * to broadcast.
 *
 * This rule transforms the pattern:
 * {{{
 *   Join(Union(c1, c2, ..., cN), right, joinType, cond)
 * }}}
 * into:
 * {{{
 *   Union(Join(c1, right, joinType, cond1), Join(c2, right, joinType, cond2), ...)
 * }}}
 *
 * where each `condK` has the Union output attributes rewritten to the corresponding child's
 * output attributes.
 *
 * This is beneficial when the right side is small enough to broadcast, because each Union
 * branch can directly perform a broadcast join with the right side, avoiding the need to
 * materialize the entire (potentially very large) Union result before the Join.
 *
 * Applicable join types: Inner, LeftOuter.
 */
case class PushDownJoinThroughUnion(override val conf: SQLConf)
  extends Rule[LogicalPlan]
  with JoinSelectionHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.PUSH_DOWN_JOIN_THROUGH_UNION_ENABLED)) return plan
    plan.transformUpWithPruning(
      _.containsAllPatterns(JOIN, UNION), ruleId) {

    case join @ Join(u: Union, right, joinType, joinCond, hint)
      if (joinType == Inner || joinType == LeftOuter) &&
        canPlanAsBroadcastHashJoin(join, conf) &&
        // Exclude right subtrees containing subqueries, as DeduplicateRelations
        // may not correctly handle correlated references when cloning.
        !right.exists(_.expressions.exists(SubqueryExpression.hasSubquery)) &&
        // Exclude non-deterministic right subtrees, as duplicating them would
        // change query semantics (each copy could produce different results).
        !right.exists(_.expressions.exists(!_.deterministic)) =>

      val unionHeadOutput = u.children.head.output
      val newChildren = u.children.zipWithIndex.map { case (child, idx) =>
        val (newRight, rightRewrites) = if (idx == 0) {
          (right, AttributeMap.empty[Attribute])
        } else {
          val deduped = dedupRight(right)
          (deduped, AttributeMap(right.output.zip(deduped.output)))
        }
        val leftRewrites = AttributeMap(unionHeadOutput.zip(child.output))
        val newCond = joinCond.map(_.transform {
          case a: Attribute if leftRewrites.contains(a) => leftRewrites(a)
          case a: Attribute if rightRewrites.contains(a) => rightRewrites(a)
        })
        Join(child, newRight, joinType, newCond, hint)
      }
      u.withNewChildren(newChildren)
  }
  }

  /**
   * Creates a copy of `plan` with fresh ExprIds on all output attributes,
   * using the same "fake self-join + DeduplicateRelations" pattern as InlineCTE.
   */
  private def dedupRight(plan: LogicalPlan): LogicalPlan = {
    DeduplicateRelations(
      Join(plan, plan, Inner, None, JoinHint.NONE)
    ) match {
      case Join(_, deduped, _, _, _) => deduped
      case other =>
        throw SparkException.internalError(
          s"Unexpected plan shape after DeduplicateRelations: ${other.getClass.getName}")
    }
  }
}
