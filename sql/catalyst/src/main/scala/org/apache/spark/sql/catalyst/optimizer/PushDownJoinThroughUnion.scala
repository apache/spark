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
 * Pushes down `Join` through `Union` when every resulting join would broadcast its right side.
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
 * This is beneficial when each branch broadcasts the right side, because the branch can then join
 * it directly instead of materializing the whole Union first. The right side has to be the
 * broadcast side: the rewrite duplicates it once per branch, and a copy that is probed rather than
 * broadcast is scanned on its own.
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
        // Requires equi-join keys, and rejects the join when a shuffle hash hint already decided
        // the strategy. The check below adds the build side direction, which this one does not
        // constrain.
        canPlanAsBroadcastHashJoin(join, conf) &&
        broadcastsRightForEveryBranch(u, join) &&
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
   * Whether every join produced by the rewrite is expected to broadcast its right side.
   *
   * `canPlanAsBroadcastHashJoin` is not enough. It holds when either side is broadcastable, and
   * for an inner join the planner may build from either side, choosing the smaller one when both
   * qualify. The rewrite replaces the `Union` on the left with one of its children, so the build
   * side is decided per branch against a smaller left. Any branch that ends up building from the
   * left leaves its copy of the right side as a plain probe input, which is not reused, so the
   * right side is read once per such branch instead of once in total.
   *
   * The check follows the planner's broadcast precedence, hints before sizes. It predicts rather
   * than guarantees the final build side, because later rules and AQE re-estimate the sizes it
   * reads. It is also all or nothing: one branch small enough to be the build side blocks the
   * rewrite for the others, trading a missed optimization for never duplicating a probe side. Only
   * an inner join can build from the left, so for a left outer join this reduces to asking whether
   * the right side is broadcastable at all.
   */
  private def broadcastsRightForEveryBranch(u: Union, join: Join): Boolean = {
    // The planner tries these hints between a hinted broadcast and a size-based one, so the sizes
    // below are usually not what decides the strategy. When the hinted strategy turns out not to
    // apply the planner does fall back to a size-based broadcast, so this errs towards not
    // rewriting. A shuffle hash hint needs no such check: given hash-joinable keys it makes
    // `canPlanAsBroadcastHashJoin` reject the join, and a broadcast hint that outranks it is
    // already answered by the hint-only query below.
    val hintPicksOtherStrategy =
      hintToSortMergeJoin(join.hint) || hintToShuffleReplicateNL(join.hint)
    u.children.forall { child =>
      // Only the join type, the hints and each side's stats are read below, so the condition is
      // left as is, still referencing the Union output rather than the branch output.
      val branchJoin = join.copy(left = child)
      getBroadcastBuildSide(branchJoin, hintOnly = true, conf)
        .orElse {
          if (hintPicksOtherStrategy) None
          else getBroadcastBuildSide(branchJoin, hintOnly = false, conf)
        }
        .contains(BuildRight)
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
