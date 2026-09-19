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

    case join @ Join(u: Union, right, joinType, _, _)
      if (joinType == Inner || joinType == LeftOuter) &&
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
        branchJoin(join, unionHeadOutput, child, newRight, rightRewrites)
      }
      u.withNewChildren(newChildren)
  }
  }

  /**
   * The join for one `Union` branch: the branch on the left, `newRight` on the right, and the
   * condition rewritten from the `Union` output to the outputs of both new children.
   */
  private def branchJoin(
      join: Join,
      unionHeadOutput: Seq[Attribute],
      child: LogicalPlan,
      newRight: LogicalPlan,
      rightRewrites: AttributeMap[Attribute]): Join = {
    val leftRewrites = AttributeMap(unionHeadOutput.zip(child.output))
    val newCond = join.condition.map(_.transform {
      case a: Attribute if leftRewrites.contains(a) => leftRewrites(a)
      case a: Attribute if rightRewrites.contains(a) => rightRewrites(a)
    })
    Join(child, newRight, join.joinType, newCond, join.hint)
  }

  /**
   * Whether every join produced by the rewrite is expected to broadcast its right side.
   *
   * Asking whether a broadcast hash join is possible is not enough: for an inner join the planner
   * may build from either side, choosing the smaller one when both qualify. The rewrite replaces
   * the `Union` on the left with one of its children, so the build side is decided per branch
   * against a smaller left. Any branch that ends up building from the left leaves its copy of the
   * right side as a plain probe input, which is not reused, so the right side is read once per such
   * branch instead of once in total.
   *
   * `getBroadcastHashJoinBuildSide` returns `None` when the join has no equi-join keys, so this one
   * check also carries the requirement the removed `canPlanAsBroadcastHashJoin` conjunct used to.
   * It is not a statement about what the planner ends up choosing: with keys no hash join supports
   * it still answers from the sizes, while `JoinSelection` falls through to a sort merge join. A
   * `SHUFFLE_MERGE` or `SHUFFLE_REPLICATE_NL` hint is handled here rather than there: the planner
   * tries those between a hinted broadcast and a size-based one, and falls back to the sizes only
   * when the hinted strategy does not apply, so declining to rewrite errs on the safe side.
   *
   * The result predicts rather than guarantees the final build side, because later rules and AQE
   * re-estimate the sizes it reads. It is also all or nothing: one branch small enough to be the
   * build side blocks the rewrite for the others, trading a missed optimization for never
   * duplicating a probe side. Only an inner join can build from the left, so for a left outer join
   * this reduces to asking whether the right side is broadcastable at all.
   */
  private def broadcastsRightForEveryBranch(u: Union, join: Join): Boolean = {
    val hintPicksOtherStrategy =
      hintToSortMergeJoin(join.hint) || hintToShuffleReplicateNL(join.hint)
    val unionHeadOutput = u.children.head.output
    u.children.forall { child =>
      // The condition has to be rewritten to the branch output: `getBroadcastHashJoinBuildSide`
      // extracts the equi-join keys, which do not resolve against a branch other than the first.
      val probe =
        branchJoin(join, unionHeadOutput, child, join.right, AttributeMap.empty[Attribute])
      // A hinted broadcast outranks the hints above, so only a size-based answer is vetoed.
      val hinted = getBroadcastBuildSide(probe, hintOnly = true, conf).isDefined
      val planned =
        if (hinted || !hintPicksOtherStrategy) getBroadcastHashJoinBuildSide(probe, conf) else None
      planned.contains(BuildRight)
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
