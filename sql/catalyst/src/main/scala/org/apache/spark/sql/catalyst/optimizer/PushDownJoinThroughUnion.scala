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
 * This is beneficial when the right side is small enough to broadcast, because it avoids
 * shuffling the (potentially very large) Union result before the Join. Instead, each Union
 * branch joins independently with the broadcasted right side.
 *
 * Applicable join types: Inner, LeftOuter.
 */
object PushDownJoinThroughUnion
  extends Rule[LogicalPlan]
  with JoinSelectionHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsAllPatterns(JOIN, UNION), ruleId) {

    case join @ Join(u: Union, right, joinType, joinCond, hint)
      if (joinType == Inner || joinType == LeftOuter) &&
        canPlanAsBroadcastHashJoin(join, conf) &&
        // Conservatively exclude any right subtree containing subqueries,
        // as DeduplicateRelations may not correctly handle correlated references.
        // Non-correlated subqueries are safe in theory but excluded for simplicity.
        !right.exists(_.expressions.exists(SubqueryExpression.hasSubquery)) =>

      // Each Union branch gets its own independent copy of `right` with fresh
      // ExprIds to avoid duplicate ExprIds in the plan tree. The first branch
      // reuses the original `right` directly; subsequent branches use the
      // "fake self-join + DeduplicateRelations" pattern (same as InlineCTE)
      // to clone the subtree.
      //
      // Note: join condition attributes referencing `right.output` are assumed
      // to share the same ExprIds, which holds after the analysis phase.
      val unionHeadOutput = u.children.head.output
      val newChildren = u.children.zipWithIndex.map { case (child, idx) =>
        val newRight = if (idx == 0) right else dedupRight(right)
        // For idx == 0, child == u.children.head, so leftRewrites is identity
        // and rightRewrites is empty; the condition is used as-is.
        val leftRewrites = AttributeMap(unionHeadOutput.zip(child.output))
        val rightRewrites = if (idx == 0) {
          AttributeMap.empty[Attribute]
        } else {
          AttributeMap(right.output.zip(newRight.output))
        }
        val newCond = joinCond.map(_.transform {
          case a: Attribute if leftRewrites.contains(a) => leftRewrites(a)
          case a: Attribute if rightRewrites.contains(a) => rightRewrites(a)
        })
        Join(child, newRight, joinType, newCond, hint)
      }
      u.withNewChildren(newChildren)
  }

  /**
   * Creates a copy of `plan` with fresh ExprIds on all output attributes.
   * Uses the same "fake self-join + DeduplicateRelations" pattern as InlineCTE.
   *
   * This works for any plan whose leaf nodes implement `MultiInstanceRelation`
   * (e.g., `LocalRelation`, `LogicalRelation`, `HiveTableRelation`), which covers
   * both test and production scenarios. If a leaf node does not implement
   * `MultiInstanceRelation` (e.g., some custom data sources), `DeduplicateRelations`
   * will not refresh its ExprIds. Such cases are rare in practice.
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
