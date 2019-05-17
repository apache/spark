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

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Replaces [[ResolvedHint]] operators from the plan. Move the [[HintInfo]] to associated [[Join]]
 * operators, otherwise remove it if no [[Join]] operator is matched.
 */
object EliminateResolvedHint extends Rule[LogicalPlan] {
  // This is also called in the beginning of the optimization phase, and as a result
  // is using transformUp rather than resolveOperators.
  def apply(plan: LogicalPlan): LogicalPlan = {
    val pulledUp = plan transformUp {
      case j: Join =>
        val (newLeft, leftHints) = extractHintsFromPlan(j.left)
        val (newRight, rightHints) = extractHintsFromPlan(j.right)
        val newJoinHint = JoinHint(mergeHints(leftHints), mergeHints(rightHints))
        j.copy(left = newLeft, right = newRight, hint = newJoinHint)
    }
    pulledUp.transformUp {
      case h: ResolvedHint =>
        handleInvalidHintInfo(h.hints)
        h.child
    }
  }

  /**
   * Combine a list of [[HintInfo]]s into one [[HintInfo]].
   */
  private def mergeHints(hints: Seq[HintInfo]): Option[HintInfo] = {
    hints.reduceOption((h1, h2) => h1.merge(h2, handleOverriddenHintInfo))
  }

  /**
   * Extract all hints from the plan, returning a list of extracted hints and the transformed plan
   * with [[ResolvedHint]] nodes removed. The returned hint list comes in top-down order.
   * Note that hints can only be extracted from under certain nodes. Those that cannot be extracted
   * in this method will be cleaned up later by this rule, and may emit warnings depending on the
   * configurations.
   */
  private[sql] def extractHintsFromPlan(plan: LogicalPlan): (LogicalPlan, Seq[HintInfo]) = {
    plan match {
      case h: ResolvedHint =>
        val (plan, hints) = extractHintsFromPlan(h.child)
        (plan, h.hints +: hints)
      case u: UnaryNode =>
        val (plan, hints) = extractHintsFromPlan(u.child)
        (u.withNewChildren(Seq(plan)), hints)
      // TODO revisit this logic:
      // except and intersect are semi/anti-joins which won't return more data then
      // their left argument, so the broadcast hint should be propagated here
      case i: Intersect =>
        val (plan, hints) = extractHintsFromPlan(i.left)
        (i.copy(left = plan), hints)
      case e: Except =>
        val (plan, hints) = extractHintsFromPlan(e.left)
        (e.copy(left = plan), hints)
      case p: LogicalPlan => (p, Seq.empty)
    }
  }

  private def handleInvalidHintInfo(hint: HintInfo): Unit = {
    logWarning(s"A join hint $hint is specified but it is not part of a join relation.")
  }

  private def handleOverriddenHintInfo(hint: HintInfo): Unit = {
    logWarning(s"Join hint $hint is overridden by another hint and will not take effect.")
  }
}
