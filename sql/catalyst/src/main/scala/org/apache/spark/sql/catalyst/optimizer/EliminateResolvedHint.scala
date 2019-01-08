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
        val leftHint = mergeHints(collectHints(j.left))
        val rightHint = mergeHints(collectHints(j.right))
        j.copy(hint = JoinHint(leftHint, rightHint))
    }
    pulledUp.transform {
      case h: ResolvedHint => h.child
    }
  }

  private def mergeHints(hints: Seq[HintInfo]): Option[HintInfo] = {
    hints.reduceOption((h1, h2) => HintInfo(
      broadcast = h1.broadcast || h2.broadcast))
  }

  private def collectHints(plan: LogicalPlan): Seq[HintInfo] = {
    plan match {
      case h: ResolvedHint => collectHints(h.child) :+ h.hints
      case u: UnaryNode => collectHints(u.child)
      // TODO revisit this logic:
      // except and intersect are semi/anti-joins which won't return more data then
      // their left argument, so the broadcast hint should be propagated here
      case i: Intersect => collectHints(i.left)
      case e: Except => collectHints(e.left)
      case _ => Seq.empty
    }
  }
}
