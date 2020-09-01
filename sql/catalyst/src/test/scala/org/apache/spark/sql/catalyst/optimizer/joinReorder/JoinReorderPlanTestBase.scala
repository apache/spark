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

package org.apache.spark.sql.catalyst.optimizer.joinReorder

import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.sideBySide

trait JoinReorderPlanTestBase extends PlanTest {

  def outputsOf(plans: LogicalPlan*): Seq[Attribute] = {
    plans.map(_.output).reduce(_ ++ _)
  }

  def assertEqualJoinPlans(
      optimizer: RuleExecutor[LogicalPlan],
      originalPlan: LogicalPlan,
      groundTruthBestPlan: LogicalPlan): Unit = {
    val analyzed = originalPlan.analyze
    val optimized = optimizer.execute(analyzed)
    val expected = EliminateResolvedHint.apply(groundTruthBestPlan.analyze)

    // if this fails, the expected plan itself is incorrect
    assert(equivalentOutput(analyzed, expected))
    assert(equivalentOutput(analyzed, optimized))

    compareJoinOrder(optimized, expected)
  }

  private def equivalentOutput(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    normalizeExprIds(plan1).output == normalizeExprIds(plan2).output
  }

  /** Fails the test if the join order in the two plans do not match */
  private def compareJoinOrder(plan1: LogicalPlan, plan2: LogicalPlan): Unit = {
    val normalized1 = normalizePlan(normalizeExprIds(plan1))
    val normalized2 = normalizePlan(normalizeExprIds(plan2))
    if (!sameJoinPlan(normalized1, normalized2)) {
      fail(
        s"""
           |== FAIL: Plans do not match ===
           |${sideBySide(
          rewriteNameFromAttrNullability(normalized1).treeString,
          rewriteNameFromAttrNullability(normalized2).treeString).mkString("\n")}
         """.stripMargin)
    }
  }

  /** Consider symmetry for joins when comparing plans. */
  private def sameJoinPlan(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    (plan1, plan2) match {
      case (j1: Join, j2: Join) =>
        (sameJoinPlan(j1.left, j2.left) && sameJoinPlan(j1.right, j2.right)
          && j1.hint.leftHint == j2.hint.leftHint && j1.hint.rightHint == j2.hint.rightHint) ||
          (sameJoinPlan(j1.left, j2.right) && sameJoinPlan(j1.right, j2.left)
            && j1.hint.leftHint == j2.hint.rightHint && j1.hint.rightHint == j2.hint.leftHint)
      case (p1: Project, p2: Project) =>
        p1.projectList == p2.projectList && sameJoinPlan(p1.child, p2.child)
      case _ =>
        plan1 == plan2
    }
  }
}
