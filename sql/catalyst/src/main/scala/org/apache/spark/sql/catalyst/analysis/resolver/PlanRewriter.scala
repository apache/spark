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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION

/**
 * Utility wrapper on top of [[RuleExecutor]], used to apply post-resolution rules on single-pass
 * resolution result. [[SinglePassRewriter]] transforms the plan and the subqueries inside.
 */
class PlanRewriter(planRewriteRules: Seq[Rule[LogicalPlan]]) {
  private val planRewriter = new RuleExecutor[LogicalPlan] {
    override def batches: Seq[Batch] =
      Seq(
        Batch(
          "Plan Rewriting",
          Once,
          planRewriteRules: _*
        )
      )
  }

  /**
   * Rewrites the plan by first recursing into all subqueries and applying post-resolution rules on
   * them and then applying post-resolution rules on the entire plan.
   */
  def rewriteWithSubqueries(plan: LogicalPlan): LogicalPlan =
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      val planWithRewrittenSubqueries =
        plan.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
          case subqueryExpression: SubqueryExpression =>
            val rewrittenSubqueryPlan = rewrite(subqueryExpression.plan)

            subqueryExpression.withNewPlan(rewrittenSubqueryPlan)
        }

      rewrite(planWithRewrittenSubqueries)
    }

  /**
   * Rewrites the plan __without__ recursing into the subqueries.
   */
  private def rewrite(plan: LogicalPlan): LogicalPlan = planRewriter.execute(plan)
}
