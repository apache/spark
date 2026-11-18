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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, SubExprUtils, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.catalyst.util.{toPrettySQL, AUTO_GENERATED_ALIAS}

/**
 * Utility wrapper on top of [[RuleExecutor]], used to apply post-resolution rules on single-pass
 * resolution result. [[PlanRewriter]] transforms the plan and the subqueries inside.
 */
class PlanRewriter(
    planRewriteRules: Seq[Rule[LogicalPlan]],
    extendedRewriteRules: Seq[Rule[LogicalPlan]])
    extends ResolverMetricTracker
    {
  private val planRewriter = new RuleExecutor[LogicalPlan] {
    override def batches: Seq[Batch] =
      Seq(
        Batch(
          "Plan Rewriting",
          Once,
          planRewriteRules: _*
        ),
        Batch(
          "Extended Rewriting",
          Once,
          extendedRewriteRules: _*
        )
      )
  }

  /**
   * Rewrites the plan by first recursing into all subqueries and applying post-resolution rules on
   * them and then applying post-resolution rules on the entire plan.
   */
  def rewriteWithSubqueries(plan: LogicalPlan): LogicalPlan = {
    recordProfile("rewriteWithSubqueries") {
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        doRewriteWithSubqueries(plan)
      }
    }
  }

  private def doRewriteWithSubqueries(plan: LogicalPlan): LogicalPlan = {
    val subqueriesWithChangedSql = new mutable.HashSet[ExprId]
    val planWithRewrittenSubqueries =
      plan.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
        case subqueryExpression: SubqueryExpression =>
          val rewrittenSubqueryPlan = doRewriteWithSubqueries(subqueryExpression.plan)

          if (rewrittenSubqueryPlan.fastEquals(subqueryExpression.plan)) {
            subqueryExpression
          } else {
            // A post-resolution rule can change the outer references of the subquery plan, e.g.
            // `ApplyCharTypePadding` can reference an outer attribute more than once when it pads
            // it. The fixed-point Analyzer collects the outer references after running these
            // rules on the subquery plan, so collect them again from the rewritten plan.
            val rewrittenSubqueryExpression = subqueryExpression
              .withNewPlan(rewrittenSubqueryPlan)
              .withNewOuterAttrs(SubExprUtils.getOuterReferences(rewrittenSubqueryPlan))
            if (toPrettySQL(rewrittenSubqueryExpression) != toPrettySQL(subqueryExpression)) {
              subqueriesWithChangedSql += subqueryExpression.exprId
            }
            rewrittenSubqueryExpression
          }
      }

    if (subqueriesWithChangedSql.nonEmpty) {
      checkNoImplicitNameOfChangedSubquery(planWithRewrittenSubqueries, subqueriesWithChangedSql)
    }

    rewrite(planWithRewrittenSubqueries)
  }

  /**
   * An unaliased expression in a select list is named after its SQL during resolution, e.g. an
   * EXISTS subquery with the outer reference `c` is named `exists(c)`. If the post-resolution
   * rules then change the outer references of the subquery, its SQL changes, e.g. to
   * `exists(c, c)`, but its name does not. The fixed-point Analyzer names it after running these
   * rules on the subquery plan, so it gives the column the new name. Renaming the column here
   * would also require renaming every reference to it, so fall back to the fixed-point Analyzer
   * instead. An explicit alias is kept as written by both analyzers.
   */
  private def checkNoImplicitNameOfChangedSubquery(
      plan: LogicalPlan,
      subqueriesWithChangedSql: mutable.Set[ExprId]): Unit = {
    plan.foreach { operator =>
      operator.expressions.foreach(_.foreach {
        case alias: Alias
            if alias.metadata.contains(AUTO_GENERATED_ALIAS) && alias.child.exists {
              case subqueryExpression: SubqueryExpression =>
                subqueriesWithChangedSql.contains(subqueryExpression.exprId)
              case _ => false
            } =>
          throw new ExplicitlyUnsupportedResolverFeature(
            "an implicitly named column whose subquery has its outer references changed by " +
            "the post-resolution rules"
          )
        case _ =>
      })
    }
  }

  /**
   * Rewrites the plan __without__ recursing into the subqueries.
   */
  private def rewrite(plan: LogicalPlan): LogicalPlan = planRewriter.execute(plan)
}
