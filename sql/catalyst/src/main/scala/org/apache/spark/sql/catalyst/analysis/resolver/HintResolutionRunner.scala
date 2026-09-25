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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.ResolveHints
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan, UnresolvedWith}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.{PLAN_EXPRESSION, UNRESOLVED_WITH}
import org.apache.spark.sql.internal.SQLConf

/**
 * Utility wrapper on top of [[RuleExecutor]], used to apply the hint resolution rules injected
 * through `SparkSessionExtensions.injectHintResolutionRule` on the unresolved plan.
 *
 * The fixed-point Analyzer runs these rules in its "Hints" batch, which completes before the
 * "Resolution" batch, so the rules always observe an unresolved plan. The single-pass Resolver
 * has to run them before the [[MetadataResolver]] pre-pass for the same reason: the pre-pass
 * resolves relation metadata eagerly (which, for path-based relations, lists files), therefore
 * rules that rewrite [[UnresolvedRelation]]s have to run ahead of it.
 *
 * The "Disable Hints" batch precedes the "Hints" batch, exactly like in the fixed-point Analyzer,
 * so that `spark.sql.optimizer.disableHints` keeps hiding the hints from the injected rules.
 *
 * The rules are applied on the main plan, the CTE definitions and the subquery plans. The
 * fixed-point Analyzer covers those same subtrees, through two different mechanisms:
 *  - `CTESubstitution` merges the CTE definitions into the main plan in the "Substitution" batch,
 *    which runs before the "Hints" batch, so the rules observe them as part of the main plan;
 *  - subquery plans are resolved by `ResolveSubquery` re-entering the whole Analyzer through
 *    `executeSameContext`, which replays every batch, "Hints" included.
 *
 * View bodies are covered for the same reason: the fixed-point Analyzer resolves them by
 * re-entering the Analyzer (`ViewResolution.resolve` is called with `executeSameContext`), and the
 * single-pass Resolver calls [[Resolver.lookupMetadataAndResolve]], and therefore this runner, once
 * per unresolved [[View]].
 */
class HintResolutionRunner(hintResolutionRules: Seq[Rule[LogicalPlan]])
    extends ResolverMetricTracker
    with SQLConfHelper {

  private lazy val hintResolver = new RuleExecutor[LogicalPlan] {
    override def batches: Seq[Batch] =
      Seq(
        Batch("Disable Hints", Once, new ResolveHints.DisableHints),
        Batch(
          "Hints",
          FixedPoint(
            conf.analyzerMaxIterations,
            errorOnExceed = true,
            maxIterationsSetting = SQLConf.ANALYZER_MAX_ITERATIONS.key
          ),
          hintResolutionRules: _*
        )
      )
  }

  /**
   * Applies the hint resolution rules by first recursing into the CTE definitions and the
   * subqueries and then applying the rules on the entire plan. No rules are injected by default,
   * in which case the plan is returned as is and the [[RuleExecutor]] is never constructed
   * (`hintResolver` is lazy). That also skips the "Disable Hints" batch, which only matters for
   * plans that still carry an [[UnresolvedHint]] - a shape the single-pass Resolver does not
   * support in the first place.
   *
   * The recursion needs [[AnalysisHelper.allowInvokingTransformsInAnalyzer]] to rewrite the inner
   * plans, so the rules are invoked within its scope, like the rewrite rules in [[PlanRewriter]].
   */
  def resolveWithSubqueries(plan: LogicalPlan): LogicalPlan = {
    if (hintResolutionRules.isEmpty) {
      plan
    } else {
      recordProfile("resolveWithSubqueries") {
        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          doResolveWithSubqueries(plan)
        }
      }
    }
  }

  private def doResolveWithSubqueries(plan: LogicalPlan): LogicalPlan = {
    val planWithResolvedCteDefinitions = resolveCteDefinitions(plan)

    val planWithResolvedSubqueries =
      planWithResolvedCteDefinitions.transformAllExpressionsWithPruning(
        _.containsPattern(PLAN_EXPRESSION)
      ) {
        case subqueryExpression: SubqueryExpression =>
          subqueryExpression.withNewPlan(doResolveWithSubqueries(subqueryExpression.plan))
      }

    hintResolver.execute(planWithResolvedSubqueries)
  }

  /**
   * [[UnresolvedWith]] does not expose its CTE definitions as children, so the rules would not
   * reach them on their own. This is the same reason [[MetadataResolver]] matches
   * [[UnresolvedWith]] explicitly.
   *
   * The rules are applied on the definition body, and not on its [[SubqueryAlias]] wrapper, since
   * [[UnresolvedCTERelation.plan]] is typed as [[SubqueryAlias]]: the result of a rule that
   * replaced the wrapper - any rule of the `plan => Limit(1, plan)` shape does - could not be
   * written back.
   *
   * This is a deliberate limitation of the rule contract rather than full parity: the fixed-point
   * Analyzer runs the "Hints" batch on `WithCTE(CTERelationDef(SubqueryAlias(body)))`, so a rule
   * that matches [[SubqueryAlias]] does observe the CTE aliases there. Rules that rewrite
   * relations, which is what this hook is used for, are unaffected.
   */
  private def resolveCteDefinitions(plan: LogicalPlan): LogicalPlan =
    plan.transformDownWithPruning(_.containsPattern(UNRESOLVED_WITH)) {
      case unresolvedWith: UnresolvedWith =>
        val newCteRelations = unresolvedWith.cteRelations.map { cteRelation =>
          val cteDefinition = cteRelation.plan
          val newCteDefinition = CurrentOrigin.withOrigin(cteDefinition.origin) {
            cteDefinition.copy(child = doResolveWithSubqueries(cteDefinition.child))
          }
          newCteDefinition.copyTagsFrom(cteDefinition)

          cteRelation.copy(plan = newCteDefinition)
        }

        unresolvedWith.copy(cteRelations = newCteRelations)
    }
}
