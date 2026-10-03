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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.ResolveHints
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, CTERelationDef, LogicalPlan, SubqueryAlias, UnresolvedWith, WithCTE}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION, UNRESOLVED_WITH}
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
 * The plan is presented to the rules in the shape the fixed-point "Hints" batch observes:
 *  - `CTESubstitution` runs in the "Substitution" batch, before the "Hints" batch, and turns
 *    every `WITH` into a [[WithCTE]] whose [[CTERelationDef]]s are children of the root. The
 *    runner does the same with each [[UnresolvedWith]] for the duration of the batch, so the
 *    rules receive one root per iteration and reach the CTE definitions as part of it, and
 *    converts the [[WithCTE]] back to an [[UnresolvedWith]] afterwards. Unlike the fixed-point
 *    Analyzer, references to the CTEs are still [[UnresolvedRelation]]s at that point, since the
 *    single-pass Resolver links them only during resolution.
 *  - subquery plans are resolved by `ResolveSubquery` re-entering the whole Analyzer through
 *    `executeSameContext`, which replays every batch, "Hints" included, once the "Hints" batch has
 *    completed on the containing plan. The runner applies the batch to each subquery plan
 *    separately and after the containing plan in the same way, so the containing plan observes
 *    subqueries that have not had the batch applied yet, and a subquery created by an injected
 *    rule still receives the batch before relation metadata is resolved.
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
   * Applies the hint resolution rules to `plan`, including its CTE definitions, first, then to the
   * subquery plans that exist afterwards, including subqueries the rules just created. No rules
   * are injected by default, in which case the plan is returned as is and the [[RuleExecutor]] is
   * never constructed (`hintResolver` is lazy). That also skips the "Disable Hints" batch, which
   * only matters for plans that still carry an [[UnresolvedHint]] - a shape the single-pass
   * Resolver does not support in the first place.
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
    val exposedWiths = new mutable.HashMap[Seq[Long], UnresolvedWith]
    val planWithExposedCteDefinitions = exposeCteDefinitions(plan, exposedWiths)

    // The containing plan's batch runs before nested plans, matching the fixed-point Analyzer:
    // its "Hints" batch completes before ResolveSubquery re-enters the Analyzer. Running the
    // batch first also picks up subqueries that the rules themselves create.
    val planAfterHints = hintResolver.execute(planWithExposedCteDefinitions)

    // The CTE definitions are still children at this point, so subqueries in their bodies are
    // reached as well.
    val planWithResolvedSubqueries = planAfterHints.transformAllExpressionsWithPruning(
      _.containsPattern(PLAN_EXPRESSION)
    ) {
      case subqueryExpression: SubqueryExpression =>
        subqueryExpression.withNewPlan(doResolveWithSubqueries(subqueryExpression.plan))
    }

    restoreUnresolvedWiths(planWithResolvedSubqueries, exposedWiths)
  }

  /**
   * [[UnresolvedWith]] does not expose its CTE definitions as children, so the rules would not
   * reach them on their own. This is the same reason [[MetadataResolver]] matches
   * [[UnresolvedWith]] explicitly. Each [[UnresolvedWith]], nested ones included, is therefore
   * replaced with the [[WithCTE]] that `CTESubstitution` would have produced for it, and recorded
   * in `exposedWiths` under the IDs of its new [[CTERelationDef]]s. The IDs survive the rules
   * copying the [[WithCTE]], unlike tree node tags, and do not match any [[WithCTE]] that was in
   * the plan already.
   */
  private def exposeCteDefinitions(
      plan: LogicalPlan,
      exposedWiths: mutable.Map[Seq[Long], UnresolvedWith]): LogicalPlan =
    plan.transformDownWithPruning(_.containsPattern(UNRESOLVED_WITH)) {
      case unresolvedWith: UnresolvedWith if unresolvedWith.cteRelations.nonEmpty =>
        val cteDefinitions = unresolvedWith.cteRelations.map { cteRelation =>
          CurrentOrigin.withOrigin(cteRelation.plan.origin) {
            CTERelationDef(
              child = cteRelation.plan,
              maxDepth = cteRelation.maxDepth,
              materialized = cteRelation.materialized
            )
          }
        }
        exposedWiths(cteDefinitions.map(_.id)) = unresolvedWith

        WithCTE(plan = unresolvedWith.child, cteDefs = cteDefinitions)
    }

  /**
   * Converts the [[WithCTE]]s created by [[exposeCteDefinitions]] back to the [[UnresolvedWith]]s
   * they were created from, keeping the CTE names, options, origin and tags of the latter.
   *
   * [[UnresolvedCTERelation.plan]] is typed as [[SubqueryAlias]], so a rule that replaced the
   * alias of a CTE definition, or that added or removed CTE definitions, produces a plan that
   * cannot be written back. That is a deliberate limitation of the rule contract rather than full
   * parity: rules that rewrite relations, which is what this hook is used for, are unaffected.
   */
  private def restoreUnresolvedWiths(
      plan: LogicalPlan,
      exposedWiths: mutable.Map[Seq[Long], UnresolvedWith]): LogicalPlan = {
    if (exposedWiths.isEmpty) {
      plan
    } else {
      plan.transformUpWithPruning(_.containsPattern(CTE)) {
        case withCte: WithCTE if exposedWiths.contains(withCte.cteDefs.map(_.id)) =>
          val unresolvedWith = exposedWiths(withCte.cteDefs.map(_.id))
          val newCteRelations = unresolvedWith.cteRelations.zip(withCte.cteDefs).map {
            case (cteRelation, CTERelationDef(cteDefinition: SubqueryAlias, _, _, _, _, _, _)) =>
              cteRelation.copy(plan = cteDefinition)
            case (cteRelation, _) =>
              throw SparkException.internalError(
                s"A hint resolution rule replaced the SubqueryAlias of CTE ${cteRelation.name}."
              )
          }

          val newUnresolvedWith = CurrentOrigin.withOrigin(unresolvedWith.origin) {
            unresolvedWith.copy(child = withCte.plan, cteRelations = newCteRelations)
          }
          newUnresolvedWith.copyTagsFrom(unresolvedWith)
          newUnresolvedWith

        case withCte: WithCTE
            if withCte.cteDefs.exists(cteDefinition =>
              exposedWiths.keysIterator.exists(_.contains(cteDefinition.id))) =>
          throw SparkException.internalError(
            "A hint resolution rule added or removed CTE definitions."
          )
      }
    }
  }
}
