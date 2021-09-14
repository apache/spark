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

package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{ListQuery, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{DYNAMIC_PRUNING_SUBQUERY, IN_SUBQUERY, SCALAR_SUBQUERY}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule wraps the query plan with an [[AdaptiveSparkPlanExec]], which executes the query plan
 * and re-optimize the plan during execution based on runtime data statistics.
 *
 * Note that this rule is stateful and thus should not be reused across query executions.
 */
case class InsertAdaptiveSparkPlan(
    adaptiveExecutionContext: AdaptiveExecutionContext) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = applyInternal(plan, false)

  private def applyInternal(plan: SparkPlan, isSubquery: Boolean): SparkPlan = plan match {
    case _ if !conf.adaptiveExecutionEnabled => plan
    case _: ExecutedCommandExec => plan
    case _: CommandResultExec => plan
    case c: DataWritingCommandExec => c.copy(child = apply(c.child))
    case c: V2CommandExec => c.withNewChildren(c.children.map(apply))
    case _ if shouldApplyAQE(plan, isSubquery) =>
      if (supportAdaptive(plan)) {
        try {
          // Plan sub-queries recursively and pass in the shared stage cache for exchange reuse.
          // Fall back to non-AQE mode if AQE is not supported in any of the sub-queries.
          val subqueryMap = buildSubqueryMap(plan)
          val planSubqueriesRule = PlanAdaptiveSubqueries(subqueryMap)
          val preprocessingRules = Seq(
            planSubqueriesRule)
          // Run pre-processing rules.
          val newPlan = AdaptiveSparkPlanExec.applyPhysicalRules(plan, preprocessingRules)
          logDebug(s"Adaptive execution enabled for plan: $plan")
          AdaptiveSparkPlanExec(newPlan, adaptiveExecutionContext, preprocessingRules, isSubquery)
        } catch {
          case SubqueryAdaptiveNotSupportedException(subquery) =>
            logWarning(s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} is enabled " +
              s"but is not supported for sub-query: $subquery.")
            plan
        }
      } else {
        logWarning(s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} is enabled " +
          s"but is not supported for query: $plan.")
        plan
      }

    case _ => plan
  }

  // AQE is only useful when the query has exchanges or sub-queries. This method returns true if
  // one of the following conditions is satisfied:
  //   - The config ADAPTIVE_EXECUTION_FORCE_APPLY is true.
  //   - The input query is from a sub-query. When this happens, it means we've already decided to
  //     apply AQE for the main query and we must continue to do it.
  //   - The query contains exchanges.
  //   - The query may need to add exchanges. It's an overkill to run `EnsureRequirements` here, so
  //     we just check `SparkPlan.requiredChildDistribution` and see if it's possible that the
  //     the query needs to add exchanges later.
  //   - The query contains sub-query.
  private def shouldApplyAQE(plan: SparkPlan, isSubquery: Boolean): Boolean = {
    conf.getConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY) || isSubquery || {
      plan.find {
        case _: Exchange => true
        case p if !p.requiredChildDistribution.forall(_ == UnspecifiedDistribution) => true
        case p => p.expressions.exists(_.find {
          case _: SubqueryExpression => true
          case _ => false
        }.isDefined)
      }.isDefined
    }
  }

  private def supportAdaptive(plan: SparkPlan): Boolean = {
    sanityCheck(plan) &&
      !plan.logicalLink.exists(_.isStreaming) &&
    plan.children.forall(supportAdaptive)
  }

  private def sanityCheck(plan: SparkPlan): Boolean =
    plan.logicalLink.isDefined

  /**
   * Returns an expression-id-to-execution-plan map for all the sub-queries.
   * For each sub-query, generate the adaptive execution plan for each sub-query by applying this
   * rule, or reuse the execution plan from another sub-query of the same semantics if possible.
   */
  private def buildSubqueryMap(plan: SparkPlan): Map[Long, BaseSubqueryExec] = {
    val subqueryMap = mutable.HashMap.empty[Long, BaseSubqueryExec]
    if (!plan.containsAnyPattern(SCALAR_SUBQUERY, IN_SUBQUERY, DYNAMIC_PRUNING_SUBQUERY)) {
      return subqueryMap.toMap
    }
    plan.foreach(_.expressions.foreach(_.foreach {
      case expressions.ScalarSubquery(p, _, exprId, _)
          if !subqueryMap.contains(exprId.id) =>
        val executedPlan = compileSubquery(p)
        verifyAdaptivePlan(executedPlan, p)
        val subquery = SubqueryExec.createForScalarSubquery(
          s"subquery#${exprId.id}", executedPlan)
        subqueryMap.put(exprId.id, subquery)
      case expressions.InSubquery(_, ListQuery(query, _, exprId, _, _))
          if !subqueryMap.contains(exprId.id) =>
        val executedPlan = compileSubquery(query)
        verifyAdaptivePlan(executedPlan, query)
        val subquery = SubqueryExec(s"subquery#${exprId.id}", executedPlan)
        subqueryMap.put(exprId.id, subquery)
      case expressions.DynamicPruningSubquery(value, buildPlan,
          buildKeys, broadcastKeyIndex, onlyInBroadcast, exprId)
          if !subqueryMap.contains(exprId.id) =>
        val executedPlan = compileSubquery(buildPlan)
        verifyAdaptivePlan(executedPlan, buildPlan)

        val name = s"dynamicpruning#${exprId.id}"
        val subquery = SubqueryAdaptiveBroadcastExec(
          name, broadcastKeyIndex, onlyInBroadcast,
          buildPlan, buildKeys, executedPlan)
        subqueryMap.put(exprId.id, subquery)
      case _ =>
    }))

    subqueryMap.toMap
  }

  def compileSubquery(plan: LogicalPlan): SparkPlan = {
    // Apply the same instance of this rule to sub-queries so that sub-queries all share the
    // same `stageCache` for Exchange reuse.
    this.applyInternal(
      QueryExecution.createSparkPlan(adaptiveExecutionContext.session,
        adaptiveExecutionContext.session.sessionState.planner, plan.clone()), true)
  }

  private def verifyAdaptivePlan(plan: SparkPlan, logicalPlan: LogicalPlan): Unit = {
    if (!plan.isInstanceOf[AdaptiveSparkPlanExec]) {
      throw SubqueryAdaptiveNotSupportedException(logicalPlan)
    }
  }
}

private case class SubqueryAdaptiveNotSupportedException(plan: LogicalPlan) extends Exception {}
