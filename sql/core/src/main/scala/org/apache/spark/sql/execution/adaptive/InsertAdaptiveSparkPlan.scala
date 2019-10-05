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

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

import org.apache.spark.sql.{execution, SparkSession}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule wraps the query plan with an [[AdaptiveSparkPlanExec]], which executes the query plan
 * and re-optimize the plan during execution based on runtime data statistics.
 *
 * Note that this rule is stateful and thus should not be reused across query executions.
 */
case class InsertAdaptiveSparkPlan(
    session: SparkSession,
    queryExecution: QueryExecution) extends Rule[SparkPlan] {

  private val conf = session.sessionState.conf

  // Subquery-reuse is shared across the entire query.
  private val subqueryCache = new TrieMap[SparkPlan, BaseSubqueryExec]()

  // Exchange-reuse is shared across the entire query, including sub-queries.
  private val stageCache = new TrieMap[SparkPlan, QueryStageExec]()

  override def apply(plan: SparkPlan): SparkPlan = applyInternal(plan, queryExecution)

  private def applyInternal(plan: SparkPlan, qe: QueryExecution): SparkPlan = plan match {
    case _: ExecutedCommandExec => plan
    case _ if conf.adaptiveExecutionEnabled && supportAdaptive(plan) =>
      try {
        // Plan sub-queries recursively and pass in the shared stage cache for exchange reuse. Fall
        // back to non-adaptive mode if adaptive execution is supported in any of the sub-queries.
        val subqueryMap = buildSubqueryMap(plan)
        val planSubqueriesRule = PlanAdaptiveSubqueries(subqueryMap)
        val preprocessingRules = Seq(
          planSubqueriesRule)
        // Run pre-processing rules.
        val newPlan = AdaptiveSparkPlanExec.applyPhysicalRules(plan, preprocessingRules)
        logDebug(s"Adaptive execution enabled for plan: $plan")
        AdaptiveSparkPlanExec(newPlan, session, preprocessingRules, subqueryCache, stageCache, qe)
      } catch {
        case SubqueryAdaptiveNotSupportedException(subquery) =>
          logWarning(s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} is enabled " +
            s"but is not supported for sub-query: $subquery.")
          plan
      }
    case _ =>
      if (conf.adaptiveExecutionEnabled) {
        logWarning(s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} is enabled " +
          s"but is not supported for query: $plan.")
      }
      plan
  }

  private def supportAdaptive(plan: SparkPlan): Boolean = {
    // TODO migrate dynamic-partition-pruning onto adaptive execution.
    sanityCheck(plan) &&
      !plan.logicalLink.exists(_.isStreaming) &&
      !plan.expressions.exists(_.find(_.isInstanceOf[DynamicPruningSubquery]).isDefined) &&
    plan.children.forall(supportAdaptive)
  }

  private def sanityCheck(plan: SparkPlan): Boolean =
    plan.logicalLink.isDefined

  /**
   * Returns an expression-id-to-execution-plan map for all the sub-queries.
   * For each sub-query, generate the adaptive execution plan for each sub-query by applying this
   * rule, or reuse the execution plan from another sub-query of the same semantics if possible.
   */
  private def buildSubqueryMap(plan: SparkPlan): mutable.HashMap[Long, ExecSubqueryExpression] = {
    val subqueryMap = mutable.HashMap.empty[Long, ExecSubqueryExpression]
    plan.foreach(_.expressions.foreach(_.foreach {
      case expressions.ScalarSubquery(p, _, exprId)
          if !subqueryMap.contains(exprId.id) =>
        val executedPlan = compileSubquery(p)
        verifyAdaptivePlan(executedPlan, p)
        val scalarSubquery = execution.ScalarSubquery(
          SubqueryExec(s"subquery${exprId.id}", executedPlan), exprId)
        subqueryMap.put(exprId.id, scalarSubquery)
      case _ =>
    }))

    subqueryMap
  }

  def compileSubquery(plan: LogicalPlan): SparkPlan = {
    val queryExec = new QueryExecution(session, plan)
    // Apply the same instance of this rule to sub-queries so that sub-queries all share the
    // same `stageCache` for Exchange reuse.
    this.applyInternal(queryExec.sparkPlan, queryExec)
  }

  private def verifyAdaptivePlan(plan: SparkPlan, logicalPlan: LogicalPlan): Unit = {
    if (!plan.isInstanceOf[AdaptiveSparkPlanExec]) {
      throw SubqueryAdaptiveNotSupportedException(logicalPlan)
    }
  }
}

private case class SubqueryAdaptiveNotSupportedException(plan: LogicalPlan) extends Exception {}
