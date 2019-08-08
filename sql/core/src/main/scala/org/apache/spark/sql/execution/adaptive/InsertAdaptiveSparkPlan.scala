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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

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
        // Run preparation rules.
        val preparations = AdaptiveSparkPlanExec.createQueryStagePreparationRules(
          session.sessionState.conf, subqueryMap)
        val newPlan = AdaptiveSparkPlanExec.applyPhysicalRules(plan, preparations)
        logDebug(s"Adaptive execution enabled for plan: $plan")
        AdaptiveSparkPlanExec(newPlan, session, subqueryMap, stageCache, qe)
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
  private def buildSubqueryMap(plan: SparkPlan): Map[Long, ExecSubqueryExpression] = {
    val subqueryMapBuilder = mutable.HashMap.empty[Long, ExecSubqueryExpression]
    plan.foreach(_.expressions.foreach(_.foreach {
      case expressions.ScalarSubquery(p, _, exprId)
          if !subqueryMapBuilder.contains(exprId.id) =>
        val executedPlan = getExecutedPlan(p)
        val scalarSubquery = execution.ScalarSubquery(
          SubqueryExec(s"subquery${exprId.id}", executedPlan), exprId)
        subqueryMapBuilder.put(exprId.id, scalarSubquery)
      case _ =>
    }))

    // Reuse subqueries
    if (session.sessionState.conf.subqueryReuseEnabled) {
      // Build a hash map using schema of subqueries to avoid O(N*N) sameResult calls.
      val reuseMap = mutable.HashMap[StructType, mutable.ArrayBuffer[BaseSubqueryExec]]()
      subqueryMapBuilder.keySet.foreach { exprId =>
        val sub = subqueryMapBuilder(exprId)
        val sameSchema =
          reuseMap.getOrElseUpdate(sub.plan.schema, mutable.ArrayBuffer.empty)
        val sameResult = sameSchema.find(_.sameResult(sub.plan))
        if (sameResult.isDefined) {
          val newExpr = sub.withNewPlan(ReusedSubqueryExec(sameResult.get))
          subqueryMapBuilder.update(exprId, newExpr)
        } else {
          sameSchema += sub.plan
        }
      }
    }

    subqueryMapBuilder.toMap
  }

  private def getExecutedPlan(plan: LogicalPlan): SparkPlan = {
    val queryExec = new QueryExecution(session, plan)
    // Apply the same instance of this rule to sub-queries so that sub-queries all share the
    // same `stageCache` for Exchange reuse.
    val adaptivePlan = this.applyInternal(queryExec.sparkPlan, queryExec)
    if (!adaptivePlan.isInstanceOf[AdaptiveSparkPlanExec]) {
      throw SubqueryAdaptiveNotSupportedException(plan)
    }
    adaptivePlan
  }
}

private case class SubqueryAdaptiveNotSupportedException(plan: LogicalPlan) extends Exception {}
