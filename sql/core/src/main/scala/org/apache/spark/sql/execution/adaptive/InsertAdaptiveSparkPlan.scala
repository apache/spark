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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.types.StructType

/**
 * This rule wraps the query plan with an [[AdaptiveSparkPlanExec]], which executes the query plan
 * and re-optimize the plan during execution based on runtime data statistics.
 */
case class InsertAdaptiveSparkPlan(session: SparkSession) extends Rule[SparkPlan] {

  private val conf = session.sessionState.conf

  // Exchange-reuse is shared across the entire query, including sub-queries.
  private val stageCache = new TrieMap[StructType, mutable.Buffer[(Exchange, QueryStageExec)]]()

  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case _: ExecutedCommandExec => plan
    case _ if conf.runtimeReoptimizationEnabled
      && supportAdaptive(plan) =>
      try {
        // Plan sub-queries recursively and pass in the shared stage cache for exchange reuse. Fall
        // back to non-adaptive mode if adaptive execution is supported in any of the sub-queries.
        val subqueryMap = planSubqueries(plan)
        // Run preparation rules.
        val preparations = AdaptiveSparkPlanExec.createQueryStagePreparationRules(
          session.sessionState.conf, subqueryMap)
        val newPlan = AdaptiveSparkPlanExec.applyPhysicalRules(plan, preparations)
        logDebug(s"Adaptive execution enabled for plan: $plan")
        AdaptiveSparkPlanExec(newPlan, session.cloneSession(), subqueryMap, stageCache)
      } catch {
        case _: SubqueryAdaptiveNotSupportedException =>
          plan
      }
    case _ => plan
  }

  private def supportAdaptive(plan: SparkPlan): Boolean = {
    sanityCheck(plan) &&
      !plan.logicalLink.exists(_.isStreaming) &&
      plan.children.forall(supportAdaptive)
  }

  private def sanityCheck(plan: SparkPlan): Boolean = plan match {
    case _: SortExec | _: ShuffleExchangeExec => true
    case _ => plan.logicalLink.isDefined
  }

  private def planSubqueries(plan: SparkPlan): Map[Long, ExecSubqueryExpression] = {
    val subqueryMapBuilder = mutable.HashMap.empty[Long, ExecSubqueryExpression]
    plan.foreach(_.expressions.foreach(_.foreach {
      case expressions.ScalarSubquery(p, _, exprId)
          if !subqueryMapBuilder.contains(exprId.id) =>
        val executedPlan = getExecutedPlan(p)
        val scalarSubquery = ScalarSubquery(
          SubqueryExec(s"subquery${exprId.id}", executedPlan), exprId)
        subqueryMapBuilder.put(exprId.id, scalarSubquery)
      case _ =>
    }))

    // Reuse subqueries
    if (session.sessionState.conf.subqueryReuseEnabled) {
      // Build a hash map using schema of subqueries to avoid O(N*N) sameResult calls.
      val reuseMap = mutable.HashMap[StructType, mutable.ArrayBuffer[BaseSubqueryExec]]()
      subqueryMapBuilder.keySet.foreach { exprId =>
        val sub = subqueryMapBuilder.get(exprId).get
        val sameSchema =
          reuseMap.getOrElseUpdate(sub.plan.schema, mutable.ArrayBuffer.empty)
        val sameResult = sameSchema.find(_.sameResult(sub.plan))
        if (sameResult.isDefined) {
          val newExpr = sub.withNewPlan(ReusedSubqueryExec(sameResult.get))
          subqueryMapBuilder.update(exprId, newExpr.asInstanceOf[ExecSubqueryExpression])
        } else {
          sameSchema += sub.plan
        }
      }
    }

    subqueryMapBuilder.toMap
  }

  private def getExecutedPlan(plan: LogicalPlan): SparkPlan = {
    val queryExec = new QueryExecution(session, plan)
    val adaptivePlan = this.apply(queryExec.sparkPlan)
    if (!adaptivePlan.isInstanceOf[AdaptiveSparkPlanExec]) {
      throw new SubqueryAdaptiveNotSupportedException(plan)
    }
    adaptivePlan
  }
}

private class SubqueryAdaptiveNotSupportedException(plan: LogicalPlan) extends Exception {}
