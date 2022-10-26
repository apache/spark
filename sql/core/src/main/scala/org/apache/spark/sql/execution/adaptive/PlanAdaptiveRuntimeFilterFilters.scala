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

import org.apache.spark.sql.catalyst.expressions.{BindReferences, RuntimeFilterExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{RUNTIME_FILTER_EXPRESSION, SUBQUERY_WRAPPER}
import org.apache.spark.sql.execution.{ScalarSubquery, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryExec, SubqueryWrapper}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeExecProxy, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{HashedRelationBroadcastMode, HashJoin}

/**
 * A rule to insert runtime filter in order to reuse the results of broadcast.
 */
case class PlanAdaptiveRuntimeFilterFilters(
    rootPlan: AdaptiveSparkPlanExec) extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.runtimeFilterBloomFilterEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(
      _.containsAllPatterns(RUNTIME_FILTER_EXPRESSION, SUBQUERY_WRAPPER)) {
      case RuntimeFilterExpression(SubqueryWrapper(
          SubqueryAdaptiveBroadcastExec(name, index, true, _, buildKeys,
          adaptivePlan: AdaptiveSparkPlanExec), exprId)) =>
        val filterCreationSidePlan = getFilterCreationSidePlan(adaptivePlan.executedPlan)
        val packedKeys = BindReferences.bindReferences(
          HashJoin.rewriteKeyExpr(buildKeys), filterCreationSidePlan.output)
        val mode = HashedRelationBroadcastMode(packedKeys)
        // plan a broadcast exchange of the build side of the join
        val exchange = BroadcastExchangeExec(mode, filterCreationSidePlan)

        val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          rootPlan.context.exchangeCache.contains(exchange.canonicalized)

        val newExchange =
          rootPlan.context.exchangeCache.get(exchange.canonicalized).getOrElse(exchange)

        val bloomFilterSubquery = if (canReuseExchange && newExchange.supportsRowBased) {
          exchange.setLogicalLink(filterCreationSidePlan.logicalLink.get)
          val reusedExchange = ReusedExchangeExec(filterCreationSidePlan.output, newExchange)
          val broadcastProxy = BroadcastExchangeExecProxy(name, index, buildKeys, reusedExchange)

          val newExecutedPlan = adaptivePlan.executedPlan transformUp {
            case hashAggregateExec: ObjectHashAggregateExec
              if hashAggregateExec.child.eq(filterCreationSidePlan) =>
              hashAggregateExec.copy(child = broadcastProxy)
          }
          val newAdaptivePlan = adaptivePlan.copy(inputPlan = newExecutedPlan)

          ScalarSubquery(
            SubqueryExec.createForScalarSubquery(
              s"scalar-subquery#${exprId.id}",
              newAdaptivePlan), exprId)
        } else {
          ScalarSubquery(
            SubqueryExec.createForScalarSubquery(
              s"scalar-subquery#${exprId.id}",
              adaptivePlan), exprId)
        }

        RuntimeFilterExpression(bloomFilterSubquery)
    }
  }

  private def getFilterCreationSidePlan(plan: SparkPlan): SparkPlan = {
    assert(plan.isInstanceOf[ObjectHashAggregateExec])
    plan.asInstanceOf[ObjectHashAggregateExec].child match {
      case objectHashAggregate: ObjectHashAggregateExec =>
        objectHashAggregate.child
      case shuffleExchange: ShuffleExchangeExec =>
        shuffleExchange.child.asInstanceOf[ObjectHashAggregateExec].child
      case other => other
    }
  }
}
