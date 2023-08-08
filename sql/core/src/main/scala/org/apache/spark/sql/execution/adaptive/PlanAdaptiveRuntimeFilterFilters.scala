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
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{RUNTIME_FILTER_EXPRESSION, SUBQUERY_WRAPPER}
import org.apache.spark.sql.execution.{QueryExecution, ScalarSubquery, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryBroadcastExec, SubqueryExec, SubqueryWrapper}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeExecProxy, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, HashJoin}

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
        val executedFilterCreationSidePlan =
          QueryExecution.prepareExecutedPlan(adaptivePlan.session, filterCreationSidePlan)
        val packedKeys = BindReferences.bindReferences(
          HashJoin.rewriteKeyExpr(buildKeys), executedFilterCreationSidePlan.output)
        val mode = HashedRelationBroadcastMode(packedKeys)
        // plan a broadcast exchange of the build side of the join
        val exchange = BroadcastExchangeExec(mode, executedFilterCreationSidePlan)

        val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          find(rootPlan) {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
              if (left.isInstanceOf[BroadcastQueryStageExec]) {
                left.asInstanceOf[BroadcastQueryStageExec].plan.sameResult(exchange)
              } else {
                left.sameResult(exchange)
              }
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
              if (right.isInstanceOf[BroadcastQueryStageExec]) {
                right.asInstanceOf[BroadcastQueryStageExec].plan.sameResult(exchange)
              } else {
                right.sameResult(exchange)
              }
            case _ => false
          }.isDefined

        val bloomFilterSubquery = if (canReuseExchange) {
          exchange.setLogicalLink(filterCreationSidePlan.logicalLink.get)

          val broadcastValues = SubqueryBroadcastExec(name, index, buildKeys, exchange)
          val broadcastProxy =
            BroadcastExchangeExecProxy(broadcastValues, executedFilterCreationSidePlan.output)

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
    plan match {
      case objectHashAggregate: ObjectHashAggregateExec =>
        getFilterCreationSidePlan(objectHashAggregate.child)
      case shuffleExchange: ShuffleExchangeExec =>
        getFilterCreationSidePlan(shuffleExchange.child)
      case queryStageExec: ShuffleQueryStageExec =>
        getFilterCreationSidePlan(queryStageExec.plan)
      case other => other
    }
  }
}
