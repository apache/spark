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

import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Literal}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._

case class InsertDynamicPruningFilters(
    stageCache: TrieMap[SparkPlan, QueryStageExec]) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan transformAllExpressions {
      case DynamicPruningExpression(InSubqueryExec(
      value, SubqueryAdaptiveBroadcastExec(
      name, index, buildKeys, adaptivePlan: AdaptiveSparkPlanExec, exchange), exprId, _)) =>

        val existingStage = stageCache.get(exchange.canonicalized)
        if (existingStage.nonEmpty && conf.exchangeReuseEnabled) {
          val name = s"dynamicpruning#${exprId.id}"

          val reuseQueryStage = existingStage.get.newReuseInstance(
            adaptivePlan.stageId, exchange.output)
          adaptivePlan.setStageId(adaptivePlan.stageId + 1)

          // Set the logical link for the reuse query stage.
          val link = exchange.getTagValue(AdaptiveSparkPlanExec.TEMP_LOGICAL_PLAN_TAG).orElse(
            exchange.logicalLink.orElse(exchange.collectFirst {
              case p if p.getTagValue(AdaptiveSparkPlanExec.TEMP_LOGICAL_PLAN_TAG).isDefined =>
                p.getTagValue(AdaptiveSparkPlanExec.TEMP_LOGICAL_PLAN_TAG).get
              case p if p.logicalLink.isDefined => p.logicalLink.get
            }))
          assert(link.isDefined)
          reuseQueryStage.setLogicalLink(link.get)

          val broadcastValues =
            SubqueryBroadcastExec(name, index, buildKeys, reuseQueryStage)
          DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
        } else {
          DynamicPruningExpression(Literal.TrueLiteral)
        }
    }
  }
}
