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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSeq, BindReferences, DynamicPruningExpression, DynamicPruningSubquery, Expression, ListQuery, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.aggregate.{BuildBloomFilter, Max, Min}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, RepartitionByExpression}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{InSubqueryExec, MixedFilterSubqueryExec, QueryExecution, SparkPlan, SubqueryBroadcastExec, SubqueryExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.SQLConf

/**
 * This planner rule aims at rewriting dynamic pruning predicates in order to reuse the
 * results of broadcast. For joins that are not planned as broadcast hash joins we keep
 * the fallback mechanism with subquery duplicate.
*/
case class PlanDynamicPruningFilters(sparkSession: SparkSession)
    extends Rule[SparkPlan] with PredicateHelper with JoinSelectionHelper {

  private val conf = SQLConf.get

  /**
   * Identify the shape in which keys of a given plan are broadcasted.
   */
  private def broadcastMode(keys: Seq[Expression], output: AttributeSeq): BroadcastMode = {
    val packedKeys = BindReferences.bindReferences(HashJoin.rewriteKeyExpr(keys), output)
    HashedRelationBroadcastMode(packedKeys)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan transformAllExpressions {
      case DynamicPruningSubquery(
          value, buildPlan, buildKeys, broadcastKeyIndex, onlyInBroadcast, exprId) =>
        val sparkPlan = QueryExecution.createSparkPlan(
          sparkSession, sparkSession.sessionState.planner, buildPlan)
        // Using `sparkPlan` is a little hacky as it is based on the assumption that this rule is
        // the first to be applied (apart from `InsertAdaptiveSparkPlan`).
        val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          plan.find {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
              left.sameResult(sparkPlan)
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
              right.sameResult(sparkPlan)
            case _ => false
          }.isDefined
        val name = s"dynamicpruning#${exprId.id}"

        if (canReuseExchange) {
          val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, sparkPlan)
          val mode = broadcastMode(buildKeys, executedPlan.output)
          // plan a broadcast exchange of the build side of the join
          val exchange = BroadcastExchangeExec(mode, executedPlan)
          // place the broadcast adaptor for reusing the broadcast results on the probe side
          val broadcastValues =
            SubqueryBroadcastExec(name, broadcastKeyIndex, buildKeys, exchange)
          DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
        } else if (onlyInBroadcast) {
          // it is not worthwhile to execute the query, so we fall-back to a true literal
          DynamicPruningExpression(Literal.TrueLiteral)
        } else {
          val buildKey = buildKeys(broadcastKeyIndex)
          if (BuildBloomFilter.isSupportBuildBloomFilter(buildKey) &&
            !canBroadcastBySize(buildPlan, conf)) {
            val sizePerRow = EstimationUtils.getSizePerRow(buildKey.references.toSeq)
            val sizeInBytes = buildPlan.stats.sizeInBytes
            val rowCount = buildPlan.references.map {
              attr => buildPlan.stats.attributeStats.get(attr).flatMap(_.distinctCount)
            }.max.orElse(buildPlan.stats.rowCount).getOrElse(sizeInBytes / sizePerRow).toLong
            val maxBloomFilterEntries = conf.dynamicPartitionPruningMaxBloomFilterEntries
            val expectedNumItems = math.max(math.min(rowCount, maxBloomFilterEntries), 1L)
            val aggregateExpressions = buildKey.flatMap { e =>
              Seq(
                Alias(Min(e).toAggregateExpression(), s"min_${e.toString}")(),
                Alias(Max(e).toAggregateExpression(), s"max_${e.toString}")(),
                Alias(BuildBloomFilter(e, expectedNumItems).toAggregateExpression(),
                  s"bloom_filter${e.toString}")()
              )
            }
            // Adding RepartitionByExpression may reuse exchange
            val withRepartition = if (buildKeys.size == 1 && !canBroadcastBySize(buildPlan, conf)) {
              RepartitionByExpression(Seq(buildKey), buildPlan, None)
            } else {
              buildPlan
            }

            val aggregate = Aggregate(Nil, aggregateExpressions, withRepartition)
            val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, aggregate)
            DynamicPruningExpression(
              MixedFilterSubqueryExec(value, SubqueryExec(name, executedPlan), exprId))
          } else {
            // we need to apply an aggregate on the buildPlan in order to be column pruned
            val alias = Alias(buildKey, buildKey.toString)()
            val aggregate = Aggregate(Seq(alias), Seq(alias), buildPlan)
            DynamicPruningExpression(expressions.InSubquery(
              Seq(value), ListQuery(aggregate, childOutputs = aggregate.output)))
          }
        }
    }
  }
}
