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
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSeq, BindReferences, DynamicPartitionPruningSubquery, DynamicPruningExpression, DynamicShufflePruningSubquery, Expression, ListQuery, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{BloomFilterSubqueryExec, InSubqueryExec, QueryExecution, SparkPlan, SubqueryBroadcastExec, SubqueryExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.types.{AtomicType, BooleanType, ByteType, ShortType}

/**
 * This planner rule aims at rewriting dynamic pruning predicates in order to reuse the
 * results of broadcast. For joins that are not planned as broadcast hash joins we keep
 * the fallback mechanism with subquery duplicate.
*/
case class PlanDynamicPruningFilters(sparkSession: SparkSession)
    extends Rule[SparkPlan] with PredicateHelper {

  private val sqlConf = sparkSession.sessionState.conf

  private def preferBloomFilter(buildKey: Expression, buildPlan: LogicalPlan): Boolean = {
    buildKey.dataType match {
      case BooleanType | ByteType | ShortType => false
      case _: AtomicType =>
        val sizePerRow = EstimationUtils.getSizePerRow(buildKey.references.toSeq)
        val sizeInBytes = buildPlan.stats.sizeInBytes
        val rowCount = buildPlan.references.map {
          attr => buildPlan.stats.attributeStats.get(attr).flatMap(_.distinctCount)
        }.max.orElse(buildPlan.stats.rowCount).getOrElse(sizeInBytes / sizePerRow).toLong
        rowCount > sqlConf.dynamicPartitionPruningBloomFilterThreshold
      case _ => false
    }
  }

  /**
   * Identify the shape in which keys of a given plan are broadcasted.
   */
  private def broadcastMode(keys: Seq[Expression], output: AttributeSeq): BroadcastMode = {
    val packedKeys = BindReferences.bindReferences(HashJoin.rewriteKeyExpr(keys), output)
    HashedRelationBroadcastMode(packedKeys)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!sqlConf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan transformAllExpressions {
      case DynamicPartitionPruningSubquery(
          value, buildPlan, buildKeys, broadcastKeyIndex, onlyInBroadcast, exprId) =>
        val sparkPlan = QueryExecution.createSparkPlan(
          sparkSession, sparkSession.sessionState.planner, buildPlan)
        // Using `sparkPlan` is a little hacky as it is based on the assumption that this rule is
        // the first to be applied (apart from `InsertAdaptiveSparkPlan`).
        val canReuseExchange = sqlConf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          plan.find {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
              left.sameResult(sparkPlan)
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
              right.sameResult(sparkPlan)
            case _ => false
          }.isDefined

        if (canReuseExchange) {
          val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, sparkPlan)
          val mode = broadcastMode(buildKeys, executedPlan.output)
          // plan a broadcast exchange of the build side of the join
          val exchange = BroadcastExchangeExec(mode, executedPlan)
          val name = s"dynamicpruning#${exprId.id}"
          // place the broadcast adaptor for reusing the broadcast results on the probe side
          val broadcastValues = SubqueryBroadcastExec(name, broadcastKeyIndex, buildKeys, exchange)
          if (preferBloomFilter(buildKeys(broadcastKeyIndex), buildPlan)) {
            DynamicPruningExpression(BloomFilterSubqueryExec(value, broadcastValues, exprId))
          } else {
            DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
          }
        } else if (onlyInBroadcast) {
          // it is not worthwhile to execute the query, so we fall-back to a true literal
          DynamicPruningExpression(Literal.TrueLiteral)
        } else {
          // we need to apply an aggregate on the buildPlan in order to be column pruned
          val alias = Alias(buildKeys(broadcastKeyIndex), buildKeys(broadcastKeyIndex).toString)()
          val aggregate = Aggregate(Seq(alias), Seq(alias), buildPlan)
          if (preferBloomFilter(buildKeys(broadcastKeyIndex), buildPlan)) {
            val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, aggregate)
            val name = s"dynamicpruning#${exprId.id}"
            DynamicPruningExpression(
              BloomFilterSubqueryExec(value, SubqueryExec(name, executedPlan), exprId))
          } else {
            DynamicPruningExpression(expressions.InSubquery(
              Seq(value), ListQuery(aggregate, childOutputs = aggregate.output)))
          }
        }

      case DynamicShufflePruningSubquery(
          value, buildPlan, buildKeys, broadcastKeyIndex, onlyInBroadcast, exprId) =>
        val sparkPlan = QueryExecution.createSparkPlan(
          sparkSession, sparkSession.sessionState.planner, buildPlan)
        // Using `sparkPlan` is a little hacky as it is based on the assumption that this rule is
        // the first to be applied (apart from `InsertAdaptiveSparkPlan`).
        val canReuseExchange = sqlConf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          plan.find {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
              left.sameResult(sparkPlan)
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
              right.sameResult(sparkPlan)
            case _ => false
          }.isDefined

        if (canReuseExchange) {
          val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, sparkPlan)
          val mode = broadcastMode(buildKeys, executedPlan.output)
          // plan a broadcast exchange of the build side of the join
          val exchange = BroadcastExchangeExec(mode, executedPlan)
          val name = s"dynamicpruning#${exprId.id}"
          // place the broadcast adaptor for reusing the broadcast results on the probe side
          val broadcastValues = SubqueryBroadcastExec(name, broadcastKeyIndex, buildKeys, exchange)
          if (preferBloomFilter(buildKeys(broadcastKeyIndex), buildPlan)) {
            DynamicPruningExpression(BloomFilterSubqueryExec(value, broadcastValues, exprId))
          } else {
            DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
          }
        } else {
          DynamicPruningExpression(Literal.TrueLiteral)
        }
    }
  }
}
