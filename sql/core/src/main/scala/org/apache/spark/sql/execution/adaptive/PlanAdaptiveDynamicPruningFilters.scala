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

import org.apache.spark.sql.catalyst.expressions.{Alias, BindReferences, BloomFilterMightContain, DynamicPruningExpression, Literal, XxHash64}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, RepartitionByExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.{ScalarSubquery => ScalarSubqueryExec}
import org.apache.spark.sql.execution.aggregate.AggUtils
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, HashJoin}

/**
 * A rule to insert dynamic pruning predicates in order to reuse the results of broadcast.
 */
case class PlanAdaptiveDynamicPruningFilters(
    rootPlan: AdaptiveSparkPlanExec) extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled && !conf.runtimeRowLevelOperationGroupFilterEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(
      _.containsAllPatterns(DYNAMIC_PRUNING_EXPRESSION, IN_SUBQUERY_EXEC)) {
      case DynamicPruningExpression(InSubqueryExec(
          value, SubqueryAdaptiveBroadcastExec(name, index, onlyInBroadcast, buildPlan, buildKeys,
          adaptivePlan: AdaptiveSparkPlanExec), exprId, _, _, _)) =>
        val packedKeys = BindReferences.bindReferences(
          HashJoin.rewriteKeyExpr(buildKeys), adaptivePlan.executedPlan.output)
        val mode = HashedRelationBroadcastMode(packedKeys)
        // plan a broadcast exchange of the build side of the join
        val exchange = BroadcastExchangeExec(mode, adaptivePlan.executedPlan)

        val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          find(rootPlan) {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
              left.sameResult(exchange)
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
              right.sameResult(exchange)
            case _ => false
          }.isDefined

        if (canReuseExchange) {
          exchange.setLogicalLink(adaptivePlan.executedPlan.logicalLink.get)
          val newAdaptivePlan = adaptivePlan.copy(inputPlan = exchange)

          val broadcastValues = SubqueryBroadcastExec(
            name, index, buildKeys, newAdaptivePlan)
          DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
        } else if (onlyInBroadcast) {
          DynamicPruningExpression(Literal.TrueLiteral)
        } else {
          val childPlan = adaptivePlan.executedPlan
          val reusedShuffleExchange = collectFirst(rootPlan) {
            case s: ShuffleExchangeExec if s.child.sameResult(childPlan) => s
          }

          val rowCount = buildPlan.stats.rowCount
          val bloomFilterAgg = if (rowCount.exists(_.longValue() > 0L)) {
            new BloomFilterAggregate(new XxHash64(buildKeys(index)),
              Literal(rowCount.get.longValue))
          } else {
            new BloomFilterAggregate(new XxHash64(buildKeys(index)))
          }

          val bfLogicalPlan = Aggregate(Nil,
            Seq(Alias(bloomFilterAgg.toAggregateExpression(), "bloomFilter")()),
            RepartitionByExpression(buildKeys, buildPlan, Some(conf.numShufflePartitions)))

          val physicalAggregation = PhysicalAggregation.unapply(bfLogicalPlan)
          if (reusedShuffleExchange.nonEmpty && physicalAggregation.nonEmpty) {

            val (groupingExps, aggExps, resultExps, _) = physicalAggregation.get
            val bfPhysicalPlan = AggUtils.planAggregateWithoutDistinct(
              groupingExps,
              aggExps.map(_.asInstanceOf[AggregateExpression]),
              resultExps,
              reusedShuffleExchange.get).head

            bfPhysicalPlan.setLogicalLink(bfLogicalPlan)

            val executedPlan = QueryExecution.prepareExecutedPlan(
              adaptivePlan.context.session, bfPhysicalPlan, adaptivePlan.context)
            val scalarSubquery = ScalarSubqueryExec(SubqueryExec.createForScalarSubquery(
              s"scalar-subquery#${exprId.id}", executedPlan), exprId)
            DynamicPruningExpression(BloomFilterMightContain(scalarSubquery, new XxHash64(value)))
          } else {
            DynamicPruningExpression(Literal.TrueLiteral)
          }
        }
    }
  }
}
