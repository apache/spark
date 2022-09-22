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

import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Literal}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ScalarSubquery, SparkPlan, SubqueryExec}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

/**
 * A rule to add re-usuable Exchange in bloom creation plan.
 * This is only used in AQE execution flow.
 */
object PlanAdaptiveRuntimeBloomFilters extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {

  private def findMatchingPlanInRootPlan(
      rootPlan: SparkPlan,
      plan: SparkPlan): Option[SparkPlan] = {
    find(rootPlan) {
      case ShuffleExchangeExec(_, child, _) => child.sameResult(plan)
      case _ => false
    }
  }

  private def getTargetExchangePartitioning(
      rootPlan: SparkPlan,
      buildPlan: SparkPlan): Partitioning = {
    val bloomChildPlan = getBloomChildPlan(buildPlan)
    val prunedExchange = findMatchingPlanInRootPlan(rootPlan, bloomChildPlan).get
    prunedExchange.outputPartitioning
  }

  private def getBloomChildPlan(plan: SparkPlan): SparkPlan = {
    // Returns plan just below local ObjectHashAggregate node of bloom creation plan
    if (!plan.isInstanceOf[ObjectHashAggregateExec]) return plan

    plan.asInstanceOf[ObjectHashAggregateExec].child match {
      case oHA: ObjectHashAggregateExec =>
        oHA.child
      case sEE: ShuffleExchangeExec =>
        sEE.child.asInstanceOf[ObjectHashAggregateExec].child
      case e: SparkPlan => e
    }
  }

  def isBloomExchangeRemoved(currentPlan: SparkPlan, newPlan: SparkPlan): Boolean = {
    (currentPlan.asInstanceOf[ObjectHashAggregateExec].child match {
      case ShuffleExchangeExec(
            _,
            ObjectHashAggregateExec(_, _, _, _, _, _, _, _, ShuffleExchangeExec(_, _, _)),
            _) =>
        true
      case _ => false
    }) && (newPlan.asInstanceOf[ObjectHashAggregateExec].child match {
      case ShuffleExchangeExec(_, ObjectHashAggregateExec(_, _, _, _, _, _, _, _, child), _)
          if !child.isInstanceOf[ShuffleExchangeExec] =>
        true
      case _ => false
    })
  }

  def addNewExchangeNodeForBloom(plan: SparkPlan, distribution: Partitioning): SparkPlan = {
    val localAgg = plan
      .asInstanceOf[ObjectHashAggregateExec]
      .child
      .asInstanceOf[ShuffleExchangeExec]
      .child
      .asInstanceOf[ObjectHashAggregateExec]
    plan transformUp {
      case targetAgg: ObjectHashAggregateExec if targetAgg.eq(localAgg) =>
        val exchangeExec = ShuffleExchangeExec(distribution, targetAgg.child)
        exchangeExec.setLogicalLink(targetAgg.child.logicalLink.get)
        targetAgg.copy(child = exchangeExec)
    }
  }

  private def planBloom(
      rootPlan: SparkPlan,
      scalarSubquery: ScalarSubquery): Option[ScalarSubquery] = {
    // ScalarSubquery
    //  SubqueryExec
    //    ASPE
    //      OHA
    //        Ex
    //          OHA
    val buildSparkPlan =
      scalarSubquery.plan.child.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
    val bloomChildPlan = getBloomChildPlan(buildSparkPlan)
    val canReuseExchange = conf.exchangeReuseEnabled &&
      findMatchingPlanInRootPlan(rootPlan, bloomChildPlan).isDefined

    if (canReuseExchange) {
      val executedPlan = buildSparkPlan
      val distribution = getTargetExchangePartitioning(rootPlan, buildSparkPlan)

      val newPlanWithExchange = addNewExchangeNodeForBloom(executedPlan, distribution)

      val newAdaptivePlan = scalarSubquery.plan.child
        .asInstanceOf[AdaptiveSparkPlanExec]
        .copy(inputPlan = newPlanWithExchange)
      Some(
        ScalarSubquery(
          SubqueryExec.createForScalarSubquery(
            s"scalar-subquery#${scalarSubquery.exprId.id}",
            newAdaptivePlan),
          scalarSubquery.exprId))
    } else None
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.cboEnabled || !conf.runtimeFilterBloomFilterEnabled) {
      return plan
    }

    val newPlan = plan.transformAllExpressions {
      case original @ BloomFilterMightContain(scalarSubquery: ScalarSubquery, _, fromBroadcast) =>
        planBloom(plan, scalarSubquery) match {
          case Some(newSubquery) => original.copy(bloomFilterExpression = newSubquery)
          case None if fromBroadcast => original
          case None => Literal.TrueLiteral
        }
    }
    newPlan
  }
}
