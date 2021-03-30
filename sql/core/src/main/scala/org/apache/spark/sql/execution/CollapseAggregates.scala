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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
// import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

/**
 * Collapse Physical aggregate exec nodes together if there is no exchange between them and they
 * correspond to Partial and Final Aggregation for same
 * [[org.apache.spark.sql.catalyst.plans.logical.Aggregate]] logical node.
 */
object CollapseAggregates extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    collapseAggregates(plan)
  }

  private def collapseAggregates(plan: SparkPlan): SparkPlan = {
    plan transform {
      case parent@HashAggregateExec(_, _, _, _, _, _, child: HashAggregateExec)
        if checkIfAggregatesCanBeCollapsed(parent, child) =>
        val completeAggregateExpressions = child.aggregateExpressions.map(_.copy(mode = Complete))
        HashAggregateExec(
          requiredChildDistributionExpressions = Some(child.groupingExpressions),
          groupingExpressions = child.groupingExpressions,
          aggregateExpressions = completeAggregateExpressions,
          aggregateAttributes = completeAggregateExpressions.map(_.resultAttribute),
          initialInputBufferOffset = 0,
          resultExpressions = parent.resultExpressions,
          child = child.child)

      case parent@SortAggregateExec(_, _, _, _, _, _, child: SortAggregateExec)
        if checkIfAggregatesCanBeCollapsed(parent, child) =>
        val completeAggregateExpressions = child.aggregateExpressions.map(_.copy(mode = Complete))
        SortAggregateExec(
          requiredChildDistributionExpressions = Some(child.groupingExpressions),
          groupingExpressions = child.groupingExpressions,
          aggregateExpressions = completeAggregateExpressions,
          aggregateAttributes = completeAggregateExpressions.map(_.resultAttribute),
          initialInputBufferOffset = 0,
          resultExpressions = parent.resultExpressions,
          child = child.child)

      case parent@ObjectHashAggregateExec(_, _, _, _, _, _, child: ObjectHashAggregateExec)
        if checkIfAggregatesCanBeCollapsed(parent, child) =>
        val completeAggregateExpressions = child.aggregateExpressions.map(_.copy(mode = Complete))
        ObjectHashAggregateExec(
          requiredChildDistributionExpressions = Some(child.groupingExpressions),
          groupingExpressions = child.groupingExpressions,
          aggregateExpressions = completeAggregateExpressions,
          aggregateAttributes = completeAggregateExpressions.map(_.resultAttribute),
          initialInputBufferOffset = 0,
          resultExpressions = parent.resultExpressions,
          child = child.child)
      case parent@HashAggregateExec(_, _, _, _, _, _, child) =>
        // if checkIfAggregatesCanBeCollapsed(parent, child) =>
        child match {
          case ShuffleExchangeExec(_, c, _) =>
            c match {
              case agg@HashAggregateExec(_, _, _, _, _, _, c2) =>
                c2 match {
                  case r: SparkPlan =>
                    val completeAggregateExpressions =
                      agg.aggregateExpressions.map(_.copy(mode = Complete))
                    HashAggregateExec(
                      requiredChildDistributionExpressions = Some(agg.groupingExpressions),
                      groupingExpressions = agg.groupingExpressions,
                      aggregateExpressions = completeAggregateExpressions,
                      aggregateAttributes = completeAggregateExpressions.map(_.resultAttribute),
                      initialInputBufferOffset = 0,
                      resultExpressions = parent.resultExpressions,
                      child = agg.child)
                }
            }
        }
    }
  }

  private def checkIfAggregatesCanBeCollapsed(
      parent: BaseAggregateExec,
      child: BaseAggregateExec): Boolean = {
    val parentHasFinalMode = parent.aggregateExpressions.forall(_.mode == Final)
    if (!parentHasFinalMode) {
      return false
    }
    val childHasPartialMode = child.aggregateExpressions.forall(_.mode == Partial)
    if (!childHasPartialMode) {
      return false
    }
    val parentChildAggExpressionsSame = parent.aggregateExpressions.map(
      _.copy(mode = Partial)) == child.aggregateExpressions
    if (!parentChildAggExpressionsSame) {
      return false
    }
    true
  }
}