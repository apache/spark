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

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BuildBloomFilter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Repartition}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This optimization rule find the build bloom filter expression that added by dynamic bloom filter
 * pruning and set expectedNumItems from LogicalQueryStage which is more accurate.
 */
object OptimizeBloomFilterJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.dynamicBloomFilterJoinPruningEnabled) {
      return plan
    }

    plan.transformDown {
      case a @ Aggregate(Nil, Seq(alias @ Alias(ae @ AggregateExpression(
        b: BuildBloomFilter, _, _, _, _), _)), Repartition(_, false, l: LogicalQueryStage))
          if b.changeExpectedNumItemsByAQE && l.stats.rowCount.nonEmpty =>
      val newBuildBloomFilter = b.copy(
        changeExpectedNumItemsByAQE = false,
        expectedNumItems = l.stats.rowCount.get.toLong)
      val aggregateExpression = ae.copy(aggregateFunction = newBuildBloomFilter)
      val aggs = Seq(alias.copy(child = aggregateExpression)(
        alias.exprId, alias.qualifier, alias.explicitMetadata, alias.nonInheritableMetadataKeys))
      a.copy(aggregateExpressions = aggs)
    }
  }
}
