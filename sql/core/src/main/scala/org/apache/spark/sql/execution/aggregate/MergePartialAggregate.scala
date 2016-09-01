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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

object MergePartialAggregate extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = plan transform {
    // Normal partial aggregate pair
    case outer @ HashAggregateExec(_, _, _, _, _, _, inner: HashAggregateExec)
      if outer.aggregateExpressions.forall(_.mode == Final) &&
        inner.aggregateExpressions.forall(_.mode == Partial) =>
      inner.copy(
        aggregateExpressions = inner.aggregateExpressions.map(_.copy(mode = Complete)),
        aggregateAttributes = inner.aggregateExpressions.map(_.resultAttribute),
        resultExpressions = outer.resultExpressions)

    // First partial aggregate pair for aggregation with distinct
    case outer @ HashAggregateExec(_, _, _, _, _, _, inner: HashAggregateExec)
      if outer.aggregateExpressions.forall(_.mode == PartialMerge) &&
        inner.aggregateExpressions.forall(_.mode == Partial) =>
      inner

    // Second partial aggregate pair for aggregation with distinct.
    // This is actually a no-op. For aggregation with distinct, the output of first partial
    // aggregate is partitioned by grouping expressions and distinct attributes, and the second
    // partial aggregate requires input to be partitioned by grouping attributes, which is not
    // satisfied. `EnsureRequirements` will always insert exchange between these 2 aggregate exec
    // and we will never hit this branch.
    case outer @ HashAggregateExec(_, _, _, _, _, _, inner: HashAggregateExec)
      if outer.aggregateExpressions.forall(_.mode == Final) &&
        inner.aggregateExpressions.forall(_.mode == PartialMerge) =>
      outer.copy(child = inner.child)

    // Add similar logic for sort aggregate
  }
}
