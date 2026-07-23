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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, LeafExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.trees.TreePattern.{NO_GROUPING_AGGREGATE_REFERENCE, SCALAR_SUBQUERY_REFERENCE, TreePattern}
import org.apache.spark.sql.types.DataType

// The temporal reference placeholders below are produced by the `MergeSubplans` rule (now in
// sql/core) but must remain in catalyst: `ScalarSubqueryReference` is referenced by the catalyst
// expression `BloomFilterMightContain`, and catalyst cannot depend on sql/core.

/**
 * Temporal reference to a subquery which is added to a `PlanMerger`.
 *
 * @param level The level of the replaced subquery. It defines the `PlanMerger` instance into which
 *              the subquery is merged.
 * @param mergedPlanIndex The index of the merged plan in the `PlanMerger`.
 * @param outputIndex The index of the output attribute of the merged plan.
 * @param dataType The dataType of original scalar subquery.
 * @param exprId The expression id of the original scalar subquery.
 */
case class ScalarSubqueryReference(
    level: Int,
    mergedPlanIndex: Int,
    outputIndex: Int,
    override val dataType: DataType,
    exprId: ExprId) extends LeafExpression with Unevaluable {
  override def nullable: Boolean = true

  final override val nodePatterns: Seq[TreePattern] = Seq(SCALAR_SUBQUERY_REFERENCE)
}

/**
 * Temporal reference to a non-grouping aggregate which is added to a `PlanMerger`.
 *
 * @param level The level of the replaced aggregate. It defines the `PlanMerger` instance into which
 *              the aggregate is merged.
 * @param mergedPlanIndex The index of the merged plan in the `PlanMerger`.
 * @param outputIndices The indices of the output attributes of the merged plan.
 * @param output The output of original aggregate.
 */
case class NonGroupingAggregateReference(
    level: Int,
    mergedPlanIndex: Int,
    outputIndices: Seq[Int],
    override val output: Seq[Attribute]) extends LeafNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(NO_GROUPING_AGGREGATE_REFERENCE)
}
