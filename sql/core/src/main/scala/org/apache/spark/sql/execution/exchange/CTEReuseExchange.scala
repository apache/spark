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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A physical leaf node representing a reference to a shared CTE subplan. The [[subplan]]
 * (a [[ShuffleExchangeExec]] over the CTE body, produced from the Repartition in
 * [[CTEReuseRelation.sharedSubplan]]) is stored as metadata, not as a tree child, so
 * AQE's bottom-up stage creation does not traverse into it and redundantly plan its
 * substages before CTEReuseExchange is resolved.
 *
 * Consumed before execution:
 *  - AQE on: `createNonResultQueryStages` replaces this with [[CTEReuseQueryStageExec]],
 *    building a shared inner [[AdaptiveSparkPlanExec]] keyed by `cteId`.
 *  - AQE off: `UnwrapCTEReuseExchange` replaces each ref with its `LOCAL_SHUFFLE_FOR_CTE`
 *    shuffle (tagged with `cteId`); the final `ReuseExchangeAndSubquery` then reuses the
 *    canonically-equal copies via [[ReusedExchangeExec]], and `VerifyCTEReuse` confirms it held.
 *
 * @param cteId    Unique identifier for the CTE definition to be reused.
 * @param subplan  The [[ShuffleExchangeLike]] over the shared subplan. Its logical link
 *                 points to the Repartition in [[CTEReuseRelation.sharedSubplan]] so the
 *                 inner AQE can re-optimize correctly.
 * @param innerAQE The shared inner [[AdaptiveSparkPlanExec]] for this `cteId`, attached by
 *                 `PlanCTEReuse` (AQE on only) from `AdaptiveExecutionContext.cteAQERegistry`.
 *                 It is auxiliary metadata carried so the Photon compiler can read the CTE
 *                 subplan's photonization estimate (`initialPlanForExplain`) off this node; the
 *                 registry, shared across the main query and its subqueries, remains the source
 *                 of truth. `None` before `PlanCTEReuse` runs and on the AQE-off path.
 */
case class CTEReuseExchange(
    cteId: Long,
    subplan: ShuffleExchangeLike,
    innerAQE: Option[AdaptiveSparkPlanExec] = None
) extends LeafExecNode {

  override def output: Seq[Attribute] = subplan.output

  // Canonical identity is the CTE reference (`cteId` + `subplan`). Drop the attached `innerAQE`:
  // it is auxiliary metadata, and (being an [[AdaptiveSparkPlanExec]] with mutable replanning
  // state) must never feed canonical equality.
  override protected def doCanonicalize(): SparkPlan =
    copy(subplan = subplan.canonicalized.asInstanceOf[ShuffleExchangeLike], innerAQE = None)

  final override val nodePatterns: Seq[TreePattern] = Seq(CTE_REUSE_EXCHANGE)

  override def outputPartitioning: Partitioning = subplan.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = subplan.outputOrdering

  // Reaching any execute path is a planner bug -- the node must always be consumed.
  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException("CTEReuseExchange must be consumed before execution.")
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    throw new IllegalStateException("CTEReuseExchange must be consumed before execution.")

  override def simpleString(maxFields: Int): String = {
    s"CTEReuseExchange cteId=$cteId, partitioning=${subplan.outputPartitioning}"
  }

  // Override to include the subplan in EXPLAIN output and plan traversal methods
  // that walk `innerChildren` (e.g., `treeString`).
  override def innerChildren: Seq[SparkPlan] = Seq(subplan)
}
