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
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A physical leaf node representing a reference to a shared CTE subplan. The [[subplan]]
 * (a [[ShuffleExchangeExec]] over the CTE body, produced from the plan-reuse
 * RepartitionByExpression in [[CTEReuseRelation.sharedSubplan]]) is stored as metadata, not as a
 * tree child, so AQE's bottom-up stage creation does not traverse into it and redundantly plan its
 * substages before CTEReuseExchange is resolved.
 *
 * Consumed before execution:
 *  - AQE on: `createNonResultQueryStages` replaces this with [[CTEReuseQueryStageExec]] over the
 *    shared inner `AdaptiveSparkPlanExec` that `PlanCTEReuse` built and registered for `cteId`
 *    in `AdaptiveExecutionContext.cteAQERegistry`.
 *  - AQE off: `UnwrapCTEReuseExchange` replaces each ref with its shuffle, whose
 *    `LOCAL_SHUFFLE_FOR_CTE(cteId)` origin carries the `cteId`; the final
 *    `ReuseExchangeAndSubquery` then reuses the canonically-equal copies via
 *    [[ReusedExchangeExec]], and `VerifyCTEReuse` confirms it held.
 *
 * @param cteId    The plan-reuse id of the shared body, which every reference of one sealed
 *                 definition carries. Sealed definitions whose deterministic bodies are
 *                 canonically equal share it, and so does a sealed definition whose body only
 *                 selects another sealed definition. It is not the CTERelationDef id that plans
 *                 print.
 * @param subplan  The [[ShuffleExchangeLike]] over the shared subplan. Its logical link
 *                 points to a private clone of [[CTEReuseRelation.sharedSubplan]] that the
 *                 planning strategy made, so the inner AQE re-optimizes its own copy of the
 *                 body rather than the object that other plannings of the reference read.
 */
case class CTEReuseExchange(
    cteId: Long,
    subplan: ShuffleExchangeLike
) extends LeafExecNode {

  override def output: Seq[Attribute] = subplan.output

  // Canonical identity is the shared shuffle itself, whose `LOCAL_SHUFFLE_FOR_CTE(cteId)` origin
  // carries the `cteId`: the AQE-off unwrap leaves that shuffle in the plan, and the AQE query
  // stage that replaces this node keeps this canonical form, so a freshly planned reference to the
  // same CTE, such as a dynamic-pruning build side, has the same result as the reference in the
  // plan.
  // With AQE on, `PlanCTEReuse` first replaces a `subplan` that holds subqueries with its
  // preprocessed form, so the canonical form a query stage keeps holds no unplanned subquery.
  override protected def doCanonicalize(): SparkPlan = subplan.canonicalized

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
