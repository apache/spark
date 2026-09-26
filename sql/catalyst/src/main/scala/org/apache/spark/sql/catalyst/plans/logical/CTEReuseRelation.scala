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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreePattern._

/**
 * A logical leaf node that represents a reference to a shared subplan that should be
 * physically reused (materialized once, results shared across all consumers).
 *
 * All nodes with the same [[cteId]] are guaranteed to have [[sharedSubplan]] objects
 * having the same canonicalized form. Theses shared subplans will be backed by the
 * same physical exchange stage in AQE.
 *
 * [[sharedSubplan]] is NOT a child of this node. This design is intended to allow
 * the AQE reoptimization for the outer plan to be independent of the reoptimization
 * for the shared subplan. The reoptimization for the outer plan treats the node as a
 * leaf node and does not descend into it while the reoptimization for the shared subplan
 * is handled separately by the inner AQE thread.
 *
 * This node is generated from [[Repartition]] nodes which are marked for plan reuse (CTE references
 * are first turned into such repartitions by [[ReplaceCTERefWithRepartition]]). This generation
 * happens in the [[ReplaceRepartitionWithCTEReuse]] rule.
 *
 * @param cteId Unique identifier for the subplan or CTE to be reused.
 * @param partitioning Pre-determined partitioning for the shared exchange. Either a
 *                     HashPartitioning derived from the common distribution requirements of
 *                     all consumers, or LocalPartition as a safe fallback.
 * @param sharedSubplan The logical plan of the CTE definition. NOT a child node.
 */
case class CTEReuseRelation(
    cteId: Long,
    partitioning: Partitioning,
    sharedSubplan: LogicalPlan
) extends LeafNode {

  override def output: Seq[Attribute] = sharedSubplan.output

  final override val nodePatterns: Seq[TreePattern] = Seq(CTE_REUSE)

  override def computeStats(): Statistics = sharedSubplan.stats

  // Canonicalization normalizes attribute exprIds against `allAttributes`, which for a LeafNode
  // defaults to the (empty) child outputs -- leaving raw exprIds in `partitioning`'s keys. Expose
  // `sharedSubplan.output` (what those keys reference) so partitionings equal modulo exprIds match,
  // as `LogicalPlanIntegrity.validateCTEReuseRelations` requires for nodes sharing a `cteId`.
  override def allAttributes: AttributeSeq = sharedSubplan.output

  // `sharedSubplan` is metadata, not a child, so neither `mapExpressions` nor `mapChildren` reaches
  // it. Canonicalize it explicitly.
  override def doCanonicalize(): LogicalPlan =
    super.doCanonicalize().asInstanceOf[CTEReuseRelation]
      .copy(sharedSubplan = sharedSubplan.canonicalized)

  override def simpleString(maxFields: Int): String = {
    s"CTEReuseRelation cteId=$cteId, partitioning=$partitioning"
  }

  // `sharedSubplan` is metadata, not a child, so the tree-string traversal (which walks `children`)
  // never reaches it and EXPLAIN would render this node as an opaque leaf. Surface it as an inner
  // child so the CTE body shows up nested under the node, the same way
  // `CTEReuseExchange`/`InMemoryTableScanExec` expose their metadata subplans. This affects only
  // string rendering -- `innerChildren` is not part of the tree structure that
  // analysis/optimization traverses.
  override def innerChildren: Seq[QueryPlan[_]] = Seq(sharedSubplan) ++ super.innerChildren
}
