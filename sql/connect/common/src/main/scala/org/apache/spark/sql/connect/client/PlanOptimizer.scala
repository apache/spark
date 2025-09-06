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
package org.apache.spark.sql.connect.client

import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable.SeqMap
import scala.collection.mutable

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.WithRelations.ResolutionMethod

/**
 * Optimizer for Spark Connect plans. This optimizer moves all duplicate subtrees from a query
 * tree (Relation) into a top level WithRelations node, the duplicates in the plan are replaced by
 * references. This has a couple of advantages: it reduces the number of nodes in the plan, it
 * reduces the plan size, it avoids redundant work on the server side (both during planning, and -
 * if supported - analysis).
 *
 * This optimization assumes that nodes with the same plan_id are structurally equivalent.
 *
 * The optimization will retain all plan_ids in the input plan. This is needed because plan_ids
 * can be referenced by UnresolvedAttribute, UnresolvedStar, UnresolvedRegex, and
 * SubqueryExpression expressions. If the plan can be optimized, the new plan will contain an
 * additional plan_id: the plan_id of the top-level WithRelations node.
 *
 * The current optimization uses a 2-pass approach. The first step identifies duplicate subtrees.
 * This has a runtime and space complexity of O(num_unique_relations). The second step rewrites
 * the plan. This has a runtime and space complexity of O(num_unique_relations).
 *
 * In theory this can be implemented as a single pass algorithm by replace duplicates with a
 * reference once we identify them. This has two downsides: it requires that the client and the
 * server have exactly the same traversal order, and it makes the plans much harder to read.
 *
 * @param nextPlanId
 *   generator for new plan_ids.
 */
class PlanOptimizer(nextPlanId: () => Long) {
  def this(planIdGenerator: AtomicLong) =
    this(() => planIdGenerator.incrementAndGet())

  /**
   * Optimize the given plan by deduplicating subtrees.
   *
   * @param plan
   *   The plan to optimize.
   * @return
   *   The optimized plan with deduplicated subtrees. If the plan cannot be optimized, this
   *   returns the original plan.
   */
  def optimize(plan: proto.Plan): proto.Plan =
    PlanOptimizer.optimize(plan, nextPlanId)

  /**
   * Optimize the given relation by deduplicating subtrees.
   *
   * @param relation
   *   The relation to optimize.
   * @return
   *   The optimized relation with deduplicated subtrees. If the relation cannot be optimized,
   *   this returns the original relation.
   */
  def optimize(relation: proto.Relation): proto.Relation =
    PlanOptimizer.optimize(relation, nextPlanId)
}

private[connect] object PlanOptimizer {
  import RelationTreeUtils._

  def optimize(plan: proto.Plan, nextPlanId: () => Long): proto.Plan = {
    if (plan.hasRoot) {
      val relation = plan.getRoot
      val optimizedRelation = optimize(relation, nextPlanId)
      if (optimizedRelation ne relation) {
        plan.toBuilder.setRoot(optimizedRelation).build()
      } else {
        plan
      }
    } else {
      plan
    }
  }

  def optimize(relation: proto.Relation, nextPlanId: () => Long): proto.Relation = {
    val relations = analyze(relation)
    if (relations.nonEmpty) {
      rewriteRelation(relation, relations, nextPlanId)
    } else {
      relation
    }
  }

  /**
   * Find all repeated (duplicate) query fragments in a query tree.
   *
   * @param root
   *   node of the query tree
   * @return
   *   a map that contains all repeated query fragments, keyed by their plan id.
   */
  def analyze(root: proto.Relation): SeqMap[Long, proto.Relation] = {
    // We can reduce memory consumption by using a bitset that tracks the planIds of nodes with a
    // single occurrence. We only need to start tracking detailed information once there are
    // multiple occurrences. For this we need a bitset that can deal with sparse planIds; there are
    // libraries for this (e.g. RoaringBitMap), however that requires us to add a library to the
    // Spark Connect client classpath which is something we need to trade off against overall size
    // of that classpath.
    val relationsMap = mutable.LinkedHashMap.empty[Long, RelationHolder]
    visit(root) {
      case relation @ PlanId(id) =>
        // Increase the stats for the plan id. If we have already seen the plan we will not
        // visit its children, because we have already seen them before.
        val holder = relationsMap.getOrElseUpdate(id, new RelationHolder(relation))
        holder.increaseNumOccurrences() == 1
      case _ =>
        // Always visit the subtree if there is no plan id. Its subtree might contain nodes we
        // have not visited before.
        true
    }

    // Retain all relations that are duplicated.
    relationsMap.to(SeqMap).collect {
      case (id, holder) if holder.occurrences > 1 =>
        id -> holder.relation
    }
  }

  /**
   * Rewrite the query tree using the map of reference relations. This transform moves all
   * reference relations to a top-level WithRelations node, and replaces all instances of these
   * relations with a reference.
   *
   * @param root
   *   relation to rewrite.
   * @param referenceMap
   *   a map of relations that will be moved to the top-level withRelations node.
   * @param nextPlanId
   *   function to generate the plan_id of the new root node.
   * @return
   *   the rewritten plan.
   */
  private def rewriteRelation(
      root: proto.Relation,
      referenceMap: SeqMap[Long, proto.Relation],
      nextPlanId: () => Long): proto.Relation = {
    val builder = proto.Relation.newBuilder()
    builder.getCommonBuilder.setPlanId(nextPlanId())
    val withRelationsBuilder = builder.getWithRelationsBuilder
      .setResolutionMethod(ResolutionMethod.BY_REFERENCE_ID)
    val referencePlanIds = referenceMap.keySet
    referenceMap.foreach { case (id, reference) =>
      withRelationsBuilder.addReferences(
        rewriteSingleRelation(reference, referencePlanIds.filterNot(_ == id)))
    }
    withRelationsBuilder.setRoot(rewriteSingleRelation(root, referencePlanIds))
    builder.build()
  }

  private def rewriteSingleRelation(
      relation: proto.Relation,
      referencePlanIds: Set[Long]): proto.Relation = transform(relation) {
    case PlanId(id) if referencePlanIds(id) =>
      createReference(id)
    case relation if relation.hasWithRelations =>
      // Rewrite the WithRelations node. If a reference is shared we either remove it (the reference
      // will be added to the top-level WithRelations node), or we replace it with a reference when
      // the WithRelation node is a CTE (ResolutionMethod.BY_NAME). The latter is needed because
      // name based resolution needs to account for nested name reuse (a.k.a. shadowing).
      val withRelations = relation.getWithRelations
      val resolveByName = withRelations.getResolutionMethod == ResolutionMethod.BY_NAME
      val builder = relation.toBuilder
      val withRelationsBuilder = builder.getWithRelationsBuilder.clearReferences()
      withRelations.getReferencesList.forEach {
        case reference @ PlanId(id) if referencePlanIds(id) =>
          if (resolveByName && reference.hasSubqueryAlias) {
            withRelationsBuilder.addReferences(createReference(id))
          }
        case reference =>
          withRelationsBuilder.addReferences(reference)
      }
      builder.build()
  }

  private def createReference(planId: Long): proto.Relation = {
    // We don't set a plan id here because this is a reference to an existing plan.
    proto.Relation.newBuilder().setReferencedPlanId(planId).build()
  }

  object PlanId {
    def apply(relation: proto.Relation): Long = unapply(relation).get
    def get(relation: proto.Relation): Option[Long] = unapply(relation)
    def unapply(relation: proto.Relation): Option[Long] = {
      val common = relation.getCommon
      if (common.hasPlanId) {
        Some(common.getPlanId)
      } else {
        None
      }
    }
  }

  private class RelationHolder(val relation: proto.Relation) {
    private var numOccurrences = 0
    def occurrences: Int = numOccurrences
    def increaseNumOccurrences(): Int = {
      numOccurrences += 1
      numOccurrences
    }
  }
}
