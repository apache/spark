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

package org.apache.spark.sql.execution.planmerging

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeMap, AttributeReference, AttributeSet, Expression, ExpressionSet, If, Literal, NamedExpression, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.read.SupportsScanMerging
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation, V2ScanRelationPushDown}
import org.apache.spark.sql.internal.SQLConf

/**
 * Result of attempting to merge a plan via [[PlanMerger.merge]].
 *
 * @param mergedPlan The resulting plan, either:
 *                   - An existing cached plan (if identical match found)
 *                   - A newly merged plan combining the input with a cached plan
 *                   - The original input plan (if no merge was possible)
 * @param mergedPlanIndex The index of this plan in the PlanMerger's cache.
 * @param outputMap Maps attributes of the input plan to their positional index in
 *                  `mergedPlan.plan.output`. The index remains stable across subsequent
 *                  [[PlanMerger.merge]] calls because outputs are only ever appended.
 */
case class MergeResult(
    mergedPlan: MergedPlan,
    mergedPlanIndex: Int,
    outputMap: AttributeMap[Int])

/**
 * Represents a plan in the PlanMerger's cache.
 *
 * @param plan The logical plan, which may have been merged from multiple original plans.
 * @param merged Whether this plan is the result of merging two or more plans (true), or
 *               is an original unmerged plan (false). Merged plans typically require special
 *               handling such as wrapping in CTEs.
 */
case class MergedPlan(plan: LogicalPlan, merged: Boolean)

object PlanMerger {
  // Marker tag placed on Filter nodes that were produced by filter propagation. Its presence
  // signals that the Filter's condition is already an OR of propagated filter attributes and
  // its child Project already contains the corresponding aliases, so a subsequent merge only
  // needs to add one new alias for the incoming plan rather than wrapping both sides again.
  val MERGED_FILTER_TAG: TreeNodeTag[Unit] = TreeNodeTag("mergedFilter")

  // Global counter for generating unique names for propagated filter attributes across all
  // PlanMerger instances.
  private[planmerging] val curId = new java.util.concurrent.atomic.AtomicLong()
  private[planmerging] def newId: Long = curId.getAndIncrement()
}

/**
 * A stateful utility for merging identical or similar logical plans to enable query plan reuse.
 *
 * `PlanMerger` maintains a cache of previously seen plans and attempts to either:
 * 1. Reuse an identical plan already in the cache
 * 2. Merge a new plan with a cached plan by combining their outputs
 *
 * The merging process preserves semantic equivalence while combining outputs from multiple
 * plans into a single plan. This is primarily used by [[MergeSubplans]] to deduplicate subplan
 * execution.
 *
 * Supported plan types for merging:
 * - [[Project]]: Merges project lists
 * - [[Aggregate]]: Merges aggregate expressions with identical grouping
 * - [[Filter]]: Requires identical filter conditions
 * - [[Join]]: Requires identical join type, hints, and conditions
 *
 * When `filterPropagationEnabled` is true, non-grouping [[Aggregate]]s over the same base plan
 * with different [[Filter]] conditions can also be merged. The filter conditions are exposed as
 * boolean [[Project]] attributes and consumed at the [[Aggregate]] as FILTER clauses.
 * When both sides carry a [[Filter]] (the symmetric case), merging broadens the scan to OR(f1, f2),
 * which may reduce IO pruning. This path is separately gated by
 * `symmetricFilterPropagationEnabled`.
 * When plans also differ in intermediate [[Project]] expressions, those are wrapped with
 * `If(filterAttr, expr, null)` to avoid computing the expression for rows that do not match that
 * side's filter condition.
 * Filter propagation also works through [[Join]] nodes: a filter on one child of the join produces
 * a boolean attribute that flows through the join output to the enclosing [[Aggregate]].
 * Propagation is only safe when the filter originates from the non-nullable side of the join, as
 * enforced by `filterSafeForJoin`. When the filter is on the nullable side, the merged base plan
 * restores rows that were filtered out of the nullable child, turning what were unmatched
 * NULL-padded rows in the original plan into matched rows with real column values. This changes the
 * result of expressions like `coalesce(col, default)` in the aggregate: an originally unmatched row
 * would have contributed `default` via `coalesce(NULL, default)`, but in the merged plan it is
 * matched, its real column value fails the filter, and `FILTER (WHERE false)` discards it entirely.
 * Propagation is also skipped when both the left and right children simultaneously produce filter
 * attributes, as combining them would require an additional AND alias above the join (not yet
 * supported).
 *
 * {{{
 *   // Input plans
 *   Aggregate [sum(a) AS sum_a]         Aggregate [max(d) AS max_d]
 *   +- Filter (a < 1)                   +- Project [udf(a) AS d]
 *      +- Scan t                           +- Filter (a > 1)
 *                                             +- Scan t
 *
 *   // Merged plan
 *   Aggregate [sum(a) FILTER f0 AS sum_a, max(d0) FILTER f1 AS max_d]
 *   +- Project [a, If(f1, udf(a), null) AS d0, f0, f1]
 *      +- Filter (f0 OR f1)  [MERGED_FILTER_TAG]
 *         +- Project [a, (a < 1) AS f0, (a > 1) AS f1]
 *            +- Scan t
 * }}}
 *
 * @example
 * {{{
 *   val merger = PlanMerger()
 *   val result1 = merger.merge(plan1)  // Adds plan1 to cache
 *   val result2 = merger.merge(plan2)  // Merges with plan1 if compatible
 *   // result2.mergedPlan.merged == true if plans were merged
 *   // result2.outputMap maps plan2's attributes to the merged plan's attributes
 * }}}
 */
class PlanMerger(
    filterPropagationEnabled: Boolean =
      SQLConf.get.getConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED),
    symmetricFilterPropagationEnabled: Boolean =
      SQLConf.get.getConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED),
    filterPropagationThroughJoinEnabled: Boolean =
      SQLConf.get.getConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED)) {
  val cache = mutable.ArrayBuffer.empty[MergedPlan]

  /**
   * Attempts to merge the given plan with cached plans, or adds it to the cache.
   *
   * The method tries the following in order:
   * 1. Check if an identical plan exists in cache (using canonicalized comparison)
   * 2. Try to merge with each cached plan using [[tryMergePlans]]
   * 3. If no merge is possible, add as a new cache entry
   *
   * @param plan The logical plan to merge or cache.
   * @param subqueryPlan If the logical plan is a subquery plan.
   * @return A [[MergeResult]] containing:
   *         - The merged/cached plan to use
   *         - Its index in the cache
   *         - An attribute mapping for rewriting expressions
   */
  def merge(plan: LogicalPlan, subqueryPlan: Boolean): MergeResult = {
    cache.zipWithIndex.collectFirst(Function.unlift {
      case (mp, i) =>
        checkIdenticalPlans(plan, mp.plan).map { _ =>
          // Identical subquery expression plans are not marked as `merged` as the
          // `ReusedSubqueryExec` rule can handle them without extracting the plans to CTEs.
          // But, when a non-subquery subplan is identical to a cached plan we need to mark the plan
          // `merged` and so extract it to a CTE later.
          val newMergedPlan = MergedPlan(mp.plan, mp.merged || !subqueryPlan)
          cache(i) = newMergedPlan
          val outputMap = AttributeMap(plan.output.zipWithIndex)
          MergeResult(newMergedPlan, i, outputMap)
        }.orElse {
          tryMergePlans(plan, mp.plan, false).collect {
            case TryMergeResult(mergedPlan, npMapping, None, None) =>
              val newMergedPlan = MergedPlan(mergedPlan, true)
              cache(i) = newMergedPlan
              val outputMap = AttributeMap(npMapping.iterator.map { case (origAttr, mergedAttr) =>
                origAttr -> mergedPlan.output.indexWhere(_.exprId == mergedAttr.exprId)
              }.toSeq)
              MergeResult(newMergedPlan, i, outputMap)
          }
        }
      case _ => None
    }).getOrElse {
      val newMergedPlan = MergedPlan(plan, false)
      cache += newMergedPlan
      val outputMap = AttributeMap(plan.output.zipWithIndex)
      MergeResult(newMergedPlan, cache.length - 1, outputMap)
    }
  }

  /**
   * Returns all plans currently in the cache as an immutable indexed sequence.
   *
   * @return An indexed sequence of [[MergedPlan]]s in cache order. The index of each plan
   *         corresponds to the `mergedPlanIndex` returned by [[merge]].
   */
  def mergedPlans(): IndexedSeq[MergedPlan] = cache.toIndexedSeq

  // If 2 plans are identical return the attribute mapping from the new to the cached version.
  private def checkIdenticalPlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan): Option[AttributeMap[Attribute]] = {
    if (newPlan.canonicalized == cachedPlan.canonicalized) {
      Some(AttributeMap(newPlan.output.zip(cachedPlan.output)))
    } else {
      None
    }
  }

  /**
   * Result of a successful [[tryMergePlans]] call.
   *
   * @param mergedPlan The combined logical plan.
   * @param newPlanMapping Mapping from attributes in the new plan to the corresponding
   *                         attributes in the merged plan. Used by parent nodes to remap
   *                         new-plan-side expressions.
   * @param newPlanFilter A boolean [[Attribute]] in the merged plan that encodes the filter
   *                      condition from the new plan's side, to be applied as an aggregate
   *                      `FILTER (WHERE ...)` clause when the propagation reaches an enclosing
   *                      [[Aggregate]] node. The boolean component is `true` if the attribute was
   *                      freshly aliased and must be appended to enclosing [[Project]] nodes, or
   *                      `false` if it was reused from an existing alias already present in the
   *                      merged plan. `None` when no differing filter was propagated.
   * @param cachedPlanFilter Like `newPlanFilter` but for the cached plan's side. Always a freshly
   *                         created alias when present, so no `isNew` flag is needed.
   */
  case class TryMergeResult(
      mergedPlan: LogicalPlan,
      newPlanMapping: AttributeMap[Attribute],
      newPlanFilter: Option[(Attribute, Boolean)] = None,
      cachedPlanFilter: Option[Attribute] = None)

  /**
   * Recursively attempts to merge two plans by traversing their tree structures.
   *
   * Two plans can be merged if:
   * - They are identical (canonicalized forms match), OR
   * - They have compatible root nodes with mergeable children
   *
   * Supported merge patterns:
   * - Project nodes: Combines project lists from both plans
   * - Aggregate nodes: Combines aggregate expressions if grouping is identical and both
   *   support the same aggregate implementation (hash/object-hash/sort-based)
   * - Filter nodes: Only if filter conditions are identical
   * - Join nodes: Requires identical join type, hints, and conditions; filter propagation is
   *   forwarded into the join's children so a filter difference on one child can still be merged
   *
   * @param newPlan The plan to merge into the cached plan.
   * @param cachedPlan The cached plan to merge with.
   * @return Some([[TryMergeResult]]) if merge succeeds, None if plans cannot be merged.
   */
  private def tryMergePlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan,
      filterPropagationSupported: Boolean): Option[TryMergeResult] = {
    checkIdenticalPlans(newPlan, cachedPlan).map(TryMergeResult(cachedPlan, _)).orElse(
      (newPlan, cachedPlan) match {
        case (np: Project, cp: Project) =>
          tryMergePlans(np.child, cp.child, filterPropagationSupported).map {
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter) =>
              val (mergedProjectList, newNPMapping) =
                mergeNamedExpressions(np.projectList, cp.projectList, npMapping, npFilter, cpFilter)
              TryMergeResult(Project(mergedProjectList, mergedChild), newNPMapping, npFilter,
                cpFilter)
          }
        case (np, cp: Project) =>
          tryMergePlans(np, cp.child, filterPropagationSupported).map {
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter) =>
              val (mergedProjectList, newNPMapping) =
                mergeNamedExpressions(np.output, cp.projectList, npMapping, npFilter, cpFilter)
              TryMergeResult(Project(mergedProjectList, mergedChild), newNPMapping, npFilter,
                cpFilter)
          }
        case (np: Project, cp) =>
          tryMergePlans(np.child, cp, filterPropagationSupported).map {
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter) =>
              val (mergedProjectList, newNPMapping) =
                mergeNamedExpressions(np.projectList, cp.output, npMapping, npFilter, cpFilter)
              TryMergeResult(Project(mergedProjectList, mergedChild), newNPMapping, npFilter,
                cpFilter)
          }

        case (np: Aggregate, cp: Aggregate) if supportedAggregateMerge(np, cp) =>
          // Filter propagation into the aggregate is only safe when there is no grouping.
          val childFilterPropagationSupported = filterPropagationEnabled &&
            np.groupingExpressions.isEmpty && cp.groupingExpressions.isEmpty
          tryMergePlans(np.child, cp.child, childFilterPropagationSupported).flatMap {
            case TryMergeResult(mergedChild, npMapping, None, None) =>
              val mappedNPGroupingExpression =
                np.groupingExpressions.map(mapAttributes(_, npMapping))
              // Order of grouping expression does matter as merging different grouping orders can
              // introduce "extra" shuffles/sorts that might not present in all of the original
              // subqueries.
              if (mappedNPGroupingExpression.map(_.canonicalized) ==
                  cp.groupingExpressions.map(_.canonicalized)) {
                val (mergedAggregateExpressions, newNPMapping) =
                  mergeNamedExpressions(np.aggregateExpressions, cp.aggregateExpressions, npMapping)
                val mergedPlan =
                  Aggregate(cp.groupingExpressions, mergedAggregateExpressions, mergedChild)
                Some(TryMergeResult(mergedPlan, newNPMapping))
              } else {
                None
              }
            case TryMergeResult(mergedChild, npMapping, npFilterOpt, cpFilterOpt) =>
              // childFilterPropagationSupported guarantees both aggregates have no grouping, so
              // the grouping-match check is skipped.
              assert(childFilterPropagationSupported)

              // Apply each propagated boolean attribute as a FILTER (WHERE ...) clause on the
              // corresponding side's aggregate expressions.
              // A None filter means the side's aggregate expressions already carry their individual
              // FILTER attributes from a previous merge round and should be left unchanged.
              // Filter propagation is consumed here and not passed further up.
              val filteredNPAggregateExpressions = npFilterOpt.fold(np.aggregateExpressions) {
                case (f, _) => applyFilterToAggregateExpressions(np.aggregateExpressions, f)
              }
              val filteredCPAggregateExpressions = cpFilterOpt.fold(cp.aggregateExpressions)(
                applyFilterToAggregateExpressions(cp.aggregateExpressions, _))
              val (mergedAggregateExpressions, newNPMapping) =
                mergeNamedExpressions(filteredNPAggregateExpressions,
                  filteredCPAggregateExpressions, npMapping)
              val mergedPlan = Aggregate(Seq.empty, mergedAggregateExpressions, mergedChild)
              Some(TryMergeResult(mergedPlan, newNPMapping))
          }

        case (np: Filter, cp: Filter) =>
          tryMergePlans(np.child, cp.child, filterPropagationSupported).flatMap {
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter) =>
              val mappedNPCondition = mapAttributes(np.condition, npMapping)
              // Comparing the canonicalized form is required to ignore different forms of the same
              // expression.
              if (mappedNPCondition.canonicalized == cp.condition.canonicalized) {
                // Identical conditions: the filter node adds no new discrimination between the two
                // sides, so keep it unchanged. But if it sits above a merged DSv2 scan, recover the
                // row-group pruning for its condition on that scan (Phase 2).
                val prunedChild = rePushMergedScan(mergedChild, Some(cp.condition))
                val mergedPlan = Filter(cp.condition, prunedChild)
                Some(TryMergeResult(mergedPlan, npMapping, npFilter, cpFilter))
              } else if (filterPropagationSupported && symmetricFilterPropagationEnabled) {
                if (cp.getTagValue(PlanMerger.MERGED_FILTER_TAG).isDefined) {
                  // cp Filter is already a merged filter from a previous round: its condition
                  // is OR(f0, f1, ...) and its child Project already contains aliases for those
                  // attributes. Only create a new alias for the np side, and extend the OR
                  // condition.
                  val newNPCondition = npFilter.fold(mappedNPCondition) {
                    case (f, _) => And(f, mappedNPCondition)
                  }
                  val childProject = mergedChild match {
                    case p: Project => p
                    case other => throw new IllegalStateException(
                      "Expected Project child under MERGED_FILTER_TAG filter, got " +
                        s"${other.getClass.getSimpleName}")
                  }
                  // If newNPCondition is already aliased in the child Project (e.g. a third
                  // subplan whose filter matches one from a previous merge round), reuse the
                  // existing attribute instead of creating a redundant alias.
                  val existingNPFilter = childProject.projectList.collectFirst {
                    case a: Alias if a.child.canonicalized == newNPCondition.canonicalized =>
                      a.toAttribute
                  }
                  val (newProjectList, newCondition, newNPFilterOut) = existingNPFilter match {
                    case Some(reusedFilter) =>
                      // np matches an existing side: no new alias, OR condition unchanged.
                      (childProject.projectList, cp.condition, (reusedFilter, false))
                    case None =>
                      val newNPFilterAlias =
                        Alias(newNPCondition, s"propagatedFilter_${PlanMerger.newId}")()
                      (childProject.projectList :+ newNPFilterAlias,
                        Or(cp.condition, newNPFilterAlias.toAttribute): Expression,
                        (newNPFilterAlias.toAttribute, true))
                  }
                  // Phase 2: the leaf re-merge rebuilt the scan with strict filters only, dropping
                  // the OR pruning established in earlier rounds, so re-establish it here from ALL
                  // propagated conditions, not just the new side's. Only the aliases the OR
                  // condition references are filter sides; other aliases in the Project are
                  // computed columns, not filters.
                  val conditions = newProjectList.collect {
                    case a: Alias if newCondition.references.contains(a.toAttribute) => a.child
                  }
                  val prunedChild =
                    rePushMergedScan(childProject.child, conditions.reduceOption(Or))
                  val newProject = childProject.copy(
                    projectList = newProjectList, child = prunedChild)
                  val newFilter = Filter(newCondition, newProject)
                  newFilter.copyTagsFrom(cp)
                  Some(TryMergeResult(newFilter, npMapping, Some(newNPFilterOut), None))
                } else {
                  // First-time filter propagation: alias both sides' conditions as boolean
                  // attributes in a new Project below the Filter, and set the Filter condition
                  // to OR(newNPFilter, newCPFilter).
                  // Note: the new Project always uses mergedChild as its child (rather than
                  // flattening into an existing Project below) because mergedChild.output may
                  // contain previously-propagated filter attributes that cp.condition references.
                  val newNPCondition =
                    npFilter.fold(mappedNPCondition) { case (f, _) => And(f, mappedNPCondition) }
                  val newCPCondition = cpFilter.fold(cp.condition)(And(_, cp.condition))
                  // Phase 2: the OR-widen moves both conditions into a boolean Project above the
                  // merged scan, so the scan itself would read the full table. Recover row-group
                  // pruning by re-pushing OR(np condition, cp condition) onto the merged DSv2 scan
                  // (the Filter above still enforces exactness). The pruning is derived from the
                  // scan-level conditions, not the propagated filter attributes in newNP/newCP.
                  val prunedChild = rePushMergedScan(
                    mergedChild, Some(Or(mappedNPCondition, cp.condition)))
                  val newNPFilterAlias =
                    Alias(newNPCondition, s"propagatedFilter_${PlanMerger.newId}")()
                  val newCPFilterAlias =
                    Alias(newCPCondition, s"propagatedFilter_${PlanMerger.newId}")()
                  val newNPFilter = newNPFilterAlias.toAttribute
                  val newCPFilter = newCPFilterAlias.toAttribute
                  val project = Project(
                    prunedChild.output.toList ++ Seq(newNPFilterAlias, newCPFilterAlias),
                    prunedChild)
                  val newFilter = Filter(Or(newNPFilter, newCPFilter), project)
                  newFilter.copyTagsFrom(cp)
                  newFilter.setTagValue(PlanMerger.MERGED_FILTER_TAG, ())
                  Some(TryMergeResult(newFilter, npMapping, Some((newNPFilter, true)),
                    Some(newCPFilter)))
                }
              } else {
                None
              }
          }
        case (np: Filter, cp) if filterPropagationSupported =>
          tryMergePlans(np.child, cp, filterPropagationSupported).collect {
            // If the cp side already propagated a filter from deeper recursion, the merge is
            // effectively symmetric (both sides have a filter condition). Abort unless
            // symmetricFilterPropagationEnabled.
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter)
                if cpFilter.isEmpty || symmetricFilterPropagationEnabled =>
              val mappedNPCondition = mapAttributes(np.condition, npMapping)
              val newNPCondition = npFilter.fold(mappedNPCondition) {
                case (f, _) => And(f, mappedNPCondition)
              }
              val newNPFilterAlias =
                Alias(newNPCondition, s"propagatedFilter_${PlanMerger.newId}")()
              val newNPFilter = newNPFilterAlias.toAttribute
              val project = Project(
                mergedChild.output.toList :+ newNPFilterAlias,
                mergedChild)
              TryMergeResult(project, npMapping, Some((newNPFilter, true)), cpFilter)
          }
        case (np, cp: Filter) if filterPropagationSupported =>
          tryMergePlans(np, cp.child, filterPropagationSupported).collect {
            // If the np side already propagated a filter from deeper recursion, the merge is
            // effectively symmetric (both sides have a filter condition). Abort unless
            // symmetricFilterPropagationEnabled.
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter)
                if npFilter.isEmpty || symmetricFilterPropagationEnabled =>
              if (cp.getTagValue(PlanMerger.MERGED_FILTER_TAG).isDefined) {
                // cp is a previously-merged Filter: its condition is `OR(pf_0, pf_1, ...)` and cp's
                // aggregate expressions already carry individual `FILTER (WHERE pf_i)` clauses that
                // restrict each aggregation to its originating side. Synthesising a new cpFilter
                // alias for cp.condition would just produce `FILTER AND(OR(pf_0, pf_1, ...), pf_i)`
                // upstream, which simplifies to `FILTER pf_i` -- wasted work and plan bloat.
                // Drop cp's Filter and let the recursion's result flow up with cpFilter = None so
                // cp's aggregates are left untouched.
                TryMergeResult(mergedChild, npMapping, npFilter, None)
              } else {
                val newCPCondition = cpFilter.fold(cp.condition)(And(_, cp.condition))
                val newCPFilterAlias =
                  Alias(newCPCondition, s"propagatedFilter_${PlanMerger.newId}")()
                val newCPFilter = newCPFilterAlias.toAttribute
                val project = Project(
                  mergedChild.output.toList :+ newCPFilterAlias,
                  mergedChild)
                TryMergeResult(project, npMapping, npFilter, Some(newCPFilter))
              }
          }

        case (np: Join, cp: Join) if np.joinType == cp.joinType && np.hint == cp.hint =>
          tryMergePlans(np.left, cp.left, filterPropagationSupported).flatMap {
            case TryMergeResult(mergedLeft, leftNPMapping, leftNPFilter, leftCPFilter) =>
              tryMergePlans(np.right, cp.right, filterPropagationSupported).flatMap {
                case TryMergeResult(mergedRight, rightNPMapping, rightNPFilter, rightCPFilter)
                    // If both children independently propagate filter attributes we would need to
                    // AND them into a new alias above the join, which is not yet supported.
                    if !(leftNPFilter.isDefined && rightNPFilter.isDefined) &&
                       !(leftCPFilter.isDefined && rightCPFilter.isDefined) &&
                       // Gate join-crossing filter propagation behind its own config flag.
                       // When no filter attributes are in play the merge is unconditionally safe.
                       (leftNPFilter.isEmpty && leftCPFilter.isEmpty &&
                           rightNPFilter.isEmpty && rightCPFilter.isEmpty ||
                           filterPropagationThroughJoinEnabled) &&
                       // A filter attribute is only safe to propagate through a join if it comes
                       // from the "preserved" (non-nullable) side. On the nullable side, unmatched
                       // rows are NULL-padded so f=NULL, causing FILTER (WHERE f) to incorrectly
                       // exclude rows that should contribute to the aggregate. Right-side
                       // attributes are also absent from semi/anti join output.
                       (leftNPFilter.isEmpty && leftCPFilter.isEmpty  ||
                           filterSafeForJoin(fromLeft = true, cp.joinType)) &&
                       (rightNPFilter.isEmpty && rightCPFilter.isEmpty ||
                           filterSafeForJoin(fromLeft = false, cp.joinType)) =>
                  val npMapping = leftNPMapping ++ rightNPMapping
                  val mappedNPCondition = np.condition.map(mapAttributes(_, npMapping))
                  // Comparing the canonicalized form is required to ignore different forms of the
                  // same expression and `AttributeReference.qualifier`s in `cp.condition`.
                  if (mappedNPCondition.map(_.canonicalized) == cp.condition.map(_.canonicalized)) {
                    val npFilter = leftNPFilter.orElse(rightNPFilter)
                    val cpFilter = leftCPFilter.orElse(rightCPFilter)
                    Some(TryMergeResult(cp.withNewChildren(Seq(mergedLeft, mergedRight)), npMapping,
                      npFilter, cpFilter))
                  } else {
                    None
                  }
                case _ => None
              }
            case _ => None
          }

        case (np: DataSourceV2ScanRelation, cp: DataSourceV2ScanRelation) =>
          tryMergeScanRelations(np, cp)

        // Otherwise merging is not possible.
        case _ => None
      })
  }

  /**
   * Whether the two scans pushed semantically equal filters. The two scans reference their columns
   * with different `ExprId`s, so the new plan's pushed filters are first remapped onto the cached
   * plan's attributes by column name before comparing. The remap is built from the relations' full
   * output (not the scans' pruned output) because `pushedFilters` may reference columns pruned out
   * of the scan output (e.g. an unselected partition column). The caller has already checked the
   * relations are canonically equal, so every column maps by name. Trivially true when neither side
   * pushed any filter (the common best-effort case).
   */
  private def samePushedFilters(
      np: DataSourceV2ScanRelation,
      cp: DataSourceV2ScanRelation): Boolean = {
    val cpOutputByName = cp.relation.output.map(a => a.name -> a).toMap
    val npToCp = AttributeMap[Attribute](
      np.relation.output.flatMap(a => cpOutputByName.get(a.name).map(a -> _)))
    ExpressionSet(np.pushedFilters.map(mapAttributes(_, npToCp))) == ExpressionSet(cp.pushedFilters)
  }

  /**
   * Phase 1 of the DSv2 scan merge: fuse two scans of the same table that differ only in projected
   * columns (and carry the same strict pushed filters) into a single scan reading the union of
   * their columns. The connector opts in via
   * [[org.apache.spark.sql.connector.read.SupportsScanMerging]]; Spark runs the real DSv2 pushdown
   * ([[V2ScanRelationPushDown]]) on a synthetic `Filter` over the relation, extracts the merged
   * scan, and verifies the (equal) strict filters remain fully enforced. The merged relation's
   * `pushedFilters` (computed by that pushdown) let Phase 2 (the [[Filter]] merge) re-push
   * them when it rebuilds the scan for row-group pruning. A differing post-scan filter is handled
   * by that Phase 2 propagation, not here. Phase 1 has no fallback, so any anomaly (a strict filter
   * the rebuilt scan does not fully enforce, an unexpected output schema) results in `None` (no
   * merge): it must be correct on its own.
   */
  private def tryMergeScanRelations(
      np: DataSourceV2ScanRelation,
      cp: DataSourceV2ScanRelation): Option[TryMergeResult] = {
    val mergeable =
      // Same table, options, catalog and identifier: the relation's canonical form covers all of
      // these (options compares by content via `CaseInsensitiveStringMap.equals`).
      np.relation.canonicalized == cp.relation.canonicalized &&
        // Neither side carries a pushdown a rebuilt scan cannot reproduce (aggregate, join,
        // variant extraction, limit, offset, top-N or sample).
        !np.hasMergeBlockingPushdown && !cp.hasMergeBlockingPushdown &&
        // Reported partitioning/ordering (e.g. storage-partitioned join, reported sort) is not
        // reconstructed by the rebuilt scan, so decline the merge rather than silently drop it.
        // Merging these can be added as a follow-up.
        np.keyGroupedPartitioning.isEmpty && cp.keyGroupedPartitioning.isEmpty &&
        np.ordering.isEmpty && cp.ordering.isEmpty &&
        // Both scans opt in to Spark-side merging.
        np.scan.isInstanceOf[SupportsScanMerging] && cp.scan.isInstanceOf[SupportsScanMerging] &&
        // Both pushed the same strict filters, so re-pushing reproduces both sides' row sets.
        samePushedFilters(np, cp)

    if (!mergeable) {
      return None
    }

    val relation = cp.relation
    val cpNames = cp.output.map(_.name).toSet
    val npOnly = np.output.filterNot(a => cpNames.contains(a.name))
    // cp columns keep cp's exprIds; np-only columns keep np's exprIds.
    val unionAttrs = cp.output ++ npOnly

    // Build the merged scan enforcing the (equal) strict filters over the union of columns. No
    // extra pruning here -- that is Phase 2's job, once the post-scan Filter conditions are known.
    buildMergedScan(relation, unionAttrs, cp.pushedFilters, pruning = None).flatMap { scan =>
      // The rebuilt scan reuses the relation's exprIds, which match cp's for the shared columns, so
      // cp's references stay valid without remapping; np's references are remapped via npMapping.
      val scanOutByName = scan.output.map(a => a.name -> a).toMap
      val npMapping =
        AttributeMap[Attribute](np.output.flatMap(a => scanOutByName.get(a.name).map(a -> _)))
      if (npMapping.size == np.output.size) {
        Some(TryMergeResult(scan, npMapping))
      } else {
        None
      }
    }
  }

  /**
   * Rebuilds the merged DSv2 scan via [[V2ScanRelationPushDown.rebuildScan]], projecting `columns`
   * and filtering by `strict` (plus best-effort `pruning`). This reuses the production pushdown end
   * to end -- the same filter translation, column pruning, determinism/subquery handling and
   * iterative PartitionPredicate second pass -- rather than reimplementing a slice of it here.
   *
   * `strict` filters must come back fully enforced (present in the rebuilt scan's `pushedFilters`);
   * otherwise `None`, because nothing above the leaf re-checks it. `pruning` conditions
   * are best-effort, but only sound ones are offered to the source: a pruning condition is dropped
   * unless it is deterministic (a non-deterministic predicate the source prunes on would drop rows
   * the enclosing Filter cannot recover) and references only the relation's own columns (propagated
   * boolean filter attributes are not columns of the relation).
   */
  private def buildMergedScan(
      relation: DataSourceV2Relation,
      columns: Seq[AttributeReference],
      strict: Seq[Expression],
      pruning: Option[Expression]): Option[DataSourceV2ScanRelation] = {
    val relationOut = AttributeSet(relation.output)
    // Strict filters must be expressible over the relation; if not, we cannot rebuild safely.
    if (!strict.forall(_.references.subsetOf(relationOut))) {
      return None
    }
    val relOutByName = relation.output.map(a => a.name -> a).toMap
    val projectList = columns.flatMap(a => relOutByName.get(a.name))
    if (projectList.size != columns.size) {
      return None
    }
    // strict is enforced; pruning (a single best-effort OR condition from the caller) is dropped
    // unless it is both deterministic and expressible over the relation. A non-deterministic
    // pruning predicate is unsound here: the source may prune rows using its own evaluation (e.g. a
    // separate rand() draw), and the enclosing Filter re-checks exactness with a different
    // evaluation, so pruned rows would be lost. Such pruning is dropped wholesale rather than
    // weakened -- best-effort pruning degrades gracefully. Pruning that references non-relation
    // attributes (propagated boolean filter aliases) is likewise dropped.
    val conds = strict ++
      pruning.filter(p => p.deterministic && p.references.subsetOf(relationOut))
    V2ScanRelationPushDown.rebuildScan(relation, projectList, conds).filter { scan =>
      // Every intended-strict filter must be fully enforced by the rebuilt scan (nothing above
      // re-checks it), and the scan must produce exactly the requested union of columns.
      val pushedSet = ExpressionSet(scan.pushedFilters)
      strict.forall(pushedSet.contains) &&
        scan.output.length == columns.length &&
        columns.forall(a => scan.output.exists(_.name == a.name))
    }
  }

  /**
   * Phase 2 of the DSv2 scan merge: recover the row-group pruning the OR-widen would lose.
   * Per-side post-scan conditions move into a boolean Project above the merged scan, so the scan
   * itself would read the full table. This walks through Project nodes to the merged
   * [[org.apache.spark.sql.connector.read.SupportsScanMerging]] scan and rebuilds it (via the real
   * pushdown) with `pruning` pushed as a best-effort filter on top of the strict filters it already
   * enforces; the enclosing Filter still enforces exactness. Phase 2 never aborts the merge: if the
   * rebuild cannot keep the strict filters strict, it leaves the Phase 1 scan in place -- correct,
   * just without the extra pruning. `pruning` is None when there is nothing to prune on.
   */
  private def rePushMergedScan(
      mergedChild: LogicalPlan,
      pruning: Option[Expression]): LogicalPlan = mergedChild match {
    case scanRel: DataSourceV2ScanRelation if scanRel.scan.isInstanceOf[SupportsScanMerging] =>
      buildMergedScan(scanRel.relation, scanRel.output, scanRel.pushedFilters, pruning)
        .map(rebuilt => scanRel.copy(scan = rebuilt.scan))
        .getOrElse(scanRel)
    case p: Project =>
      p.withNewChildren(Seq(rePushMergedScan(p.child, pruning)))
    case other => other
  }

  // Returns true when a filter attribute originating from `fromLeft` child of a join with
  // `joinType` can be safely propagated through that join to a parent Aggregate.
  //
  // Two conditions must both hold:
  //   1. The attribute is in the join's output (rules out the right side of LeftSemi/LeftAnti).
  //   2. The filter must originate from the non-nullable ("preserved") side of the join.
  //      When a filter is on the nullable side, the merged base plan no longer applies it to the
  //      nullable child's scan, so rows that were previously absent from that child reappear as
  //      matched join rows instead of unmatched NULL-padded rows. This changes aggregate
  //      expressions that use the NULL-padded column: e.g. for `sum(coalesce(col, default))`, an
  //      originally unmatched row would have contributed `default` via `coalesce(NULL, default)`,
  //      but in the merged plan the row is now matched with its real column value, fails the
  //      filter, and FILTER (WHERE false) discards it -- losing the `default` contribution
  //      entirely.
  private def filterSafeForJoin(fromLeft: Boolean, joinType: JoinType): Boolean =
    if (fromLeft) {
      // Left side is never NULL-padded in: Inner, LeftOuter, LeftSemi, LeftAnti, Cross.
      joinType match {
        case Inner | LeftOuter | LeftSemi | LeftAnti | Cross => true
        case _ => false  // RightOuter and FullOuter can NULL-pad the left side
      }
    } else {
      // Right side is never NULL-padded AND is in the join output in: Inner, RightOuter, Cross.
      joinType match {
        case Inner | RightOuter | Cross => true
        case _ => false  // LeftOuter/FullOuter can NULL-pad right; LeftSemi/LeftAnti drop right
      }
    }

  private def mapAttributes[T <: Expression](expr: T, outputMap: AttributeMap[Attribute]) = {
    expr.transform {
      case a: Attribute => outputMap.getOrElse(a, a)
    }.asInstanceOf[T]
  }

  // Remaps attributes of `newPlanExpressions` through `newPlanMapping`, then merges them with
  // `cachedPlanExpressions` into a single expression list.
  // Returns a pair of:
  //   1. The merged expression list
  //   2. New plan output map: ne.toAttribute -> merged plan attr (for parent nodes to remap
  //      new-plan-side expressions)
  //
  // When `newPlanFilter`/`cachedPlanFilter` are provided (filter propagation active), non-matching
  // expressions from each side are wrapped with `If(filterAttr, expr, null)`. This ensures that a
  // non-matching expression from one side evaluates to null for rows that belong to the other side,
  // which is safe for aggregate FILTER (WHERE ...) semantics and avoids computing values for
  // irrelevant rows. The filter attributes themselves are appended to the merged expression list so
  // they remain visible to the enclosing Aggregate that will consume them. A newPlanFilter with
  // isNew=false was reused from a previous merge round and is already present in the merged child
  // output, so it is not appended again.
  private def mergeNamedExpressions(
      newPlanExpressions: Seq[NamedExpression],
      cachedPlanExpressions: Seq[NamedExpression],
      newPlanMapping: AttributeMap[Attribute],
      newPlanFilter: Option[(Attribute, Boolean)] = None,
      cachedPlanFilter: Option[Attribute] = None) = {
    val mergedExpressions = mutable.ArrayBuffer[NamedExpression](cachedPlanExpressions: _*)
    val matchedCachedIndices = mutable.HashSet.empty[Int]
    val newNPMapping = AttributeMap(newPlanExpressions.map { ne =>
      val mapped = mapAttributes(ne, newPlanMapping)
      val withoutAlias = mapped match {
        case Alias(child, _) => child
        case e => e
      }
      val foundIdx = mergedExpressions.indexWhere {
        case Alias(child, _) => child semanticEquals withoutAlias
        case e => e semanticEquals withoutAlias
      }
      val resultAttr = if (foundIdx >= 0) {
        // Matching expression: both sides compute the same value, no wrapping needed.
        matchedCachedIndices += foundIdx
        mergedExpressions(foundIdx).toAttribute
      } else {
        // Non-matching expression from the new plan side: wrap with the new plan filter so it
        // is only computed for rows that belong to the new plan side. Plain attribute references
        // are not wrapped since reading a column value is free.
        val wrappedExpr: NamedExpression = newPlanFilter match {
          case Some((f, _)) if !withoutAlias.isInstanceOf[Attribute] =>
            Alias(If(f, withoutAlias, Literal(null, withoutAlias.dataType)), mapped.name)()
          case _ => mapped
        }
        mergedExpressions += wrappedExpr
        wrappedExpr.toAttribute
      }
      ne.toAttribute -> resultAttr
    })

    // Wrap unmatched cached expressions with the cached plan's filter so they are only computed for
    // rows that belong to the cached plan side. Plain attribute references are not wrapped.
    cachedPlanFilter.foreach { f =>
      for (i <- 0 until cachedPlanExpressions.size if !matchedCachedIndices.contains(i)) {
        mergedExpressions(i) match {
          case ce @ Alias(child, _) if !child.isInstanceOf[Attribute] =>
            // Preserve the original ExprId so parent references to this cached attribute stay valid
            // without a cp-side remapping. (The new-plan wrapping above uses a fresh ExprId because
            // those aliases are appended rather than replacing an existing entry.)
            mergedExpressions(i) =
              Alias(If(f, child, Literal(null, child.dataType)), ce.name)(
                exprId = ce.toAttribute.exprId)
          case _ => // attribute or alias-of-attribute, no wrapping needed
        }
      }
    }

    newPlanFilter.foreach {
      case (f, true) => mergedExpressions += f
      case _ =>
    }
    cachedPlanFilter.foreach(mergedExpressions += _)

    (mergedExpressions.toSeq, newNPMapping)
  }

  // Applies filter as a FILTER (WHERE ...) clause to every AggregateExpression in exprs,
  // combining with any pre-existing filter on the aggregate via AND.
  private def applyFilterToAggregateExpressions(
      exprs: Seq[NamedExpression],
      filter: Attribute): Seq[NamedExpression] = {
    exprs.map(_.transform {
      case ae: AggregateExpression =>
        val combinedFilter = ae.filter.fold[Expression](filter)(And(filter, _))
        val newAE = ae.copy(filter = Some(combinedFilter))
        newAE.copyTagsFrom(ae)
        newAE
    }.asInstanceOf[NamedExpression])
  }

  // Only allow aggregates of the same implementation because merging different implementations
  // could cause performance regression.
  private def supportedAggregateMerge(newPlan: Aggregate, cachedPlan: Aggregate) = {
    val aggregateExpressionsSeq = Seq(newPlan, cachedPlan).map { plan =>
      plan.aggregateExpressions.flatMap(_.collect {
        case a: AggregateExpression => a
      })
    }
    val groupByExpressionSeq = Seq(newPlan, cachedPlan).map(_.groupingExpressions)

    val Seq(newPlanSupportsHashAggregate, cachedPlanSupportsHashAggregate) =
      aggregateExpressionsSeq.zip(groupByExpressionSeq).map {
        case (aggregateExpressions, groupByExpressions) =>
          Aggregate.supportsHashAggregate(
            aggregateExpressions.flatMap(
              _.aggregateFunction.aggBufferAttributes), groupByExpressions)
      }

    newPlanSupportsHashAggregate && cachedPlanSupportsHashAggregate ||
      newPlanSupportsHashAggregate == cachedPlanSupportsHashAggregate && {
        val Seq(newPlanSupportsObjectHashAggregate, cachedPlanSupportsObjectHashAggregate) =
          aggregateExpressionsSeq.zip(groupByExpressionSeq).map {
            case (aggregateExpressions, groupByExpressions) =>
              Aggregate.supportsObjectHashAggregate(aggregateExpressions, groupByExpressions)
          }
        newPlanSupportsObjectHashAggregate && cachedPlanSupportsObjectHashAggregate ||
          newPlanSupportsObjectHashAggregate == cachedPlanSupportsObjectHashAggregate
      }
  }
}
