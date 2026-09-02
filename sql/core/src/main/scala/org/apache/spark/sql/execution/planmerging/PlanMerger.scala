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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeMap, AttributeSet, Expression, ExpressionSet, If, Literal, NamedExpression, Or, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation, V2ScanPartitioningAndOrdering, V2ScanRelationPushDown}
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
      SQLConf.get.getConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED),
    dsv2SymmetricFilterPropagationEnabled: Boolean =
      SQLConf.get.getConf(SQLConf.MERGE_SUBPLANS_DSV2_SYMMETRIC_FILTER_PROPAGATION_ENABLED),
    dsv2AllowKeyGroupedPartitioningDegradation: Boolean =
      SQLConf.get.getConf(SQLConf.MERGE_SUBPLANS_DSV2_ALLOW_KEY_GROUPED_PARTITIONING_DEGRADATION),
    dsv2AllowOrderingDegradation: Boolean =
      SQLConf.get.getConf(SQLConf.MERGE_SUBPLANS_DSV2_ALLOW_ORDERING_DEGRADATION)) {
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
          tryMergePlans(plan, mp.plan, MergeContext(filterPropagationSupported = false)).collect {
            case TryMergeResult(mergedPlan, npMapping, None, None, None, _) =>
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
   * @param dsv2Merged Whether an (equal-strict) DSv2 scan merge occurred anywhere in this merged
   *                   subtree. Unlike `dsv2DeferredScan` (consumed at the enclosing Filter that
   *                   builds the scan), this fact is propagated all the way up: it lets a Filter
   *                   pair that is NOT the innermost one still recognize the merge below it and
   *                   apply the DSv2-symmetric exemption. It only ever gates behaviour when
   *                   `dsv2SymmetricFilterPropagationEnabled` is on.
   */
  case class TryMergeResult(
      mergedPlan: LogicalPlan,
      newPlanMapping: AttributeMap[Attribute],
      newPlanFilter: Option[(Attribute, Boolean)] = None,
      cachedPlanFilter: Option[Attribute] = None,
      dsv2DeferredScan: Option[DSv2DeferredScan] = None,
      dsv2Merged: Boolean = false)

  /**
   * Carries a DSv2 scan whose build has been deferred from the leaf up to the enclosing [[Filter]],
   * so the merged scan is built exactly once per merge round (strict + best-effort filters
   * together) rather than once strict-only at the leaf and then rebuilt at the Filter.
   *
   * The relation to rebuild from is not carried here: the deferring leaf leaves it in the plan as
   * the placeholder [[TryMergeResult.mergedPlan]] (the sole bare [[DataSourceV2Relation]] in the
   * subtree), and `tryBuildFilterDSv2ScanChild` recovers it from there. Only what the tree does NOT
   * hold is carried: the projected `unionAttrs` and the `strictFilters` to re-enforce.
   *
   * @param unionAttrs The union of both sides' projected columns the merged scan must produce.
   * @param strictFilters The strict pushed filters that must be re-enforced by the rebuilt scan.
   * @param requiredKeyGroupedPartitioning The key-grouped partitioning the merged scan must
   *        reproduce to keep both inputs not-worse (the inputs' combined report, in the merged
   *        relation's attribute space); empty means no requirement. Enforced unless
   *        `dsv2AllowKeyGroupedPartitioningDegradation` is set.
   * @param requiredOrdering The output ordering the merged scan must satisfy likewise; empty means
   *        no requirement. Enforced unless `dsv2AllowOrderingDegradation` is set.
   */
  case class DSv2DeferredScan(
      unionAttrs: Seq[Attribute],
      strictFilters: Seq[Expression],
      requiredKeyGroupedPartitioning: Seq[Expression],
      requiredOrdering: Seq[SortOrder])

  /**
   * Context threaded DOWN through [[tryMergePlans]] recursion.
   *
   * Invariants:
   * - `filterAboveScan` is set true ONLY by the `(Filter, Filter)` arm; the leaf defers building
   *   the merged DSv2 scan ONLY when it is true. A deferred result flows leaf -> (pass-through
   *   Project arms, which propagate `dsv2DeferredScan` unchanged) -> the enclosing `(Filter,
   *   Filter)` arm, which builds it once (via `tryBuildFilterDSv2ScanChild`) and returns
   *   `dsv2DeferredScan = None`.
   * - A `(Filter, Filter)`'s children CAN themselves be Filters: `PartitionPruning` and
   *   `InjectRuntimeFilter` insert a Filter above an existing one after `CombineFilters` has run,
   *   and the `PushDownPredicates` pass that would re-combine them runs only in a later batch. The
   *   deferral is consumed at the INNERMOST `(Filter, Filter)` pair (which builds the scan); an
   *   enclosing pair sees `dsv2DeferredScan = None`, but `dsv2Merged` still marks the merge below
   *   it, so it can apply the DSv2-symmetric exemption too. This is sound because
   *   `V2ScanRelationPushDown` only pushes the innermost (scan-adjacent) Filter chain to the scan:
   *   an outer stacked Filter was never scan pruning (a runtime filter inserted after pushdown, or
   *   one separated from the scan by an operator that blocks pushdown), so OR-widening it above the
   *   built scan drops no pruning.
   * - The `merge()` pattern requiring `dsv2DeferredScan = None` is the fail-safe backstop: if a
   *   deferred scan somehow reached `merge()` unbuilt, it declines the merge rather than emitting a
   *   plan with a placeholder relation.
   * - Eligibility gating stays at the leaf (read-only, inspects only the two input scans) with one
   *   exception: whether the rebuilt merged scan degrades a partitioning/ordering an input reported
   *   can only be decided once that scan exists, so on the deferred path that one check runs at the
   *   Filter, next to the build (see `tryBuildFilterDSv2ScanChild`).
   */
  case class MergeContext(filterPropagationSupported: Boolean, filterAboveScan: Boolean = false)

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
      context: MergeContext): Option[TryMergeResult] = {
    // The plain "reuse the cached plan as-is" result, shared by every branch below. Lazy because
    // the DSv2 merge path under a Filter does not need it when the merge itself succeeds.
    lazy val identical = checkIdenticalPlans(newPlan, cachedPlan).map(TryMergeResult(cachedPlan, _))
    // A DSv2 scan pair is handled here in one place, rather than split between this leading check
    // and the structural match below. Under a Filter, merging DEFERS the scan build so the
    // enclosing Filter's row-group pruning is pushed in a single rebuild -- so try the merge first
    // and fall back to plain reuse if the scans cannot merge. Trying the merge first even when the
    // two scans are identical is deliberate: the deferred rebuild re-pushes the enclosing Filter's
    // condition as a best-effort filter, which plain reuse would leave as a post-scan Filter, so
    // reusing identical scans here would forfeit that pruning. Without a Filter there is no pruning
    // to recover, so reuse an identical scan as-is (no rebuild) and only merge scans that differ
    // (projected columns / strict filters). Any other plan pair uses the general reuse.
    val earlyResult = (newPlan, cachedPlan) match {
      case (np: DataSourceV2ScanRelation, cp: DataSourceV2ScanRelation) =>
        if (context.filterAboveScan) {
          tryMergeScanRelations(np, cp, context).orElse(identical)
        } else {
          identical.orElse(tryMergeScanRelations(np, cp, context))
        }
      case _ => identical
    }
    earlyResult.orElse(
      (newPlan, cachedPlan) match {
        case (np: Project, cp: Project) =>
          tryMergePlans(np.child, cp.child, context).map {
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter, deferred, dsv2Merged) =>
              val (mergedProjectList, newNPMapping) =
                mergeNamedExpressions(np.projectList, cp.projectList, npMapping, npFilter, cpFilter)
              TryMergeResult(Project(mergedProjectList, mergedChild), newNPMapping, npFilter,
                cpFilter, deferred, dsv2Merged)
          }
        case (np, cp: Project) =>
          tryMergePlans(np, cp.child, context).map {
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter, deferred, dsv2Merged) =>
              val (mergedProjectList, newNPMapping) =
                mergeNamedExpressions(np.output, cp.projectList, npMapping, npFilter, cpFilter)
              TryMergeResult(Project(mergedProjectList, mergedChild), newNPMapping, npFilter,
                cpFilter, deferred, dsv2Merged)
          }
        case (np: Project, cp) =>
          tryMergePlans(np.child, cp, context).map {
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter, deferred, dsv2Merged) =>
              val (mergedProjectList, newNPMapping) =
                mergeNamedExpressions(np.projectList, cp.output, npMapping, npFilter, cpFilter)
              TryMergeResult(Project(mergedProjectList, mergedChild), newNPMapping, npFilter,
                cpFilter, deferred, dsv2Merged)
          }

        case (np: Aggregate, cp: Aggregate) if supportedAggregateMerge(np, cp) =>
          // Filter propagation into the aggregate is only safe when there is no grouping.
          val childFilterPropagationSupported = filterPropagationEnabled &&
            np.groupingExpressions.isEmpty && cp.groupingExpressions.isEmpty
          tryMergePlans(np.child, cp.child,
              MergeContext(childFilterPropagationSupported, filterAboveScan = false)).flatMap {
            case TryMergeResult(mergedChild, npMapping, None, None, _, dsv2Merged) =>
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
                Some(TryMergeResult(mergedPlan, newNPMapping, dsv2Merged = dsv2Merged))
              } else {
                None
              }
            case TryMergeResult(mergedChild, npMapping, npFilterOpt, cpFilterOpt, _, dsv2Merged) =>
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
              Some(TryMergeResult(mergedPlan, newNPMapping, dsv2Merged = dsv2Merged))
          }

        case (np: Filter, cp: Filter) =>
          tryMergePlans(np.child, cp.child, context.copy(filterAboveScan = true)).flatMap {
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter, deferred, dsv2Merged) =>
              val mappedNPCondition = mapAttributes(np.condition, npMapping)
              // Comparing the canonicalized form is required to ignore different forms of the same
              // expression.
              if (mappedNPCondition.canonicalized == cp.condition.canonicalized) {
                // Identical conditions: the filter node adds no new discrimination between the two
                // sides, so keep it unchanged. If it sits above a deferred merged DSv2 scan, build
                // that scan once here with this condition as a best-effort filter. If it cannot be
                // built to spec -- the strict filters do not come back re-enforced, or the rebuilt
                // scan's re-derived partitioning/ordering would degrade what the inputs reported --
                // decline the whole merge (the leaf's strict-only build would have failed
                // identically).
                tryBuildFilterDSv2ScanChild(mergedChild, deferred, Some(cp.condition))
                  .map { prunedChild =>
                    val mergedPlan = Filter(cp.condition, prunedChild)
                    TryMergeResult(
                      mergedPlan, npMapping, npFilter, cpFilter, dsv2Merged = dsv2Merged)
                  }
              // Symmetric propagation broadens the merged scan to OR(f1, f2); it is off by default
              // because the two sides may read disjoint data. A DSv2 scan merge is exempt under its
              // own config: `dsv2Merged` marks an (equal-strict) DSv2 merge below -- whose equal
              // strict filters mean both sides read the same base set, so the OR only weakens the
              // best-effort filter. Unlike `deferred`, `dsv2Merged` survives past the innermost
              // Filter that built the scan, so a stacked outer Filter pair recognizes it too.
              } else if (context.filterPropagationSupported &&
                  (symmetricFilterPropagationEnabled ||
                    (dsv2SymmetricFilterPropagationEnabled && dsv2Merged))) {
                if (cp.getTagValue(PlanMerger.MERGED_FILTER_TAG).isDefined) {
                  // cp Filter is already a merged filter from a previous round: its condition
                  // is OR(f0, f1, ...) and its child Project already contains aliases for those
                  // attributes. Only create a new alias for the np side, and extend the OR
                  // condition. A tagged filter is always built with a Project child (see the
                  // first-time branch below), so a non-Project child should not happen; decline the
                  // merge rather than fail, keeping the merge best-effort.
                  mergedChild match {
                    case childProject: Project =>
                      val newNPCondition = npFilter.fold(mappedNPCondition) {
                        case (f, _) => And(f, mappedNPCondition)
                      }
                      // If newNPCondition is already aliased in the child Project (e.g. a third
                      // subplan whose filter matches one from a previous merge round), reuse the
                      // existing attribute instead of creating a redundant alias.
                      val existingNPFilter = childProject.projectList.collectFirst {
                        case a: Alias if a.child.canonicalized == newNPCondition.canonicalized =>
                          a.toAttribute
                      }
                      val (newProjectList, newCondition, newNPFilterOut) =
                        existingNPFilter match {
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
                      // Phase 2: the leaf re-merge rebuilt the scan with strict filters only,
                      // dropping the OR best-effort filter established in earlier rounds, so
                      // re-establish it here from ALL propagated conditions, not just the new
                      // side's. Only the aliases the OR condition references are filter sides;
                      // other aliases in the Project are computed columns, not filters.
                      val conditions = newProjectList.collect {
                        case a: Alias if newCondition.references.contains(a.toAttribute) => a.child
                      }
                      tryBuildFilterDSv2ScanChild(
                        childProject.child, deferred, conditions.reduceOption(Or))
                        .map { prunedChild =>
                          val newProject = childProject.copy(
                            projectList = newProjectList, child = prunedChild)
                          val newFilter = Filter(newCondition, newProject)
                          newFilter.copyTagsFrom(cp)
                          TryMergeResult(newFilter, npMapping, Some(newNPFilterOut), None,
                            dsv2Merged = dsv2Merged)
                        }
                    case _ =>
                      None
                  }
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
                  // The OR-widen moves both conditions into a boolean Project above the merged
                  // scan, so the scan itself would read the full table. Build the deferred scan
                  // here with OR(np condition, cp condition) as the best-effort filter (the Filter
                  // above still enforces exactness). The best-effort filter is derived from the
                  // scan-level conditions, not the propagated filter attributes in newNP/newCP.
                  tryBuildFilterDSv2ScanChild(
                    mergedChild, deferred, Some(Or(mappedNPCondition, cp.condition)))
                    .map { prunedChild =>
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
                      TryMergeResult(newFilter, npMapping, Some((newNPFilter, true)),
                        Some(newCPFilter), dsv2Merged = dsv2Merged)
                    }
                }
              } else {
                None
              }
          }
        case (np: Filter, cp) if context.filterPropagationSupported =>
          tryMergePlans(np.child, cp, context.copy(filterAboveScan = false)).collect {
            // If the cp side already propagated a filter from deeper recursion, the merge is
            // effectively symmetric (both sides have a filter condition). Abort unless
            // symmetricFilterPropagationEnabled.
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter, _, dsv2Merged)
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
              TryMergeResult(project, npMapping, Some((newNPFilter, true)), cpFilter,
                dsv2Merged = dsv2Merged)
          }
        case (np, cp: Filter) if context.filterPropagationSupported =>
          tryMergePlans(np, cp.child, context.copy(filterAboveScan = false)).collect {
            // If the np side already propagated a filter from deeper recursion, the merge is
            // effectively symmetric (both sides have a filter condition). Abort unless
            // symmetricFilterPropagationEnabled.
            case TryMergeResult(mergedChild, npMapping, npFilter, cpFilter, _, dsv2Merged)
                if npFilter.isEmpty || symmetricFilterPropagationEnabled =>
              if (cp.getTagValue(PlanMerger.MERGED_FILTER_TAG).isDefined) {
                // cp is a previously-merged Filter: its condition is `OR(pf_0, pf_1, ...)` and cp's
                // aggregate expressions already carry individual `FILTER (WHERE pf_i)` clauses that
                // restrict each aggregation to its originating side. Synthesising a new cpFilter
                // alias for cp.condition would just produce `FILTER AND(OR(pf_0, pf_1, ...), pf_i)`
                // upstream, which simplifies to `FILTER pf_i` -- wasted work and plan bloat.
                // Drop cp's Filter and let the recursion's result flow up with cpFilter = None so
                // cp's aggregates are left untouched.
                TryMergeResult(mergedChild, npMapping, npFilter, None, dsv2Merged = dsv2Merged)
              } else {
                val newCPCondition = cpFilter.fold(cp.condition)(And(_, cp.condition))
                val newCPFilterAlias =
                  Alias(newCPCondition, s"propagatedFilter_${PlanMerger.newId}")()
                val newCPFilter = newCPFilterAlias.toAttribute
                val project = Project(
                  mergedChild.output.toList :+ newCPFilterAlias,
                  mergedChild)
                TryMergeResult(project, npMapping, npFilter, Some(newCPFilter),
                  dsv2Merged = dsv2Merged)
              }
          }

        case (np: Join, cp: Join) if np.joinType == cp.joinType && np.hint == cp.hint =>
          tryMergePlans(np.left, cp.left, context.copy(filterAboveScan = false)).flatMap {
            case TryMergeResult(mergedLeft, leftNPMapping, leftNPFilter, leftCPFilter, _,
                leftDsv2Merged) =>
              tryMergePlans(np.right, cp.right, context.copy(filterAboveScan = false)).flatMap {
                case TryMergeResult(mergedRight, rightNPMapping, rightNPFilter, rightCPFilter, _,
                    rightDsv2Merged)
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
                      npFilter, cpFilter, dsv2Merged = leftDsv2Merged || rightDsv2Merged))
                  } else {
                    None
                  }
                case _ => None
              }
            case _ => None
          }

        // Otherwise merging is not possible.
        case _ => None
      })
  }

  /**
   * The DSv2 scan merge: fuse two scans of the same table that differ only in projected columns
   * (and carry the same strict pushed filters) into a single scan reading the union of their
   * columns. The connector opts in via
   * `TableCapability.SCAN_MERGING`; Spark runs the real DSv2 pushdown
   * ([[V2ScanRelationPushDown]]) on a synthetic `Filter` over the relation, extracts the merged
   * scan, and verifies the (equal) strict filters remain fully enforced. The `mergeable` gate is
   * read-only, inspecting only the two input scans; two further checks can decline the merge -- the
   * inputs' reported partitioning/ordering must combine into a single report (checked here, before
   * any rebuild), and the rebuilt scan must not degrade that report (checked once it is built).
   *
   * When this scan sits under a [[Filter]] (`context.filterAboveScan`), the build is DEFERRED: the
   * merged scan is not built here but carried up as a [[DSv2DeferredScan]] and built exactly once
   * at the enclosing Filter (via `tryBuildFilterDSv2ScanChild`), where the strict filters and the
   * Filter's best-effort row-group pruning are known and can be pushed together in a single
   * rebuild. The placeholder plan returned in that case is the bare relation (its output is a
   * superset of the union columns); the Filter arm splices the built scan in by reference identity.
   * Otherwise (no enclosing Filter) the scan is built here, strict-only.
   *
   * A differing post-scan filter is handled by filter propagation at the Filter, not here. There is
   * no fallback, so any anomaly (a strict filter the rebuilt scan does not fully enforce, an
   * unexpected output schema) results in `None` (no merge): it must be correct on its own.
   */
  private def tryMergeScanRelations(
      np: DataSourceV2ScanRelation,
      cp: DataSourceV2ScanRelation,
      context: MergeContext): Option[TryMergeResult] = {
    // np's relation attributes paired with cp's by position, reused below. Safe because the
    // `mergeable` gate requires the two relations to be canonically equal, so both list the table's
    // columns in the same order; lazy so it is only built once that check has passed. A scan's
    // (pruned) `output` is a subset of its `relation.output` -- even where nested field pruning
    // narrows a column's type -- so this maps each scan's output, and its pushed filters (which
    // reference the relation's full output), through to the other scan's relation.
    lazy val npRelationMapping = AttributeMap[Attribute](np.relation.output.zip(cp.relation.output))

    // Each side must read a subset of its relation's columns AT THE SAME type. Column pruning may
    // drop columns, but a struct/array/map column read at a narrower type via nested schema pruning
    // leaves its extractors (e.g. GetStructField ordinals) resolved against the narrow layout; the
    // merge rebuilds the column at the relation's full type, so those ordinals would read the wrong
    // field. This subset property also guarantees every output attribute maps through
    // npRelationMapping (and, in tryBuildMergedDSv2Scan, back to the relation). Widening the
    // merged scan to the union of nested fields and remapping the ordinals is a possible follow-up.
    def readsSubsetOfRelation(scan: DataSourceV2ScanRelation): Boolean = {
      val relTypes = AttributeMap(scan.relation.output.map(a => a -> a.dataType))
      scan.output.forall(a => relTypes.get(a).contains(a.dataType))
    }

    val mergeable =
      // Same table, options, catalog and identifier: the relation's canonical form covers all of
      // these (options compares by content via `CaseInsensitiveStringMap.equals`).
      np.relation.canonicalized == cp.relation.canonicalized &&
        // Both scans are mergeable: each came out of the plain column-pruning + filter pushdown
        // path carrying only pushdowns a rebuilt scan can reproduce. A scan with a non-reproducible
        // pushdown (aggregate, join, variant, limit, offset, top-N, sample) or built by any other
        // rule is not mergeable by default.
        np.mergeableScan && cp.mergeableScan &&
        // Reported partitioning/ordering is not reconstructed by the rebuilt scan
        // (V2ScanPartitioningAndOrdering is a separate early rule that rebuildScan does not run).
        // Rather than decline here, the merged scan re-derives its own when built (see
        // tryBuildMergedDSv2Scan), and mergeDegradesReporting declines the merge if that would
        // degrade what an input reported (unless a dsv2ScanMerge config allows it).
        // The table opts in to Spark-side merging (a table capability, so a V1-fallback source
        // whose scan Spark wraps can still opt in). Both relations are the same table (canonically
        // equal, checked above), but check each to be safe.
        np.relation.table.capabilities().contains(TableCapability.SCAN_MERGING) &&
        cp.relation.table.capabilities().contains(TableCapability.SCAN_MERGING) &&
        // Each side reads a subset of its relation's columns at the same type (see above).
        readsSubsetOfRelation(np) && readsSubsetOfRelation(cp) &&
        // Both pushed the same strict filters, so re-pushing reproduces both sides' row sets. Remap
        // np's pushed filters onto cp's via npRelationMapping (the full relation-to-relation
        // mapping, since a pushed filter may reference a column pruned out of np.output) before
        // comparing as sets.
        ExpressionSet(np.pushedFilters.map(mapAttributes(_, npRelationMapping))) ==
          ExpressionSet(cp.pushedFilters)

    // The read-only gate above is settled. The report combine below can still decline (and so can
    // the degradation check once the scan is built); everything else below constructs the merge.
    if (!mergeable) {
      return None
    }

    // np's columns expressed in cp's relation space; the subset check above guarantees each maps.
    val npMapping = AttributeMap(np.output.map(a => a -> npRelationMapping(a)))
    // cp's columns are already cp.relation attributes; append the np-only columns, in np.output
    // order (npMapping.values would be exprId-hash-ordered).
    val unionAttrs = cp.output ++ np.output.map(npMapping).filterNot(cp.outputSet.contains)

    // The reported key-grouped partitioning / ordering the merged scan must preserve so BOTH inputs
    // stay not-worse. Each input reports its own, remapped into cp's relation space (cp's already
    // is; np's via npRelationMapping). The two normally agree -- the clustering expressions come
    // from the table, and for partitioning, pruning only ever drops a report wholesale (an empty
    // side, which never constrains) -- but a source is free to report per scan, e.g. an ordering
    // that holds only for the file set the filters pushed into that scan left behind. Combine them
    // into the single report the merge must keep (kGP: equal; ordering: the stronger, which
    // satisfies both). None from combine* means the inputs are INCOMPATIBLE -- no rebuilt scan
    // could keep both not-worse -- so decline HERE, before rebuilding, unless the matching config
    // accepts degrading that dimension.
    val combinedKeyGroupedPartitioning = combineRequiredKeyGroupedPartitioning(
      np.keyGroupedPartitioning.map(_.map(mapAttributes(_, npRelationMapping))).getOrElse(Nil),
      cp.keyGroupedPartitioning.getOrElse(Nil))
    val combinedOrdering = combineRequiredOrdering(
      np.ordering.map(_.map(mapAttributes(_, npRelationMapping))).getOrElse(Nil),
      cp.ordering.getOrElse(Nil))
    if ((combinedKeyGroupedPartitioning.isEmpty && !dsv2AllowKeyGroupedPartitioningDegradation) ||
        (combinedOrdering.isEmpty && !dsv2AllowOrderingDegradation)) {
      return None
    }
    // Empty = no requirement (both inputs reported none, or they were incompatible but the config
    // accepts the degradation). Otherwise the single report the merged scan must reproduce/satisfy.
    val expectedKeyGroupedPartitioning = combinedKeyGroupedPartitioning.getOrElse(Nil)
    val expectedOrdering = combinedOrdering.getOrElse(Nil)

    if (context.filterAboveScan) {
      // Defer the build to the enclosing Filter so the scan is built once with strict +
      // best-effort filters. The placeholder mergedPlan is the bare relation (its output is a
      // superset of unionAttrs). rebuildScan reuses the relation's attributes, so mapping
      // np.output to cp's relation attributes is consistent with the eventual built scan.
      Some(TryMergeResult(cp.relation, npMapping,
        dsv2DeferredScan = Some(DSv2DeferredScan(unionAttrs, cp.pushedFilters,
          expectedKeyGroupedPartitioning, expectedOrdering)), dsv2Merged = true))
    } else {
      // No enclosing Filter: build the merged scan here enforcing the (equal) strict filters over
      // the union of columns, with no best-effort filter (no post-scan Filter to prune on).
      tryBuildMergedDSv2Scan(cp.relation, unionAttrs, cp.pushedFilters, bestEffortFilter = None)
        .filterNot(mergeDegradesReporting(_, expectedKeyGroupedPartitioning, expectedOrdering))
        .map(TryMergeResult(_, npMapping, dsv2Merged = true))
    }
  }

  /**
   * Rebuilds the merged DSv2 scan via [[V2ScanRelationPushDown.rebuildScan]], projecting
   * `unionAttrs` and filtering by `strictFilters` (plus the `bestEffortFilter`). This reuses
   * the production pushdown end to end -- the same filter translation, column pruning,
   * determinism/subquery handling and iterative PartitionPredicate second pass -- rather than
   * reimplementing a slice of it here.
   *
   * `strictFilters` must come back fully enforced (present in the rebuilt scan's `pushedFilters`);
   * otherwise `None`, because nothing above the leaf re-checks it. The `bestEffortFilter` is
   * offered to the source only when sound: it is dropped unless it is deterministic (a
   * non-deterministic predicate the source prunes on would drop rows the enclosing Filter cannot
   * recover) and references only the relation's own columns (propagated boolean filter attributes
   * are not columns of the relation).
   *
   * The rebuilt scan's reported partitioning/ordering is re-derived here, but NOT checked against
   * what the inputs reported -- callers do that via [[mergeDegradesReporting]], so a degradation
   * stays distinguishable from a filter-enforcement failure.
   */
  private def tryBuildMergedDSv2Scan(
      relation: DataSourceV2Relation,
      unionAttrs: Seq[Attribute],
      strictFilters: Seq[Expression],
      bestEffortFilter: Option[Expression]): Option[DataSourceV2ScanRelation] = {
    val relationOut = relation.outputSet
    // Defensive: strict filters come from `pushedFilters`, which reference only relation columns,
    // so this holds today. If a future caller offers a filter over non-relation attributes, decline
    // the merge rather than build an unsound scan (there is no fallback above the leaf).
    if (!strictFilters.forall(_.references.subsetOf(relationOut))) {
      return None
    }
    // `unionAttrs` are the relation's own attributes (the caller builds the union in the relation's
    // space), so they are the projection directly.
    // strictFilters are enforced; the bestEffortFilter is offered too, but only when it is
    // expressible over the relation -- a condition referencing propagated boolean filter aliases
    // rather than relation columns is dropped. A non-deterministic predicate needs no handling
    // here: SPARK-58207 keeps non-deterministic filters from being pushed to a V2 source, so an
    // offered one is simply not pushed (and dropped when the scan is extracted).
    val conds = strictFilters ++ bestEffortFilter.filter(_.references.subsetOf(relationOut))
    V2ScanRelationPushDown.rebuildScan(relation, unionAttrs, conds).filter { scan =>
      // The rebuilt scan must itself be mergeable. rebuildScan re-runs the full pushdown, so any
      // non-reproducible pushdown it introduces would make the merged scan unsound. Today's
      // Project-over-Filter input only triggers the plain path (always mergeable), so this is
      // defensive: it re-validates the rebuild's output against the same gate applied to its
      // inputs, rather than trusting the rebuild if its input plan ever broadens.
      scan.mergeableScan &&
        // Every intended-strict filter must be fully enforced by the rebuilt scan (nothing above
        // re-checks it), and the scan must produce exactly the requested union of columns.
        strictFilters.forall(ExpressionSet(scan.pushedFilters).contains) &&
        scan.outputSet == AttributeSet(unionAttrs)
    }.map { scan =>
      // rebuildScan returns the merged scan with reported partitioning/ordering unset
      // (V2ScanPartitioningAndOrdering is a separate early rule the rebuild does not run), so
      // re-derive them on this single node. Safe on one node: the partitioning pass is idempotent
      // and the ordering pass is applied once to a fresh node.
      V2ScanPartitioningAndOrdering(scan).asInstanceOf[DataSourceV2ScanRelation]
    }
  }

  /**
   * Threads the deferred DSv2 scan build through a [[Filter]] arm. If the merged child carries a
   * [[DSv2DeferredScan]], build the scan once here -- at the enclosing Filter, with the strict
   * filters plus the Filter's `bestEffortFilter` -- and splice it in place of the placeholder
   * relation. Tries strict + best-effort first, then strict-only (the best-effort filter is
   * droppable); a build returns `None` only if the strict filters cannot be re-enforced at all (the
   * leaf's strict-only build would have failed identically). Each built scan is also checked
   * against the required report the leaf computed, and rejected if it would degrade that -- per
   * attempt, so a source whose report depends on which filters were pushed can still satisfy it
   * strict-only. Either way the caller must decline the merge. If there is no deferred scan (a
   * non-DSv2 child, or a scan not under a Filter), there is nothing to build and the child is
   * returned unchanged.
   */
  private def tryBuildFilterDSv2ScanChild(
      child: LogicalPlan,
      dsv2DeferredScan: Option[DSv2DeferredScan],
      bestEffortFilter: Option[Expression]): Option[LogicalPlan] = dsv2DeferredScan match {
    case None => Some(child)
    case Some(d) =>
      // The deferring leaf left the relation to rebuild from in the plan as the placeholder
      // mergedPlan. It is the sole bare DataSourceV2Relation in the subtree (early pushdown has
      // turned every other relation into a DataSourceV2ScanRelation), so recover it by type here
      // rather than carrying it on DSv2DeferredScan.
      child.collectFirst { case r: DataSourceV2Relation => r }.flatMap { relation =>
        // Check the report per attempt, and at the caller rather than inside the build: the strict
        // filters and the report are independent reasons to reject a build, so a source whose
        // report depends on what got pushed still gets its second chance from the strict-only
        // attempt, and `tryBuildMergedDSv2Scan`'s `None` keeps its single meaning. The strict-only
        // attempt is what the leaf builds when no Filter is above the scan, so checking only the
        // first attempt would leave the deferred path weaker than the leaf path.
        def build(offeredBestEffortFilter: Option[Expression]) =
          tryBuildMergedDSv2Scan(
            relation, d.unionAttrs, d.strictFilters, offeredBestEffortFilter)
            .filterNot(
              mergeDegradesReporting(_, d.requiredKeyGroupedPartitioning, d.requiredOrdering))

        build(bestEffortFilter)
          .orElse(build(None))
          .map { built =>
            child.transformUp { case r: DataSourceV2Relation if r eq relation => built }
          }
      }
  }

  // The key-grouped partitioning the merged scan must reproduce to keep both inputs not-worse: the
  // two must be equal, so a differing non-empty pair is INCOMPATIBLE (None); an empty side imposes
  // no constraint. Compared in cp's relation space (np's report was remapped into it by the
  // caller). A report carrying a `TransformExpression` compares equal across the two inputs only if
  // the connector's `BoundFunction` implements `equals` -- Spark does not derive that identity
  // itself, see `BoundFunction#equals` -- so a connector that does not gives up this merge.
  private def combineRequiredKeyGroupedPartitioning(
      a: Seq[Expression], b: Seq[Expression]): Option[Seq[Expression]] = {
    if (a.isEmpty) Some(b)
    else if (b.isEmpty) Some(a)
    else if (a.map(_.canonicalized) == b.map(_.canonicalized)) Some(a)
    else None
  }

  // The ordering the merged scan must satisfy to keep both inputs not-worse: the stronger of the
  // two (the one that satisfies the other -- satisfying it implies satisfying the weaker). If
  // neither satisfies the other they are INCOMPATIBLE (None). An empty ordering never constrains.
  private def combineRequiredOrdering(
      a: Seq[SortOrder], b: Seq[SortOrder]): Option[Seq[SortOrder]] = {
    if (SortOrder.orderingSatisfies(a, b)) Some(a)
    else if (SortOrder.orderingSatisfies(b, a)) Some(b)
    else None
  }

  // True when the rebuilt merged scan does not reproduce the required key-grouped partitioning, or
  // does not satisfy the required ordering (the combined report the merge must preserve, computed
  // at the leaf) -- that can force a shuffle/sort the original plan avoided. Gated per dimension by
  // the dsv2ScanMerge degradation configs; an empty required report imposes no constraint. Compared
  // in cp's relation space.
  //
  // Only the reported EXPRESSIONS are compared, which is all a DataSourceV2ScanRelation carries;
  // the merged scan's split count and partition values can still differ from an input's, since it
  // may push a different best-effort filter and so prune differently. And a report the merged scan
  // GAINS is not a degradation either. For partitioning that is because an input dropped its own
  // only where a pruned column left the expressions inexpressible over that scan's output
  // (V2ScanPartitioningAndOrdering's partitioning pass is reference-subset guarded), not because
  // the source stopped reporting; the ordering pass has no such guard, so an ordering report is
  // never dropped by pruning and a gained one can only come from the source. Either way, keeping it
  // is exactly the win this merge is after.
  private def mergeDegradesReporting(
      merged: DataSourceV2ScanRelation,
      requiredKeyGroupedPartitioning: Seq[Expression],
      requiredOrdering: Seq[SortOrder]): Boolean = {
    val kgpDegraded = !dsv2AllowKeyGroupedPartitioningDegradation &&
      requiredKeyGroupedPartitioning.nonEmpty &&
      !merged.keyGroupedPartitioning.exists(
        _.map(_.canonicalized) == requiredKeyGroupedPartitioning.map(_.canonicalized))
    val orderingDegraded = !dsv2AllowOrderingDegradation &&
      requiredOrdering.nonEmpty &&
      !SortOrder.orderingSatisfies(merged.ordering.getOrElse(Nil), requiredOrdering)
    kgpDegraded || orderingDegraded
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

  private def mapAttributes[T <: Expression](expr: T, outputMap: AttributeMap[_ <: Attribute]) = {
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
