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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeMap, Expression, If, Literal, NamedExpression, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
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
  private[optimizer] val curId = new java.util.concurrent.atomic.AtomicLong()
  private[optimizer] def newId: Long = curId.getAndIncrement()
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
 * When both sides carry a [[Filter]] (the symmetric case), merging broadens the scan to
 * OR(f1, f2), which may reduce IO pruning. This path is separately gated by
 * `symmetricFilterPropagationEnabled`.
 * When plans also differ in intermediate [[Project]] expressions, those are wrapped with
 * `If(filterAttr, expr, null)` to avoid computing the expression for rows that do not
 * match that side's filter condition.
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
      SQLConf.get.getConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED)) {
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
          val newMergedPlan = MergedPlan(mp.plan, cache(i).merged || !subqueryPlan)
          cache(i) = newMergedPlan
          val outputMap = AttributeMap(plan.output.zipWithIndex)
          MergeResult(newMergedPlan, i, outputMap)
        }.orElse {
          tryMergePlans(plan, mp.plan, false).collect {
            case TryMergeResult(mergedPlan, npMapping, _, None, None) =>
              val newMergePlan = MergedPlan(mergedPlan, true)
              cache(i) = newMergePlan
              val outputMap = AttributeMap(npMapping.iterator.map { case (origAttr, mergedAttr) =>
                origAttr -> mergedPlan.output.indexWhere(_.exprId == mergedAttr.exprId)
              }.toSeq)
              MergeResult(newMergePlan, i, outputMap)
          }
        }
      case _ => None
    }).getOrElse {
      val newMergePlan = MergedPlan(plan, false)
      cache += newMergePlan
      val outputMap = AttributeMap(plan.output.zipWithIndex)
      MergeResult(newMergePlan, cache.length - 1, outputMap)
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
   * @param cachedPlanMapping Mapping from original cached-plan attributes to their new alias
   *                          attributes when a cached expression was wrapped with an `If`. Used by
   *                          parent nodes to remap cached-plan-side expressions that would
   *                          otherwise reference stale attributes after wrapping. Empty when no
   *                          cached expressions were wrapped.
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
      cachedPlanMapping: AttributeMap[Attribute] = AttributeMap.empty,
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
   * - Join nodes: Only if join type, hints, and conditions are identical
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
            case TryMergeResult(mergedChild, npMapping, cpMapping, npFilter, cpFilter) =>
              val (mergedProjectList, newNPMapping, newCPMapping) =
                mergeNamedExpressions(np.projectList, cp.projectList, npMapping, cpMapping,
                  npFilter, cpFilter)
              TryMergeResult(Project(mergedProjectList, mergedChild), newNPMapping, newCPMapping,
                npFilter, cpFilter)
          }
        case (np, cp: Project) =>
          tryMergePlans(np, cp.child, filterPropagationSupported).map {
            case TryMergeResult(mergedChild, npMapping, cpMapping, npFilter, cpFilter) =>
              val (mergedProjectList, newNPMapping, newCPMapping) =
                mergeNamedExpressions(np.output, cp.projectList, npMapping, cpMapping, npFilter,
                  cpFilter)
              TryMergeResult(Project(mergedProjectList, mergedChild), newNPMapping, newCPMapping,
                npFilter, cpFilter)
          }
        case (np: Project, cp) =>
          tryMergePlans(np.child, cp, filterPropagationSupported).map {
            case TryMergeResult(mergedChild, npMapping, cpMapping, npFilter, cpFilter) =>
              val (mergedProjectList, newNPMapping, newCPMapping) =
                mergeNamedExpressions(np.projectList, cp.output, npMapping, cpMapping, npFilter,
                  cpFilter)
              TryMergeResult(Project(mergedProjectList, mergedChild), newNPMapping, newCPMapping,
                npFilter, cpFilter)
          }

        case (np: Aggregate, cp: Aggregate) if supportedAggregateMerge(np, cp) =>
          // Filter propagation into the aggregate is only safe when there is no grouping.
          val childFilterPropagationSupported = filterPropagationEnabled &&
            np.groupingExpressions.isEmpty && cp.groupingExpressions.isEmpty
          tryMergePlans(np.child, cp.child, childFilterPropagationSupported).flatMap {
            case TryMergeResult(mergedChild, npMapping, cpMapping, None, None) =>
              val mappedNPGroupingExpression =
                np.groupingExpressions.map(mapAttributes(_, npMapping))
              val mappedCPGroupingExpression =
                cp.groupingExpressions.map(mapAttributes(_, cpMapping))
              // Order of grouping expression does matter as merging different grouping orders can
              // introduce "extra" shuffles/sorts that might not present in all of the original
              // subqueries.
              if (mappedNPGroupingExpression.map(_.canonicalized) ==
                  mappedCPGroupingExpression.map(_.canonicalized)) {
                val (mergedAggregateExpressions, newNPMapping, newCPMapping) =
                  mergeNamedExpressions(np.aggregateExpressions, cp.aggregateExpressions, npMapping,
                    cpMapping)
                val mergedPlan =
                  Aggregate(mappedCPGroupingExpression, mergedAggregateExpressions, mergedChild)
                Some(TryMergeResult(mergedPlan, newNPMapping, newCPMapping))
              } else {
                None
              }
            case TryMergeResult(mergedChild, npMapping, cpMapping, npFilterOpt, cpFilterOpt) =>
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
              val (mergedAggregateExpressions, newNPMapping, newCPMapping) =
                mergeNamedExpressions(filteredNPAggregateExpressions,
                  filteredCPAggregateExpressions, npMapping, cpMapping)
              val mergedPlan = Aggregate(Seq.empty, mergedAggregateExpressions, mergedChild)
              Some(TryMergeResult(mergedPlan, newNPMapping, newCPMapping))
          }

        case (np: Filter, cp: Filter) =>
          tryMergePlans(np.child, cp.child, filterPropagationSupported).flatMap {
            case TryMergeResult(mergedChild, npMapping, cpMapping, npFilter, cpFilter) =>
              val mappedNPCondition = mapAttributes(np.condition, npMapping)
              val mappedCPCondition = mapAttributes(cp.condition, cpMapping)
              // Comparing the canonicalized form is required to ignore different forms of the same
              // expression.
              if (mappedNPCondition.canonicalized == mappedCPCondition.canonicalized) {
                // Identical conditions: the filter node itself adds no new discrimination between
                // the two sides, so we keep it unchanged and pass the child's mappings up.
                val mergedPlan = Filter(mappedCPCondition, mergedChild)
                Some(TryMergeResult(mergedPlan, npMapping, cpMapping, npFilter, cpFilter))
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
                  existingNPFilter match {
                    case Some(reusedFilter) =>
                      Some(TryMergeResult(cp, npMapping, cpMapping, Some((reusedFilter, false)),
                        None))
                    case None =>
                      val newNPFilterAlias =
                        Alias(newNPCondition, s"propagatedFilter_${PlanMerger.newId}")()
                      val newNPFilter = newNPFilterAlias.toAttribute
                      val newProject = childProject.copy(
                        projectList = childProject.projectList ++ Seq(newNPFilterAlias))
                      val newFilter = Filter(Or(mappedCPCondition, newNPFilter), newProject)
                      newFilter.copyTagsFrom(cp)
                      Some(TryMergeResult(newFilter, npMapping, cpMapping,
                        Some((newNPFilter, true)), None))
                  }
                } else {
                  // First-time filter propagation: alias both sides' conditions as boolean
                  // attributes in a new Project below the Filter, and set the Filter condition
                  // to OR(newNPFilter, newCPFilter).
                  // Note: the new Project always uses mergedChild as its child (rather than
                  // flattening into an existing Project below) because mergedChild.output may
                  // contain previously-propagated filter attributes that newCPCondition
                  // references.
                  val newNPCondition = npFilter.fold(mappedNPCondition) {
                    case (f, _) => And(f, mappedNPCondition)
                  }
                  val newCPCondition = cpFilter.fold(mappedCPCondition)(And(_, mappedCPCondition))
                  val newNPFilterAlias =
                    Alias(newNPCondition, s"propagatedFilter_${PlanMerger.newId}")()
                  val newCPFilterAlias =
                    Alias(newCPCondition, s"propagatedFilter_${PlanMerger.newId}")()
                  val newNPFilter = newNPFilterAlias.toAttribute
                  val newCPFilter = newCPFilterAlias.toAttribute
                  val project = Project(
                    mergedChild.output.toList ++ Seq(newNPFilterAlias, newCPFilterAlias),
                    mergedChild)
                  val newFilter = Filter(Or(newNPFilter, newCPFilter), project)
                  newFilter.copyTagsFrom(cp)
                  newFilter.setTagValue(PlanMerger.MERGED_FILTER_TAG, ())
                  Some(TryMergeResult(newFilter, npMapping, cpMapping, Some((newNPFilter, true)),
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
            case TryMergeResult(mergedChild, npMapping, cpMapping, npFilter, cpFilter)
                if cpFilter.isEmpty || symmetricFilterPropagationEnabled =>
              val mappedNPCondition = mapAttributes(np.condition, npMapping)
              val newNPCondition = npFilter.fold(mappedNPCondition) {
                case (f, _) => And(f, mappedNPCondition)
              }
              val newNPFilterAlias =
                Alias(newNPCondition, s"propagatedFilter_${PlanMerger.newId}")()
              val newNPFilter = newNPFilterAlias.toAttribute
              val project = Project(
                mergedChild.output.toList ++ Seq(newNPFilterAlias) ++ cpFilter.toSeq,
                mergedChild)
              TryMergeResult(project, npMapping, cpMapping, Some((newNPFilter, true)), cpFilter)
          }
        case (np, cp: Filter) if filterPropagationSupported =>
          tryMergePlans(np, cp.child, filterPropagationSupported).collect {
            // If the np side already propagated a filter from deeper recursion, the merge is
            // effectively symmetric (both sides have a filter condition). Abort unless
            // symmetricFilterPropagationEnabled.
            case TryMergeResult(mergedChild, npMapping, cpMapping, npFilter, cpFilter)
                if npFilter.isEmpty || symmetricFilterPropagationEnabled =>
              val mappedCPCondition = mapAttributes(cp.condition, cpMapping)
              val newCPCondition = cpFilter.fold(mappedCPCondition)(And(_, mappedCPCondition))
              val newCPFilterAlias =
                Alias(newCPCondition, s"propagatedFilter_${PlanMerger.newId}")()
              val newCPFilter = newCPFilterAlias.toAttribute
              val project = Project(
                mergedChild.output.toList ++ npFilter.map(_._1).toSeq ++ Seq(newCPFilterAlias),
                mergedChild)
              TryMergeResult(project, npMapping, cpMapping, npFilter, Some(newCPFilter))
          }

        case (np: Join, cp: Join) if np.joinType == cp.joinType && np.hint == cp.hint =>
          // Filter propagation across joins is not yet supported.
          tryMergePlans(np.left, cp.left, false).flatMap {
            case TryMergeResult(mergedLeft, leftNPMapping, _, None, None) =>
              tryMergePlans(np.right, cp.right, false).flatMap {
                case TryMergeResult(mergedRight, rightNPMapping, _, None, None) =>
                  val npMapping = leftNPMapping ++ rightNPMapping
                  val mappedNPCondition = np.condition.map(mapAttributes(_, npMapping))
                  // Comparing the canonicalized form is required to ignore different forms of the
                  // same expression and `AttributeReference.qualifier`s in `cp.condition`.
                  if (mappedNPCondition.map(_.canonicalized) == cp.condition.map(_.canonicalized)) {
                    val mergedPlan = cp.withNewChildren(Seq(mergedLeft, mergedRight))
                    Some(TryMergeResult(mergedPlan, npMapping))
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

  private def mapAttributes[T <: Expression](expr: T, outputMap: AttributeMap[Attribute]) = {
    expr.transform {
      case a: Attribute => outputMap.getOrElse(a, a)
    }.asInstanceOf[T]
  }

  // Remaps attributes of `newPlanExpressions` through `newPlanMapping` and attributes of
  // `cachedPlanExpressions` through `cachedPlanMapping`, then merges them into a single
  // expression list.
  // Returns a triple of:
  //   1. The merged expression list
  //   2. New plan output map: ne.toAttribute -> merged plan attr (for parent nodes to remap
  //      new-plan-side expressions)
  //   3. Cached plan output map: old wrapped cached attr -> new alias attr (for parent nodes to
  //      remap cached-plan-side expressions that would otherwise reference stale attributes after
  //      wrapping). Empty when no cached expressions were wrapped.
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
      cachedPlanMapping: AttributeMap[Attribute] = AttributeMap.empty,
      newPlanFilter: Option[(Attribute, Boolean)] = None,
      cachedPlanFilter: Option[Attribute] = None) = {
    val mergedExpressions = mutable.ArrayBuffer[NamedExpression](
      cachedPlanExpressions.map(mapAttributes(_, cachedPlanMapping)): _*)
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

    // Wrap unmatched cached expressions with the cached plan's filter so they are only computed
    // for rows that belong to the cached plan side. Plain attribute references are not wrapped.
    // Record each attr rewrite in the cached plan map so ancestor nodes can remap their stale
    // references.
    val newCPMapping = AttributeMap(cachedPlanFilter.toSeq.flatMap { f =>
      mergedExpressions.zipWithIndex.flatMap {
        case (ce, i) if !matchedCachedIndices.contains(i) =>
          val withoutAlias = ce match {
            case Alias(child, _) => child
            case e => e
          }
          // Plain attribute references are not wrapped: no remapping entry needed.
          Option.when(!withoutAlias.isInstanceOf[Attribute]) {
            val newAlias =
              Alias(If(f, withoutAlias, Literal(null, withoutAlias.dataType)), ce.name)()
            mergedExpressions(i) = newAlias
            ce.toAttribute -> newAlias.toAttribute
          }
        case _ => None
      }
    })

    newPlanFilter.foreach {
      case (f, true) => mergedExpressions += f
      case _ =>
    }
    cachedPlanFilter.foreach(mergedExpressions += _)

    (mergedExpressions.toSeq, newNPMapping, newCPMapping)
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
