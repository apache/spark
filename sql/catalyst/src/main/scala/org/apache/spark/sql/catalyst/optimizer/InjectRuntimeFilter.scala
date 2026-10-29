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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractSingleColumnNullAwareAntiJoin}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY, PYTHON_UDF, REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE, SCALA_UDF, UNION}
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.internal.SQLConf

/**
 * Insert a runtime filter on one side of the join (we call this side the application side) if
 * we can extract a runtime filter from the other side (creation side). A simple case is that
 * the creation side is a table scan with a selective filter.
 * The runtime filter is logically an IN subquery with the join keys. Currently it's always
 * bloom filter but we may add other physical implementations in the future.
 */
object InjectRuntimeFilter extends Rule[LogicalPlan] with PredicateHelper with JoinSelectionHelper {

  private case class FilterCreationSide(
      key: Expression,
      plan: LogicalPlan,
      useMaterializedThreshold: Boolean,
      materializedRowCount: Option[BigInt] = None,
      materializedSizeInBytes: Option[BigInt] = None)

  private def injectFilter(
      filterApplicationSideKey: Expression,
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSide: FilterCreationSide): LogicalPlan = {
    injectBloomFilter(
      filterApplicationSideKey,
      filterApplicationSidePlan,
      filterCreationSide
    )
  }

  private def injectBloomFilter(
      filterApplicationSideKey: Expression,
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSide: FilterCreationSide): LogicalPlan = {
    val filterCreationSideKey = filterCreationSide.key
    val filterCreationSidePlan = filterCreationSide.plan
    val creationSideThreshold = if (filterCreationSide.useMaterializedThreshold) {
      conf.runtimeFilterMaterializedCreationSideThreshold
    } else {
      conf.runtimeFilterCreationSideThreshold
    }
    // Skip if the filter creation side is too big
    if (filterCreationSide.materializedSizeInBytes
        .getOrElse(filterCreationSidePlan.stats.sizeInBytes) > creationSideThreshold) {
      return filterApplicationSidePlan
    }
    val rowCount = filterCreationSide.materializedRowCount
      .orElse(filterCreationSidePlan.stats.rowCount)
    val bloomFilterAgg =
      if (rowCount.isDefined && rowCount.get.longValue > 0L) {
        new BloomFilterAggregate(new XxHash64(Seq(filterCreationSideKey)), rowCount.get.longValue)
      } else {
        new BloomFilterAggregate(new XxHash64(Seq(filterCreationSideKey)))
      }

    val alias = Alias(bloomFilterAgg.toAggregateExpression(), "bloomFilter")()
    val aggregate =
      ConstantFolding(ColumnPruning(Aggregate(Nil, Seq(alias), filterCreationSidePlan)))
    // Runtime filters are introduced after subquery optimization, so Python UDFs in a new
    // creation-side subquery cannot be extracted into a Python evaluation operator.
    if (aggregate.containsPattern(PYTHON_UDF)) {
      return filterApplicationSidePlan
    }
    val bloomFilterSubquery = ScalarSubquery(aggregate, Nil)
    val filter = BloomFilterMightContain(bloomFilterSubquery,
      new XxHash64(Seq(filterApplicationSideKey)))
    Filter(filter, filterApplicationSidePlan)
  }

  /**
   * Extracts either a safely materialized leaf with accurate statistics or a simple selective
   * filter over a scan. Filter conditions and the expressions they reference must remain simple,
   * so the runtime-filter subquery does not introduce expensive computation. The extracted plan
   * must produce a superset of the creation side's join keys.
   */
  private def extractSelectiveFilterOverScan(
      plan: LogicalPlan,
      filterCreationSideKey: Expression,
      allowMaterializedCache: Boolean,
      applicationDistinctCount: => Option[BigInt],
      onMaterializedLeaf: => Unit = ()): Option[FilterCreationSide] = {
    def extract(
        p: LogicalPlan,
        predicateReference: AttributeSet,
        hasHitFilter: Boolean,
        hasHitSelectiveFilter: Boolean,
        currentPlan: LogicalPlan,
        targetKey: Expression): Option[FilterCreationSide] = p match {
      case Project(projectList, child) if hasHitFilter =>
        // We need to make sure all expressions referenced by filter predicates are simple
        // expressions.
        val referencedExprs = projectList.filter(predicateReference.contains)
        if (referencedExprs.forall(isSimpleExpression)) {
          extract(
            child,
            referencedExprs.map(_.references).foldLeft(AttributeSet.empty)(_ ++ _),
            hasHitFilter,
            hasHitSelectiveFilter,
            currentPlan,
            targetKey)
        } else {
          None
        }
      case Project(_, child) =>
        assert(predicateReference.isEmpty && !hasHitSelectiveFilter)
        extract(child, predicateReference, hasHitFilter, hasHitSelectiveFilter, currentPlan,
          targetKey)
      case Filter(condition, child) if isSimpleExpression(condition) =>
        extract(
          child,
          predicateReference ++ condition.references,
          hasHitFilter = true,
          hasHitSelectiveFilter = hasHitSelectiveFilter || isLikelySelective(condition),
          currentPlan,
          targetKey)
      case ExtractEquiJoinKeys(joinType, lkeys, rkeys, _, _, left, right, _) =>
        // Runtime filters use one side of the [[Join]] to build a set of join key values and prune
        // the other side of the [[Join]]. It's also OK to use a superset of the join key values
        // (ignore null values) to do the pruning. We can also extract from the other side if the
        // join keys are transitive, and the other side always produces a superset output of join
        // key values. Any join side always produce a superset output of its corresponding
        // join keys, but for transitive join keys we need to check the join type.
        // We assume other rules have already pushed predicates through join if possible.
        // So the predicate references won't pass on anymore.
        if (left.output.exists(_.semanticEquals(targetKey))) {
          extract(left, AttributeSet.empty, hasHitFilter = false, hasHitSelectiveFilter = false,
            currentPlan = left, targetKey = targetKey).orElse {
            // An example that extract from the right side if the join keys are transitive.
            //     left table: 1, 2, 3
            //     right table, 3, 4
            //     right outer join output: (3, 3), (null, 4)
            //     right key output: 3, 4
            if (canPruneLeft(joinType)) {
              lkeys.zip(rkeys).find(_._1.semanticEquals(targetKey)).map(_._2)
                .flatMap { newTargetKey =>
                  extract(right, AttributeSet.empty,
                    hasHitFilter = false, hasHitSelectiveFilter = false, currentPlan = right,
                    targetKey = newTargetKey)
                }
            } else {
              None
            }
          }
        } else if (right.output.exists(_.semanticEquals(targetKey))) {
          extract(right, AttributeSet.empty, hasHitFilter = false, hasHitSelectiveFilter = false,
            currentPlan = right, targetKey = targetKey).orElse {
            // An example that extract from the left side if the join keys are transitive.
            // left table: 1, 2, 3
            // right table, 3, 4
            // left outer join output: (1, null), (2, null), (3, 3)
            // left key output: 1, 2, 3
            if (canPruneRight(joinType)) {
              rkeys.zip(lkeys).find(_._1.semanticEquals(targetKey)).map(_._2)
                .flatMap { newTargetKey =>
                  extract(left, AttributeSet.empty,
                    hasHitFilter = false, hasHitSelectiveFilter = false, currentPlan = left,
                    targetKey = newTargetKey)
                }
            } else {
              None
            }
          }
        } else {
          None
        }
      case leaf: MaterializedLeafNode =>
        onMaterializedLeaf
        val safeLineage = currentPlan.deterministic &&
          findExpressionAndTrackLineageDown(targetKey, currentPlan).exists {
            case (trackedKey, _) => isSimpleExpression(trackedKey)
          }
        val materializedMetadata = if (allowMaterializedCache && safeLineage &&
            leaf.mayHaveUsableMaterializedStats &&
            (hasHitSelectiveFilter || leaf.hasSelectivePredicate ||
              applicationDistinctCount.isDefined)) {
          leaf.materializedMetadata.filter(_.statsAvailable).flatMap { metadata =>
            val creationSize = if (currentPlan eq leaf) {
              metadata.sizeInBytes
            } else {
              currentPlan.stats.sizeInBytes
            }
            Option.when(creationSize <= conf.runtimeFilterMaterializedCreationSideThreshold) {
              metadata -> creationSize
            }
          }
        } else {
          None
        }
        materializedMetadata match {
          case Some((metadata, creationSize)) =>
            val rowCount = metadata.rowCount
            Option.when(
              rowCount <= conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS) &&
                (hasHitSelectiveFilter || leaf.hasSelectivePredicate ||
                  applicationDistinctCount.exists(_ > rowCount))) {
              FilterCreationSide(
                targetKey,
                currentPlan,
                useMaterializedThreshold = true,
                materializedRowCount = Some(rowCount),
                materializedSizeInBytes = Some(creationSize))
            }
          case None if hasHitSelectiveFilter =>
            Some(FilterCreationSide(
              targetKey,
              currentPlan,
              useMaterializedThreshold = false))
          case _ => None
        }
      case _: LeafNode if hasHitSelectiveFilter =>
        Some(FilterCreationSide(
          targetKey,
          currentPlan,
          useMaterializedThreshold = false))
      case _ => None
    }

    if (!plan.isStreaming) {
      extract(plan, AttributeSet.empty, hasHitFilter = false, hasHitSelectiveFilter = false,
        currentPlan = plan, targetKey = filterCreationSideKey)
    } else {
      None
    }
  }

  private def isSimpleExpression(e: Expression): Boolean = {
    !e.containsAnyPattern(PYTHON_UDF, SCALA_UDF, INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY,
      REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE)
  }

  private def isProbablyShuffleJoin(left: LogicalPlan,
      right: LogicalPlan, hint: JoinHint): Boolean = {
    !hintToBroadcastLeft(hint) && !hintToBroadcastRight(hint) &&
      !canBroadcastBySize(left, conf) && !canBroadcastBySize(right, conf)
  }

  private def probablyHasShuffle(plan: LogicalPlan): Boolean = {
    plan.exists {
      case Join(left, right, _, _, hint) => isProbablyShuffleJoin(left, right, hint)
      case _: Aggregate => true
      case _: Window => true
      case _ => false
    }
  }

  // Returns the max scan byte size in the subtree rooted at `filterApplicationSide`.
  private def maxScanByteSize(filterApplicationSide: LogicalPlan): BigInt = {
    val defaultSizeInBytes = conf.getConf(SQLConf.DEFAULT_SIZE_IN_BYTES)
    filterApplicationSide.collect({
      case leaf: LeafNode => leaf
    }).map(scan => {
      // DEFAULT_SIZE_IN_BYTES means there's no byte size information in stats. Since we avoid
      // creating a Bloom filter when the filter application side is very small, so using 0
      // as the byte size when the actual size is unknown can avoid regression by applying BF
      // on a small table.
      if (scan.stats.sizeInBytes == defaultSizeInBytes) BigInt(0) else scan.stats.sizeInBytes
    }).max
  }

  // Returns true if `filterApplicationSide` satisfies the byte size requirement to apply a
  // Bloom filter; false otherwise.
  private def satisfyByteSizeRequirement(filterApplicationSide: LogicalPlan): Boolean = {
    // In case `filterApplicationSide` is a union of many small tables, disseminating the Bloom
    // filter to each small task might be more costly than scanning them itself. Thus, we use max
    // rather than sum here.
    val maxScanSize = maxScanByteSize(filterApplicationSide)
    maxScanSize >=
      conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD)
  }

  /**
   * Extracts the beneficial filter creation plan with check show below:
   * - The filterApplicationSideKey can be pushed down through joins, aggregates and windows
   *   (ie the expression references originate from a single leaf node)
   * - The filter creation side has a selective predicate, or its exact materialized row count
   *   is smaller than the application side's distinct join-key count
   * - The max filterApplicationSide scan size is greater than a configurable threshold
   */
  private def extractBeneficialFilterCreatePlan(
      filterApplicationSide: LogicalPlan,
      filterCreationSide: LogicalPlan,
      filterApplicationSideKey: Expression,
      filterCreationSideKey: Expression): Option[FilterCreationSide] = {
    if (findExpressionAndTrackLineageDown(
      filterApplicationSideKey, filterApplicationSide).isDefined &&
      satisfyByteSizeRequirement(filterApplicationSide)) {
      val allowMaterializedCache = UnsafeRowUtils.isBinaryStable(filterCreationSideKey.dataType) &&
        UnsafeRowUtils.isBinaryStable(filterApplicationSideKey.dataType)
      def distinctCount(key: Expression, plan: LogicalPlan): Option[BigInt] = key match {
        case attribute: Attribute =>
          plan.stats.attributeStats.get(attribute).flatMap(_.distinctCount)
        case _ => None
      }
      def hasOnlyJoinKeyNullChecksOverScan(
          plan: LogicalPlan,
          targetKey: Expression): Boolean = plan match {
        case project: Project =>
          hasOnlyJoinKeyNullChecksOverScan(
            project.child, replaceAlias(targetKey, getAliasMap(project)))
        case Filter(condition, child) =>
          splitConjunctivePredicates(condition).forall {
            case IsNotNull(expression) => expression.semanticEquals(targetKey)
            case _ => false
          } && hasOnlyJoinKeyNullChecksOverScan(child, targetKey)
        case _: LeafNode => true
        case _ => false
      }
      lazy val currentDistinctCount =
        distinctCount(filterApplicationSideKey, filterApplicationSide)
      lazy val lineageDistinctCount = findExpressionAndTrackLineageDown(
        filterApplicationSideKey, filterApplicationSide).flatMap {
        case (trackedKey, origin) => distinctCount(trackedKey, origin)
      }
      lazy val applicationDistinctCount = {
        if (hasOnlyJoinKeyNullChecksOverScan(
            filterApplicationSide, filterApplicationSideKey)) {
          lineageDistinctCount.orElse(currentDistinctCount)
        } else {
          currentDistinctCount
        }
      }
      if (allowMaterializedCache) {
        var sawMaterializedLeaf = false
        val selectiveCreationSide = extractSelectiveFilterOverScan(
          filterCreationSide,
          filterCreationSideKey,
          allowMaterializedCache = false,
          applicationDistinctCount = None,
          onMaterializedLeaf = { sawMaterializedLeaf = true })
        selectiveCreationSide
          .filter(_.plan.stats.sizeInBytes <= conf.runtimeFilterCreationSideThreshold)
          .orElse {
            if (sawMaterializedLeaf) {
              extractSelectiveFilterOverScan(
                filterCreationSide,
                filterCreationSideKey,
                allowMaterializedCache = true,
                applicationDistinctCount = applicationDistinctCount)
            } else {
              selectiveCreationSide
            }
          }
      } else {
        extractSelectiveFilterOverScan(
          filterCreationSide,
          filterCreationSideKey,
          allowMaterializedCache = false,
          applicationDistinctCount = None)
      }
    } else {
      None
    }
  }

  // The plans that a broadcast hash join of `plan` could broadcast. For an equi-join, it is the
  // side that `getBroadcastBuildSide` picks by a broadcast hint, or else by size, as
  // `JoinSelection` asks it. That ignores the other join hints, so the list can hold more plans
  // than physical planning broadcasts. It holds no fewer, unless the statistics change after this
  // rule. Without AQE, the right side of a null-aware anti join also counts.
  private def broadcastablePlans(plan: LogicalPlan): Seq[LogicalPlan] = plan.flatMap {
    case join @ ExtractEquiJoinKeys(_, _, _, _, _, left, right, _) =>
      getBroadcastBuildSide(join, hintOnly = true, conf)
        .orElse(getBroadcastBuildSide(join, hintOnly = false, conf))
        .map {
          case BuildLeft => withoutDppFilters(left)
          case BuildRight => withoutDppFilters(right)
        }
    // Without AQE, physical planning keeps a DPP filter whose build plan this join broadcasts,
    // though the filter builds its own broadcast. AQE plans such a filter as `true`.
    case join @ ExtractSingleColumnNullAwareAntiJoin(_, _)
        if !conf.adaptiveExecutionEnabled && canPlanAsBroadcastHashJoin(join, conf) =>
      Seq(withoutDppFilters(join.right))
    case _ => Nil
  }

  // `plan` without the DPP filters on top of it. Cleanup can still drop those filters, and then
  // physical planning sees the plan without them.
  @tailrec
  private def withoutDppFilters(plan: LogicalPlan): LogicalPlan = plan match {
    case Filter(_: DynamicPruningSubquery, child) => withoutDppFilters(child)
    case _ => plan
  }

  // The plans whose broadcast a DPP filter may reuse: its build plan, and for an `onlyInBroadcast`
  // filter, the source plan of its broadcast value projection.
  private def reusablePlans(dpp: DynamicPruningSubquery): Seq[LogicalPlan] =
    dpp.buildQuery +: dpp.usableBroadcastValueProjection.filter(_ => dpp.onlyInBroadcast)
      .map(_.sourcePlan).toSeq

  // The DPP filters on the key, which sit right on top of the join sides, as this rule is called
  // just after DPP.
  @tailrec
  private def dynamicPruningSubqueries(
      left: LogicalPlan,
      right: LogicalPlan,
      leftKey: Expression,
      rightKey: Expression,
      found: Seq[DynamicPruningSubquery] = Nil): Seq[DynamicPruningSubquery] = {
    (left, right) match {
      case (Filter(dpp: DynamicPruningSubquery, plan), _) =>
        dynamicPruningSubqueries(plan, right, leftKey, rightKey,
          if (dpp.pruningKey.fastEquals(leftKey)) found :+ dpp else found)
      case (_, Filter(dpp: DynamicPruningSubquery, plan)) =>
        dynamicPruningSubqueries(left, plan, leftKey, rightKey,
          if (dpp.pruningKey.fastEquals(rightKey)) found :+ dpp else found)
      case _ => found
    }
  }

  private def hasBloomFilter(plan: LogicalPlan, key: Expression): Boolean = {
    plan.exists {
      case Filter(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case BloomFilterMightContain(_, XxHash64(Seq(valueExpression), _))
            if valueExpression.fastEquals(key) => true
          case _ => false
        }
      case _ => false
    }
  }

  private def tryInjectRuntimeFilter(plan: LogicalPlan): LogicalPlan = {
    var filterCounter = 0
    val numFilterThreshold = conf.getConf(SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD)
    // The plans that a join could broadcast and a DPP filter could reuse. Physical planning reuses
    // a broadcast only with exchange reuse on. Only a key with a DPP filter needs this list.
    lazy val reused = if (conf.exchangeReuseEnabled) {
      val dppPlans = plan.collect { case Filter(dpp: DynamicPruningSubquery, _) => dpp }
        .flatMap(reusablePlans)
      broadcastablePlans(plan).filter(b => dppPlans.exists(_.sameResult(b)))
    } else {
      Nil
    }
    // An `onlyInBroadcast` DPP filter runs only if physical planning finds a broadcast to reuse.
    // It counts only if a plan in `reused` has the same result as one of its reusable plans.
    def isKept(dpp: DynamicPruningSubquery): Boolean =
      !dpp.onlyInBroadcast || reusablePlans(dpp).exists(p => reused.exists(_.sameResult(p)))
    // Pushdown can move a bloom filter on `key` of `side` down to the leaf that `key` comes from,
    // and into every branch of a union. If it can go into a reused plan, it can change that
    // broadcast, so physical planning could no longer reuse it.
    def goesIntoReused(key: Expression, side: LogicalPlan): Boolean =
      if (side.containsPattern(UNION)) {
        reused.exists(r => side.exists(_.fastEquals(r)) || r.exists(_.fastEquals(side)))
      } else {
        findExpressionAndTrackLineageDown(key, side).exists { case (_, leaf) =>
          reused.exists(_.exists(_.fastEquals(leaf)))
        }
      }
    plan transformUp {
      case join @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, hint) =>
        var newLeft = left
        var newRight = right
        leftKeys.lazyZip(rightKeys).foreach((l, r) => {
          lazy val keyDpps = dynamicPruningSubqueries(left, right, l, r)
          // Check if:
          // 1. The keys are simple cheap expressions
          // 2. There is already a DPP filter on the key that physical planning may keep
          if (filterCounter < numFilterThreshold &&
            isSimpleExpression(l) && isSimpleExpression(r) && !keyDpps.exists(isKept)) {
            val oldLeft = newLeft
            val oldRight = newRight
            // The key has a DPP filter, but physical planning drops it. The bloom filter must not
            // take a broadcast away from another DPP filter.
            val hasDroppedDpp = keyDpps.nonEmpty
            // Check if:
            // 1. The current join type supports prune the left side with runtime filter
            // 2. The current join is a shuffle join or a broadcast join that
            //    has a shuffle below it
            // 3. There is no bloom filter on the left key yet
            // 4. Without a dropped DPP filter, or the bloom filter would not go into a broadcast
            //    that a DPP filter may reuse
            val hasShuffle = isProbablyShuffleJoin(left, right, hint)
            if (canPruneLeft(joinType) && (hasShuffle || probablyHasShuffle(left)) &&
              !hasBloomFilter(newLeft, l) && !(hasDroppedDpp && goesIntoReused(l, left))) {
              extractBeneficialFilterCreatePlan(left, right, l, r).foreach { creationSide =>
                newLeft = injectFilter(l, newLeft, creationSide)
              }
            }
            // Did we actually inject on the left? If not, try on the right
            // Check if:
            // 1. The current join type supports prune the right side with runtime filter
            // 2. The current join is a shuffle join or a broadcast join that
            //    has a shuffle below it
            // 3. There is no bloom filter on the right key yet
            // 4. Without a dropped DPP filter, or the bloom filter would not go into a broadcast
            //    that a DPP filter may reuse
            if (newLeft.fastEquals(oldLeft) && canPruneRight(joinType) &&
              (hasShuffle || probablyHasShuffle(right)) && !hasBloomFilter(newRight, r) &&
              !(hasDroppedDpp && goesIntoReused(r, right))) {
              extractBeneficialFilterCreatePlan(right, left, r, l).foreach { creationSide =>
                newRight = injectFilter(r, newRight, creationSide)
              }
            }
            if (!newLeft.fastEquals(oldLeft) || !newRight.fastEquals(oldRight)) {
              filterCounter = filterCounter + 1
            }
          }
        })
        join.withNewChildren(Seq(newLeft, newRight))
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case s: Subquery if s.correlated => plan
    case _ if !conf.runtimeFilterBloomFilterEnabled => plan
    case _ => tryInjectRuntimeFilter(plan)
  }

}
