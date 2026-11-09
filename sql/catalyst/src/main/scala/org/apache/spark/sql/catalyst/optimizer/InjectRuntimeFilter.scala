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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractSingleColumnNullAwareAntiJoin}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY, PYTHON_UDF, REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE, SCALA_UDF}
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
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
    // The subquery evaluates the creation side again, apart from the join. A non-deterministic
    // filter or key can give it different keys than the join sees. This check does not see into
    // a leaf that returns other rows when read again, like a cache whose plan is not repeatable.
    if (!aggregate.deterministic) {
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

  // The plans that a broadcast hash join of `plan` could broadcast, as
  // `getBroadcastHashJoinBuildSide` picks them, but no null-aware anti join under AQE. The list can
  // hold more plans than physical planning broadcasts, and no fewer that a DPP filter can reuse,
  // unless the statistics change after this rule. It takes each side without the DPP filters on top
  // of it, since cleanup can still drop them.
  private def broadcastablePlans(plan: LogicalPlan): Seq[LogicalPlan] = plan.flatMap {
    // AQE plans a DPP filter as `true` even if a null-aware anti join broadcasts its build plan.
    // Without AQE, physical planning keeps such a filter, though the filter builds its own
    // broadcast.
    case ExtractSingleColumnNullAwareAntiJoin(_, _) if conf.adaptiveExecutionEnabled => Nil
    case join: Join =>
      getBroadcastHashJoinBuildSide(join, conf).map {
        case BuildLeft => withoutDppFilters(join.left)
        case BuildRight => withoutDppFilters(join.right)
      }
    case _ => Nil
  }

  // `plan` without the DPP filters on top of it that `drop` picks, by default all of them.
  private def withoutDppFilters(
      plan: LogicalPlan,
      drop: DynamicPruningSubquery => Boolean = _ => true): LogicalPlan = plan match {
    case f @ Filter(dpp: DynamicPruningSubquery, child) =>
      val newChild = withoutDppFilters(child, drop)
      if (drop(dpp)) newChild else f.withNewChildren(Seq(newChild))
    case _ => plan
  }

  // Whether evaluating `e` cannot fail, also on rows that the join does not read. It asks
  // `ExprUtils.canEvaluateUnconditionally`, after it replaces, bottom-up, a wrapper that cannot
  // fail either over a child that it takes: the normalization that `NormalizeFloatingNumbers` puts
  // around a floating point join key. The stand-in keeps the type of what it replaces. An
  // `onlyInBroadcast` DPP filter counts too, since it finds no broadcast in the bloom filter's
  // subquery and is `true` there.
  private def cannotFail(e: Expression): Boolean = e match {
    case dpp: DynamicPruningSubquery => dpp.onlyInBroadcast
    case _ => ExprUtils.canEvaluateUnconditionally(e.transformUp {
      case n @ KnownFloatingPointNormalized(NormalizeNaNAndZero(child))
          if ExprUtils.canEvaluateUnconditionally(child) =>
        AttributeReference("normalized", n.dataType, n.nullable)()
    })
  }

  // Whether the bloom filter's subquery can evaluate `creationSide` again, apart from the join. The
  // plan it reads must give the same rows again (SPARK-60808). So it must read no CTE, whose
  // reference hides the determinism of its plan, a cache only if Spark can tell that scanning it
  // again gives the same rows, and a V2 scan only if every operation pushed into it works on each
  // row by itself. A pushed limit or top-N, for example, lets the source pick other rows on a
  // second read. It must not read a file metadata column either, such as
  // `_metadata.file_block_start`, which depends on how the scan splits its files. The subquery can
  // split them differently. Its filters, and the projections that the key and the filters need,
  // must pass `cannotFail`, which takes only expressions that are deterministic in the subquery
  // and do not fail there. A part of the join side outside that plan can only drop rows from the
  // join, so the bloom filter still holds every key that the join sees.
  private def canEvaluateAgain(creationSide: FilterCreationSide): Boolean = {
    val evaluated =
      ColumnPruning(Project(Seq(Alias(creationSide.key, "key")()), creationSide.plan))
    !evaluated.exists {
      case node if node.references.exists(a => FileSourceMetadataAttribute.isValid(a.metadata)) =>
        true
      case Project(projectList, _) => !projectList.forall(e => cannotFail(trimAliases(e)))
      case Filter(condition, _) => !splitConjunctivePredicates(condition).forall(cannotFail)
      case _: CTERelationRef => true
      case scan: DataSourceV2ScanRelation => !scan.onlyRowWisePushdowns
      case leaf: MaterializedLeafNode => !leaf.isOutputRepeatable
      case _: LeafNode => false
      case _ => true
    }
  }

  // The plans whose broadcast a DPP filter may reuse: its build plan, and for an `onlyInBroadcast`
  // filter, the source plan of its broadcast value projection.
  private def reusablePlans(dpp: DynamicPruningSubquery): Seq[LogicalPlan] =
    dpp.buildQuery +: dpp.usableBroadcastValueProjection.filter(_ => dpp.onlyInBroadcast)
      .map(_.sourcePlan).toSeq

  // The DPP filters on `key` on top of `side`, where they sit when this rule runs, just after DPP.
  private def dynamicPruningSubqueries(
      side: LogicalPlan,
      key: Expression): Seq[DynamicPruningSubquery] = side match {
    case Filter(dpp: DynamicPruningSubquery, child) =>
      val rest = dynamicPruningSubqueries(child, key)
      if (dpp.pruningKey.fastEquals(key)) dpp +: rest else rest
    case _ => Nil
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
    // A key with a DPP filter gets no bloom filter, unless physical planning drops that filter. It
    // drops an `onlyInBroadcast` DPP filter when no broadcast join broadcasts a plan that the
    // filter could reuse. So for a key with a DPP filter, this rule:
    // 1. Collects `reused`: the join sides that a broadcast join could broadcast and a DPP filter
    //    could reuse.
    // 2. Counts an `onlyInBroadcast` DPP filter as kept (`isKept`) if one of its plans has the same
    //    result as a plan in `reused`. Any other DPP filter counts as kept. So does every DPP
    //    filter in a subquery. An uncorrelated IN subquery is joined with the rest of the query
    //    later, where `PartitionPruning` puts a new DPP filter on the key, and this rule decides
    //    that one against all the broadcasts. A scalar subquery, which includes an uncorrelated
    //    EXISTS, is planned on its own, so a key there with a DPP filter gets no bloom filter.
    // 3. Adds a bloom filter for a key whose DPP filters are all dropped only if `creationSide`
    //    allows it: the key's type compares by its bytes (`isBinaryStable`), pushdown can take it
    //    down safely (`canPushDown`), and the subquery can evaluate the creation side again
    //    (`canEvaluateAgain`). Pushdown moves the bloom filter along its key, down to the leaves
    //    the key comes from, and into every branch of a union. There it must not land in a plan in
    //    `reused`. It would change the broadcast, and physical planning would drop the DPP filter
    //    that reuses it. A reused broadcast elsewhere in the join side does not matter, such as a
    //    broadcast dimension joined to the same fact table. The key must also be safe at every leaf
    //    (`cannotFail`). On a partition column, the bloom filter becomes a partition filter, which
    //    evaluates the key on every partition before the data filters. So a key that can fail could
    //    fail the query (SPARK-60809).
    // Physical planning reuses a broadcast only with exchange reuse on.
    lazy val reused = if (conf.exchangeReuseEnabled) {
      // After the optimizer ran on an IN subquery on its own, a DPP filter there can be a conjunct
      // of a larger filter.
      val dppPlans = plan.flatMap {
        case Filter(condition, _) =>
          splitConjunctivePredicates(condition).collect { case dpp: DynamicPruningSubquery => dpp }
        case _ => Nil
      }.flatMap(reusablePlans)
      broadcastablePlans(plan).filter(b => dppPlans.exists(_.sameResult(b)))
    } else {
      Nil
    }
    val inSubquery = plan.isInstanceOf[Subquery]
    def isKept(dpp: DynamicPruningSubquery): Boolean =
      !dpp.onlyInBroadcast || inSubquery ||
        reusablePlans(dpp).exists(p => reused.exists(_.sameResult(p)))
    // Pushdown rewrites the key of the bloom filter through the aliases on its way down. Where the
    // key cannot be followed, pushdown can still move a copy of the bloom filter, so the check
    // fails there.
    def canPushDown(key: Expression, side: LogicalPlan): Boolean = {
      val lineages = findExpressionAndTrackAllLineagesDown(key, side, strict = true)
      lineages.nonEmpty && lineages.forall { case (leafKey, leaf) =>
        cannotFail(leafKey) && !reused.exists(_.exists(_.fastEquals(leaf)))
      }
    }
    plan transformUp {
      case join @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, hint) =>
        var newLeft = left
        var newRight = right
        leftKeys.lazyZip(rightKeys).foreach((l, r) => {
          lazy val keyDpps = dynamicPruningSubqueries(left, l) ++ dynamicPruningSubqueries(right, r)
          // Check if:
          // 1. The keys are simple cheap expressions
          // 2. There is no DPP filter on the key that physical planning may keep
          if (filterCounter < numFilterThreshold &&
            isSimpleExpression(l) && isSimpleExpression(r) && !keyDpps.exists(isKept)) {
            val oldLeft = newLeft
            val oldRight = newRight
            // The creation side for a bloom filter on `key` of `side`. It leaves out the key's own
            // DPP filters, which physical planning drops. An expression such as a UDF or a `LIKE`
            // in their build plan would also stop the search for the creation side, since
            // `isSimpleExpression` sees the plan of a subquery too. If the key has DPP filters,
            // they are all dropped, so step 3 above applies. The bloom filter evaluates its
            // creation side again, apart from the join. Its subquery can also read more of that
            // side than the join does, since filters on other keys, DPP filters or bloom filters,
            // can narrow only the join's side. And the bloom filter hashes the bytes of the key, so
            // its type must compare by its bytes, which a collated string does not.
            def creationSide(
                side: LogicalPlan,
                key: Expression,
                other: LogicalPlan,
                otherKey: Expression): Option[FilterCreationSide] =
              extractBeneficialFilterCreatePlan(
                side, withoutDppFilters(other, keyDpps.contains), key, otherKey).filter { c =>
                keyDpps.isEmpty || (UnsafeRowUtils.isBinaryStable(key.dataType) &&
                  canPushDown(key, side) && canEvaluateAgain(c))
              }
            // Check if:
            // 1. The current join type supports prune the left side with runtime filter
            // 2. The current join is a shuffle join or a broadcast join that
            //    has a shuffle below it
            // 3. There is no bloom filter on the left key yet
            val hasShuffle = isProbablyShuffleJoin(left, right, hint)
            if (canPruneLeft(joinType) && (hasShuffle || probablyHasShuffle(left)) &&
              !hasBloomFilter(newLeft, l)) {
              creationSide(left, l, right, r).foreach(c => newLeft = injectFilter(l, newLeft, c))
            }
            // Did we actually inject on the left? If not, try on the right
            // Check if:
            // 1. The current join type supports prune the right side with runtime filter
            // 2. The current join is a shuffle join or a broadcast join that
            //    has a shuffle below it
            // 3. There is no bloom filter on the right key yet
            if (newLeft.fastEquals(oldLeft) && canPruneRight(joinType) &&
              (hasShuffle || probablyHasShuffle(right)) && !hasBloomFilter(newRight, r)) {
              creationSide(right, r, left, l).foreach(c => newRight = injectFilter(r, newRight, c))
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
