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

import java.util.Locale

import scala.annotation.tailrec
import scala.util.{Left, Right}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY, PYTHON_UDF, REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE, SCALA_UDF}
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.internal.SQLConf

/**
 * Insert a runtime filter on one side of the join (we call this side the application side) if
 * we can extract a runtime filter from the other side (creation side). A simple case is that
 * the creation side is a table scan with a selective filter.
 * The runtime filter is logically an IN subquery with the join keys. Currently it's always
 * bloom filter but we may add other physical implementations in the future.
 *
 * A [[RuntimeFilterHint]] on a join side ("RUNTIME_FILTER(dim)") requests that side be used as
 * the creation side, whatever its shape, and waives the checks that only estimate whether a filter
 * pays off: the user has asserted the benefit they try to predict. It waives no correctness
 * requirement (see `JoinSelectionHelper.isRepeatableRuntimeFilterSource`), and it does not lift
 * the limits on the number of filters and on a filter's size. A hinted side is never itself
 * filtered. When the hint is not applied, the reason is reported through the hint error handler.
 */
object InjectRuntimeFilter extends Rule[LogicalPlan] with PredicateHelper with JoinSelectionHelper {

  private def hintErrorHandler = conf.hintErrorHandler

  private case class FilterCreationSide(
      key: Expression,
      plan: LogicalPlan,
      useMaterializedThreshold: Boolean,
      materializedRowCount: Option[BigInt] = None,
      materializedSizeInBytes: Option[BigInt] = None,
      hinted: Boolean = false)

  /**
   * Returns the application side with a runtime filter on its key, or the reason none was built.
   */
  private def injectFilter(
      filterApplicationSideKey: Expression,
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSide: FilterCreationSide): Either[String, LogicalPlan] = {
    injectBloomFilter(
      filterApplicationSideKey,
      filterApplicationSidePlan,
      filterCreationSide
    )
  }

  private def injectBloomFilter(
      filterApplicationSideKey: Expression,
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSide: FilterCreationSide): Either[String, LogicalPlan] = {
    val filterCreationSideKey = filterCreationSide.key
    val filterCreationSidePlan = filterCreationSide.plan
    val creationSideThresholdConf = if (filterCreationSide.useMaterializedThreshold) {
      SQLConf.RUNTIME_BLOOM_FILTER_MATERIALIZED_CREATION_SIDE_THRESHOLD
    } else {
      SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD
    }
    val creationSideThreshold = conf.getConf(creationSideThresholdConf)
    val creationSideSize = filterCreationSide.materializedSizeInBytes
      .getOrElse(filterCreationSidePlan.stats.sizeInBytes)
    // Skip if the filter creation side is too big. This estimates whether the filter is worth its
    // cost, which a hint asserts, and the filter's size is bounded by the max number of bits
    // regardless, so a hinted creation side is not subject to it.
    if (!filterCreationSide.hinted && creationSideSize > creationSideThreshold) {
      return Left(s"the creation side ($creationSideSize bytes) exceeds " +
        s"${creationSideThresholdConf.key} ($creationSideThreshold bytes)")
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
      return Left("the creation side contains a Python UDF")
    }
    val bloomFilterSubquery = ScalarSubquery(aggregate, Nil)
    val filter = BloomFilterMightContain(bloomFilterSubquery,
      new XxHash64(Seq(filterApplicationSideKey)))
    Right(Filter(filter, filterApplicationSidePlan))
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
   *
   * All three predict whether a filter pays off, so a [[RuntimeFilterHint]] on the creation side
   * (`hinted`) waives them: the creation side is used as it is, whatever its shape, as long as it
   * is a repeatable source, since the filter subquery re-executes it.
   */
  private def extractBeneficialFilterCreatePlan(
      filterApplicationSide: LogicalPlan,
      filterCreationSide: LogicalPlan,
      filterApplicationSideKey: Expression,
      filterCreationSideKey: Expression,
      hinted: Boolean): Option[FilterCreationSide] = {
    if (hinted) {
      Option.when(isRepeatableRuntimeFilterSource(filterCreationSide)) {
        FilterCreationSide(
          filterCreationSideKey,
          filterCreationSide,
          useMaterializedThreshold = false,
          hinted = true)
      }
    } else if (findExpressionAndTrackLineageDown(
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

  // This checks if there is already a DPP filter, as this rule is called just after DPP.
  @tailrec
  private def hasDynamicPruningSubquery(
      left: LogicalPlan,
      right: LogicalPlan,
      leftKey: Expression,
      rightKey: Expression): Boolean = {
    (left, right) match {
      case (Filter(DynamicPruningSubquery(pruningKey, _, _, _, _, _, _), plan), _) =>
        pruningKey.fastEquals(leftKey) || hasDynamicPruningSubquery(plan, right, leftKey, rightKey)
      case (_, Filter(DynamicPruningSubquery(pruningKey, _, _, _, _, _, _), plan)) =>
        pruningKey.fastEquals(rightKey) ||
          hasDynamicPruningSubquery(left, plan, leftKey, rightKey)
      case _ => false
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
    val bloomFilterEnabled = conf.runtimeFilterBloomFilterEnabled
    plan transformUp {
      case join @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, hint) =>
        var newLeft = left
        var newRight = right
        // A side hinted as the runtime filter source is the creation side, so the filter is
        // applied to the other side. An ambiguous hint is reported and otherwise ignored, leaving
        // the heuristics to decide.
        val hintedSource = runtimeFilterSourceSide(hint)
        val hinted = hintedSource.isDefined
        val injectLeftHinted = hintedSource.contains(BuildRight)
        val injectRightHinted = hintedSource.contains(BuildLeft)
        if (isRuntimeFilterHintAmbiguous(hint)) {
          hintErrorHandler.joinHintNotSupported(HintInfo(runtimeFilterSource = true),
            "the runtime filter source is ambiguous as both join sides are hinted")
        }
        var appliedHint = false
        // The first reason the hint could not be applied on a key. The hinted side is the same
        // for every key, so the first reason is as representative as any.
        var notAppliedReason: Option[String] = None
        def hintBlocked(reason: => String): Unit = {
          if (notAppliedReason.isEmpty) notAppliedReason = Some(reason)
        }
        lazy val hasShuffle = isProbablyShuffleJoin(left, right, hint)
        // Tries to filter `applicationSide` with a filter built from `creationSide`. Returns the
        // filtered side, recording the reason when this direction is the hinted one and no filter
        // was added. Requirements:
        // 1. The join type supports pruning the application side
        // 2. The application side is not the hinted source, which is never itself filtered
        // 3. The join is a shuffle join, or a broadcast join with a shuffle below it -- an
        //    estimate of whether the filter pays off, so a hint waives it
        // 4. There is no Bloom filter on the application side's key yet
        def tryInject(
            applicationSide: LogicalPlan,
            currentApplicationSide: LogicalPlan,
            applicationSideKey: Expression,
            creationSide: LogicalPlan,
            creationSideKey: Expression,
            canPrune: Boolean,
            applicationHinted: Boolean,
            creationHinted: Boolean,
            sideName: String): Option[LogicalPlan] = {
          def blocked(reason: => String): Option[LogicalPlan] = {
            if (applicationHinted) hintBlocked(reason)
            None
          }
          if (!canPrune) {
            blocked(s"the $sideName side of a " +
              s"${joinType.sql.toLowerCase(Locale.ROOT)} join cannot be pruned")
          } else if (creationHinted ||
            !(applicationHinted || hasShuffle || probablyHasShuffle(applicationSide))) {
            None
          } else if (hasBloomFilter(currentApplicationSide, applicationSideKey)) {
            blocked("a runtime filter on the join key already exists")
          } else {
            extractBeneficialFilterCreatePlan(applicationSide, creationSide,
              applicationSideKey, creationSideKey, applicationHinted) match {
              case Some(filterCreationSide) =>
                injectFilter(applicationSideKey, currentApplicationSide, filterCreationSide)
                  .fold(reason => blocked(reason), Some(_))
              case None =>
                blocked("the hinted side may produce different rows when evaluated again")
            }
          }
        }
        leftKeys.lazyZip(rightKeys).foreach((l, r) => {
          // A DPP filter on the key already prunes the application side, by whole partitions
          // rather than by rows, so no Bloom filter is added. That also honors the hint, if any,
          // provided the DPP predicate survives: `CleanupDynamicPruningFilters` drops it when
          // `PushDownPredicates` cannot carry it to the scan, which a non-deterministic operator
          // on the pruned side prevents. A Bloom filter needs no pushdown, so one is still added
          // for the hint in that case.
          val prunedByDpp = hasDynamicPruningSubquery(left, right, l, r) &&
            (!hinted || (if (injectLeftHinted) left else right).deterministic)
          if (prunedByDpp) {
            appliedHint = appliedHint || hinted
          } else if (!bloomFilterEnabled) {
            hintBlocked(s"${SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key} is false")
          } else if (filterCounter >= numFilterThreshold) {
            hintBlocked(
              s"${SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD.key} ($numFilterThreshold) is reached")
          } else if (!isSimpleExpression(l) || !isSimpleExpression(r)) {
            // The keys become the filter's input and must be cheap to evaluate.
            hintBlocked("the join key is not a simple expression")
          } else {
            val oldLeft = newLeft
            val oldRight = newRight
            tryInject(left, newLeft, l, right, r, canPruneLeft(joinType),
              injectLeftHinted, injectRightHinted, "left").foreach(newLeft = _)
            // Did we actually inject on the left? If not, try on the right
            if (newLeft.fastEquals(oldLeft)) {
              tryInject(right, newRight, r, left, l, canPruneRight(joinType),
                injectRightHinted, injectLeftHinted, "right").foreach(newRight = _)
            }
            if (!newLeft.fastEquals(oldLeft) || !newRight.fastEquals(oldRight)) {
              filterCounter = filterCounter + 1
              appliedHint = appliedHint || hinted
            }
          }
        })
        if (hinted && !appliedHint) {
          hintErrorHandler.joinHintNotSupported(HintInfo(runtimeFilterSource = true),
            notAppliedReason.getOrElse("no runtime filter could be built from the hinted side"))
        }
        join.withNewChildren(Seq(newLeft, newRight))
      case join @ Join(_, _, _, _, hint)
          if hintToRuntimeFilterSourceLeft(hint) || hintToRuntimeFilterSourceRight(hint) =>
        // A runtime filter is built from the join keys, so a join without equi-join keys has
        // nothing to build from.
        val reason = if (isRuntimeFilterHintAmbiguous(hint)) {
          "the runtime filter source is ambiguous as both join sides are hinted"
        } else {
          "no equi-join keys"
        }
        hintErrorHandler.joinHintNotSupported(HintInfo(runtimeFilterSource = true), reason)
        join
    }
  }

  private def hasRuntimeFilterHint(plan: LogicalPlan): Boolean = plan.exists {
    case Join(_, _, _, _, hint) =>
      hintToRuntimeFilterSourceLeft(hint) || hintToRuntimeFilterSourceRight(hint)
    case _ => false
  }

  // With Bloom filters disabled the rule still runs over a plan with a runtime filter hint, so the
  // hint is credited to a DPP filter or reported as not applied.
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case s: Subquery if s.correlated => plan
    case _ if !conf.runtimeFilterBloomFilterEnabled && !hasRuntimeFilterHint(plan) => plan
    case _ => tryInjectRuntimeFilter(plan)
  }

}
