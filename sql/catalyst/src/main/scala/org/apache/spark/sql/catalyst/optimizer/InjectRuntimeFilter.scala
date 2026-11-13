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

import scala.collection.mutable
import scala.util.{Left, Right}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractSingleColumnNullAwareAntiJoin}
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.{BLOOM_FILTER_MIGHT_CONTAIN, DYNAMIC_PRUNING_SUBQUERY, INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY, PYTHON_UDF, REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE, SCALA_UDF}
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
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
 * pays off, since the user asserts it does. It waives no correctness requirement (see
 * [[RuntimeFilterHintCheck]]) and does not lift the limits on the number of filters and on a
 * Bloom filter's size, nor the requirement that the join keys be simple expressions. A join key
 * that can fail where the filter evaluates it gets no hinted filter (`hintedKeyCannotFail`). A
 * hinted side is never itself filtered. On a join key that a DPP filter already prunes, no Bloom
 * filter is added; the other join keys still get one.
 * The rule runs after the DPP filters are pushed down and cleaned up, so it sees only the ones
 * that survive. When the hint is not applied, the reason is reported through the hint error
 * handler. A hinted side that cannot be a runtime filter source on any join key leaves the join
 * as without the hint; a hint not applied for another reason builds no filter in the opposite
 * direction instead.
 */
object InjectRuntimeFilter extends Rule[LogicalPlan] with PredicateHelper with JoinSelectionHelper {

  private def hintErrorHandler = conf.hintErrorHandler

  // Marks the Bloom filter of a hinted join with the hinted side and the key that it is built from.
  private val HINTED_CREATION_SIDE =
    TreeNodeTag[FilterCreationSide]("hintedRuntimeFilterCreationSide")

  private val CannotFollow = "a DPP filter of a join outside the hinted side prunes it, which " +
    "the subquery of a hinted filter cannot follow"

  private val NoHintedFilter = "no runtime filter could be built from the hinted side"

  private def withHint(join: Join)(f: HintInfo => HintInfo): Join = {
    def updated(hintInfo: Option[HintInfo]): Option[HintInfo] =
      hintInfo.map(f).filter(_ != HintInfo())
    val copy = join.copy(hint = JoinHint(updated(join.hint.leftHint), updated(join.hint.rightHint)))
    copy.copyTagsFrom(join)
    copy
  }

  // The optimizer runs this rule on a subquery before hoisting it into the main plan, where the
  // join is visited again, possibly rebuilt by another rule in between. The hint of a join whose
  // hinted side builds its hinted filters, or for a reason that is reported builds none, records
  // that it is decided: later passes add no runtime filter for the join.
  private def markRuntimeFilterHintDecided(join: Join): Join = withHint(join) { h =>
    if (h.runtimeFilterSource) h.copy(runtimeFilterDecided = true) else h
  }

  // A hint that is reported and ignored leaves the join as without it, also for a later pass.
  private def withoutRuntimeFilterHint(join: Join): Join =
    withHint(join)(_.copy(runtimeFilterSource = false, runtimeFilterDecided = false))

  private case class FilterCreationSide(
      key: Expression,
      plan: LogicalPlan,
      useMaterializedThreshold: Boolean,
      materializedRowCount: Option[BigInt] = None,
      materializedSizeInBytes: Option[BigInt] = None,
      hinted: Boolean = false,
      // Whether the bloom filter's subquery reads only the columns of `plan` that it needs.
      pruned: Boolean = true)

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
    bloomFilter(new XxHash64(Seq(filterApplicationSideKey)), filterCreationSide)
      .map(Filter(_, filterApplicationSidePlan)).getOrElse(filterApplicationSidePlan)
  }

  // The Bloom filter on the hash `value` that is built from `filterCreationSide`, or why its
  // subquery cannot evaluate that side.
  private def bloomFilter(
      value: Expression,
      filterCreationSide: FilterCreationSide): Either[String, BloomFilterMightContain] = {
    val aggregate = bloomFilterAggregate(filterCreationSide)
    // Runtime filters are introduced after subquery optimization, so the functions in a new
    // creation-side subquery that need an operator of their own cannot be extracted into one.
    if (containsExtractedFunction(aggregate)) {
      return Left(RuntimeFilterSourceAnalysis.ExtractedFunction)
    }
    // The subquery evaluates the creation side again, apart from the join. A non-deterministic
    // filter or key can give it different keys than the join sees. This check does not see into
    // a leaf that returns other rows when read again, like a cache whose plan is not repeatable.
    if (!aggregate.deterministic) {
      return Left("the runtime filter's subquery over the hinted side is not deterministic")
    }
    val filter = BloomFilterMightContain(ScalarSubquery(aggregate, Nil), value)
    if (filterCreationSide.hinted) filter.setTagValue(HINTED_CREATION_SIDE, filterCreationSide)
    Right(filter)
  }

  // The aggregate that the Bloom filter's subquery evaluates over the creation side.
  private def bloomFilterAggregate(filterCreationSide: FilterCreationSide): LogicalPlan = {
    val filterCreationSideKey = filterCreationSide.key
    val filterCreationSidePlan = filterCreationSide.plan
    val rowCount = filterCreationSide.materializedRowCount
      .orElse(filterCreationSidePlan.stats.rowCount)
    val bloomFilterAgg =
      if (rowCount.isDefined && rowCount.get.longValue > 0L) {
        new BloomFilterAggregate(new XxHash64(Seq(filterCreationSideKey)), rowCount.get.longValue)
      } else {
        new BloomFilterAggregate(new XxHash64(Seq(filterCreationSideKey)))
      }

    val alias = Alias(bloomFilterAgg.toAggregateExpression(), "bloomFilter")()
    val aggregate = Aggregate(Nil, Seq(alias), filterCreationSidePlan)
    ConstantFolding(if (!filterCreationSide.pruned) {
      aggregate
    } else if (filterCreationSide.hinted) {
      pruneColumns(aggregate)
    } else {
      ColumnPruning(aggregate)
    })
  }

  // Prunes the columns of the new subquery to a fixed point. A hinted creation side can have any
  // shape, e.g. an aggregate or a window whose extra columns take more than one pass to remove.
  private def pruneColumns(plan: LogicalPlan): LogicalPlan = {
    def prune(p: LogicalPlan) =
      withoutUnreadPythonUDFs(withoutUnreadOuterGenerates(ColumnPruning(p)))
    var current = plan
    var pruned = prune(current)
    var iteration = 1
    while (!pruned.fastEquals(current) && iteration < conf.optimizerMaxIterations) {
      current = pruned
      pruned = prune(current)
      iteration += 1
    }
    pruned
  }

  // An outer generate keeps every row of its child, repeated as many times as its generator yields
  // values. Column pruning keeps it even when nothing reads its output, so a non-deterministic
  // generator would leave the subquery non-deterministic. [[RuntimeFilterSourceAnalysis]] rejects a
  // hinted side that filters or keys on a count of the rows such a generate repeats, so one whose
  // output nothing else in the subquery reads is dropped. A deterministic generator stays, since an
  // aggregate above it can count the rows it repeats.
  private def withoutUnreadOuterGenerates(plan: LogicalPlan): LogicalPlan = {
    def isRead(g: Generate): Boolean = {
      val generated = AttributeSet(g.generatorOutput)
      plan.exists {
        case node if node eq g => false
        // A union outputs its children's columns by position, without referencing them.
        case u: Union => u.children.exists(_.outputSet.intersect(generated).nonEmpty)
        case node => node.references.intersect(generated).nonEmpty
      }
    }
    plan transformDown {
      case g: Generate if g.outer && !g.generator.deterministic && !isRead(g) => g.child
    }
  }

  // The optimizer moves the Python UDFs of a plan into an operator of their own once this rule
  // has run, so a later pass finds that operator in a hinted side that an earlier pass decided.
  // It evaluates its UDFs for each row of its child, which column pruning leaves in place, so one
  // whose results nothing else in the subquery reads is dropped.
  private def withoutUnreadPythonUDFs(plan: LogicalPlan): LogicalPlan = {
    def isRead(e: BaseEvalPython): Boolean = {
      val results = AttributeSet(e.resultAttrs)
      plan.exists {
        case node if node eq e => false
        case u: Union => u.children.exists(_.outputSet.intersect(results).nonEmpty)
        case node => node.references.intersect(results).nonEmpty
      }
    }
    plan transformDown {
      case e: BaseEvalPython if !isRead(e) => e.child
    }
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
      // A DPP filter here runs as its own subquery, in the join and in the bloom filter's
      // subquery alike. It is left out of the check, which would see its build plan too.
      case Filter(condition, child) if splitConjunctivePredicates(condition)
          .filterNot(_.isInstanceOf[DynamicPruningSubquery]).forall(isSimpleExpression) =>
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
   * - Both keys are binary stable. The Bloom filter hashes the bytes of the key, so it could drop
   *   a row whose key is equal under a non-binary collation but has different bytes
   * - The filterApplicationSideKey can be pushed down through joins, aggregates and windows
   *   (ie the expression references originate from a single leaf node)
   * - The filter creation side has a selective predicate, or its exact materialized row count
   *   is smaller than the application side's distinct join-key count
   * - The max filterApplicationSide scan size is greater than a configurable threshold
   *
   * A [[RuntimeFilterHint]] bypasses this method, which only estimates whether a filter pays off:
   * `tryInject` builds the hinted filter directly once a [[RuntimeFilterHintCheck]] accepts the
   * creation side.
   */
  private def extractBeneficialFilterCreatePlan(
      filterApplicationSide: LogicalPlan,
      filterCreationSide: LogicalPlan,
      filterApplicationSideKey: Expression,
      filterCreationSideKey: Expression): Option[FilterCreationSide] = {
    if (UnsafeRowUtils.isBinaryStable(filterCreationSideKey.dataType) &&
      UnsafeRowUtils.isBinaryStable(filterApplicationSideKey.dataType) &&
      findExpressionAndTrackLineageDown(
      filterApplicationSideKey, filterApplicationSide).isDefined &&
      satisfyByteSizeRequirement(filterApplicationSide)) {
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
      None
    }
  }

  // The plans that a broadcast hash join of `plan` could broadcast, as
  // `getBroadcastHashJoinBuildSide` picks them, but no null-aware anti join under AQE. The list can
  // hold more plans than physical planning broadcasts, and no fewer that a DPP filter can reuse,
  // unless the statistics change after this rule. It takes each side without its DPP filters, since
  // physical planning can still drop them.
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

  // `plan` without the DPP filters that `drop` picks, by default all of them. This rule runs after
  // the DPP filters are pushed down, so they can be conjuncts of any filter in `plan`. A DPP
  // filter's build plan keeps them as `PartitionPruning` built them, a filter per key on top of
  // another, so the transform goes bottom-up to strip the whole stack.
  private def withoutDppFilters(
      plan: LogicalPlan,
      drop: DynamicPruningSubquery => Boolean = _ => true): LogicalPlan =
    plan.transformUpWithPruning(_.containsPattern(DYNAMIC_PRUNING_SUBQUERY)) {
      case f @ Filter(condition, child) =>
        val conjuncts = splitConjunctivePredicates(condition)
        val kept = conjuncts.filterNot {
          case dpp: DynamicPruningSubquery => drop(dpp)
          case _ => false
        }
        if (kept.length == conjuncts.length) f
        else kept.reduceOption(And).map(Filter(_, child)).getOrElse(child)
    }

  // Whether evaluating `e` cannot fail, also on rows that the join does not read. It asks
  // `ExprUtils.canEvaluateUnconditionally`, after it replaces, bottom-up, a wrapper that cannot
  // fail either over a child that it takes: the normalization that `NormalizeFloatingNumbers` puts
  // around a floating point join key, and the collation key that `RewriteCollationJoin` puts
  // around a collated string one. The stand-in keeps the type of what it replaces. An
  // `onlyInBroadcast` DPP filter counts too, since it finds no broadcast in the bloom filter's
  // subquery and is `true` there.
  private def cannotFail(e: Expression): Boolean = e match {
    case dpp: DynamicPruningSubquery => dpp.onlyInBroadcast
    case _ => ExprUtils.canEvaluateUnconditionally(e.transformUp {
      case n @ KnownFloatingPointNormalized(NormalizeNaNAndZero(child))
          if ExprUtils.canEvaluateUnconditionally(child) =>
        AttributeReference("normalized", n.dataType, n.nullable)()
      case c @ CollationKey(child) if ExprUtils.canEvaluateUnconditionally(child) =>
        AttributeReference("collationKey", c.dataType, c.nullable)()
    })
  }

  // The keys that a filter on `key` on top of `side` can have where pushdown takes it, rewritten
  // through the aliases and for the branches of a union on its way.
  private def pushedDown(key: Expression, side: LogicalPlan): Seq[Expression] =
    key +: (side match {
      case p: Project => pushedDown(replaceAlias(key, getAliasMap(p)), p.child)
      case a: Aggregate => pushedDown(replaceAlias(key, getAliasMap(a)), a.child)
      case u: Union => u.children.flatMap { child =>
        val branchOutput = AttributeMap(u.output.zip(child.output))
        pushedDown(key.transform { case a: Attribute => branchOutput.getOrElse(a, a) }, child)
      }
      case _ => side.children.filter(child => key.references.subsetOf(child.outputSet))
        .flatMap(pushedDown(key, _))
    })

  /**
   * Whether a hinted runtime filter on `key` cannot fail wherever pushdown takes it in `side`.
   * Down there, the filter evaluates its key also on rows that the join does not read: the rows
   * that an operator above it drops, and on a partition column, where it becomes a partition
   * filter, every partition before the data filters (SPARK-60809). The hint waives the estimates
   * that keep a runtime filter off most keys, so a key that can fail could fail a query that
   * succeeds without the hint. Unlike `canPushDown`, this takes a key that cannot be followed down
   * to a leaf.
   */
  private[sql] def hintedKeyCannotFail(key: Expression, side: LogicalPlan): Boolean =
    pushedDown(key, side).forall(cannotFail)

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
  // filter, the source plan of its broadcast value projection. Each is taken without its DPP
  // filters, as the join sides it is compared with are.
  private def reusablePlans(dpp: DynamicPruningSubquery): Seq[LogicalPlan] =
    (dpp.buildQuery +: dpp.usableBroadcastValueProjection.filter(_ => dpp.onlyInBroadcast)
      .map(_.sourcePlan).toSeq).map(withoutDppFilters(_))

  /**
   * The DPP filters on `applicationKey` built from `creationKey` that prune `plan`. This rule runs
   * after the DPP filters are pushed down and cleaned up, so only a surviving one is found,
   * wherever the pushdown left it; it is the one [[PartitionPruning]] inserted for the same join
   * key pair, whose pruning key it rewrote on the way down as the lineage tracking does. Pushdown
   * takes it into every branch of a union, and cleanup can drop it from some branches only, so all
   * the lineages of the key are followed.
   */
  private def dynamicPruningFilters(
      plan: LogicalPlan,
      applicationKey: Expression,
      creationKey: Expression): Seq[DynamicPruningSubquery] = {
    if (!plan.containsPattern(DYNAMIC_PRUNING_SUBQUERY)) return Nil
    val trackedKeys = findExpressionAndTrackAllLineagesDown(applicationKey, plan).map(_._1)
    if (trackedKeys.isEmpty) return Nil
    plan.flatMap {
      case Filter(condition, _) => condition.collect {
        case d: DynamicPruningSubquery if trackedKeys.exists(_.semanticEquals(d.pruningKey)) &&
          d.broadcastKeyIndices.exists(i => d.buildKeys(i).semanticEquals(creationKey)) => d
      }
      case _ => Nil
    }
  }

  /** Whether `plan` has a Bloom filter on `key`. */
  private def hasBloomFilter(plan: LogicalPlan, key: Expression): Boolean = {
    plan.exists {
      case Filter(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case BloomFilterMightContain(_, XxHash64(Seq(valueExpression), _)) =>
            valueExpression.fastEquals(key)
          case _ => false
        }
      case _ => false
    }
  }

  private def tryInjectRuntimeFilter(plan: LogicalPlan): LogicalPlan = {
    var filterCounter = 0
    val numFilterThreshold = conf.getConf(SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD)
    // The plans whose broadcast each DPP filter may reuse, built once per DPP filter.
    val reusablePlansByDpp = mutable.HashMap.empty[ExprId, Seq[LogicalPlan]]
    def reusablePlansOf(dpp: DynamicPruningSubquery): Seq[LogicalPlan] =
      reusablePlansByDpp.getOrElseUpdate(dpp.exprId, reusablePlans(dpp))
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
      }.flatMap(reusablePlansOf)
      broadcastablePlans(plan).filter(b => dppPlans.exists(_.sameResult(b)))
    } else {
      Nil
    }
    val inSubquery = plan.isInstanceOf[Subquery]
    // Whether `dpp` prunes by what this pass sees: it runs as its own subquery, or reuses one of
    // the broadcasts in `reused`.
    def prunes(dpp: DynamicPruningSubquery): Boolean =
      !dpp.onlyInBroadcast || reusablePlansOf(dpp).exists(p => reused.exists(_.sameResult(p)))
    def isKept(dpp: DynamicPruningSubquery): Boolean = inSubquery || prunes(dpp)
    // Pushdown rewrites the key of the bloom filter through the aliases on its way down. Where the
    // key cannot be followed, pushdown can still move a copy of the bloom filter, so the check
    // fails there.
    def canPushDown(key: Expression, side: LogicalPlan): Boolean = {
      val lineages = findExpressionAndTrackAllLineagesDown(key, side, strict = true)
      lineages.nonEmpty && lineages.forall { case (leafKey, leaf) =>
        cannotFail(leafKey) && !reused.exists(_.exists(_.fastEquals(leaf)))
      }
    }
    val bloomFilterEnabled = conf.runtimeFilterBloomFilterEnabled
    lazy val cteDefs = cteDefinitions(plan)
    // The subquery of a hinted filter evaluates a copy of the hinted side, apart from the join.
    // The join's own side still changes after the copy is made: pushdown moves the filters of the
    // joins within it and brings in the filters of the joins above it, also in a later pass, over
    // the query that a subquery is joined with. The copy would then read rows that the join does
    // not read, and an expression of the hinted side that can fail could fail a query that
    // succeeds without the hint (SPARK-60842). So the filters are pushed down here, and each
    // hinted filter whose hinted side has changed is built again from it, until none changes. A
    // hinted filter follows its join's side unless:
    // 1. The changed side is no source of the filter as the hinted side was, e.g. another join
    //    filters it on a value that another evaluation can assign differently. The join loses its
    //    hinted filters and its hint, as a join whose hinted side is declined to begin with.
    // 2. A DPP filter that prunes the hinted side is built from a plan outside it, or reuses a
    //    broadcast from outside it, which the subquery does not have (`missesPruning`). The join
    //    loses its hinted filters.
    // A hint is reported once its join has no hinted filter left.
    def withSettledHintedFilters(injected: LogicalPlan): LogicalPlan = {
      def hintedAndPrunedSide(join: Join): Option[(LogicalPlan, LogicalPlan)] =
        runtimeFilterSourceSide(join.hint).filter(_ => isRuntimeFilterHintDecided(join.hint)).map {
          case BuildLeft => (join.left, join.right)
          case BuildRight => (join.right, join.left)
        }
      val hasHintedJoin = injected.exists {
        case join: Join => hintedAndPrunedSide(join).isDefined
        case _ => false
      }
      if (!hasHintedJoin) return injected
      // Whether a subquery that evaluates `copy` misses the pruning of a DPP filter in it: one
      // that reuses a broadcast that `copy` does not build, or that is built from a plan outside
      // `copy`. A DPP filter in a subquery counts only if it prunes by what this pass sees, unlike
      // for `isKept`: the pass over the outer query settles the hinted filters again, against all
      // the broadcasts.
      def missesPruning(copy: LogicalPlan): Boolean = {
        lazy val builtWithin = AttributeSet(copy.flatMap(_.output))
        lazy val broadcastWithin = broadcastablePlans(copy)
        copy.exists {
          case Filter(condition, _) => splitConjunctivePredicates(condition).exists {
            case dpp: DynamicPruningSubquery if prunes(dpp) =>
              if (dpp.onlyInBroadcast) {
                !reusablePlansOf(dpp).exists(p => broadcastWithin.exists(_.sameResult(p)))
              } else {
                !dpp.buildKeys.forall(_.references.subsetOf(builtWithin))
              }
            case _ => false
          }
          case _ => false
        }
      }
      // Settles the hinted filters of `join` in `prunedSide` against `hintedSide`. Returns the new
      // pruned side, why the hint is not applied if the join has no hinted filter left, and
      // whether that is because the changed hinted side is no source of them.
      def settleFilters(
          join: Join,
          hintedSide: LogicalPlan,
          prunedSide: LogicalPlan): (LogicalPlan, Option[String], Boolean) = {
        lazy val produced = AttributeSet(hintedSide.flatMap(_.output))
        // Whether a filter that was built from `creationPlan` is one of this join. The pruned side
        // also holds the hinted filters that pushdown brings in from the joins above. The plan
        // can be an earlier form of the hinted side, whose leaves produce the same attributes.
        def ofThisJoin(creationPlan: LogicalPlan): Boolean =
          creationPlan.exists(_.outputSet.intersect(produced).nonEmpty)
        // A key of `creationPlan`, an earlier form of the hinted side, as the hinted side outputs
        // it now. Once a subquery is joined with the outer query, the optimizer can remove an
        // alias that had to stay as the output of the subquery, which leaves the key with the id
        // of the alias's child.
        def currentKey(key: Expression, creationPlan: LogicalPlan): Option[Expression] = {
          def isOutput(e: Expression) = e.references.subsetOf(hintedSide.outputSet)
          if (isOutput(key)) {
            Some(key)
          } else {
            val aliased = AttributeMap(creationPlan.flatMap(_.expressions).flatMap(_.collect {
              case a @ Alias(child: Attribute, _) => a.toAttribute -> child
            }))
            Some(key.transform { case a: Attribute => aliased.getOrElse(a, a) }).filter(isOutput)
          }
        }
        lazy val misses = missesPruning(hintedSide)
        lazy val check = runtimeFilterHintCheck(hintedSide, cteDefs)
        // The Bloom filters built for this join, by creation key and whether the subquery prunes
        // the columns of the hinted side. Pushdown copies a filter into each branch of a union,
        // and the copies share one subquery.
        val bloomFilters =
          mutable.HashMap.empty[(Expression, Boolean), Either[String, BloomFilterMightContain]]
        var kept = 0
        var notApplied: Option[String] = None
        var declined = false
        def keep(filter: Expression): Option[Expression] = {
          kept += 1
          Some(filter)
        }
        def drop(reason: String, asSource: Boolean = false): Option[Expression] = {
          if (notApplied.isEmpty) notApplied = Some(reason)
          declined ||= asSource
          None
        }
        def settledConjunct(conjunct: Expression): Option[Expression] = conjunct match {
          case filter: BloomFilterMightContain if filter.getTagValue(HINTED_CREATION_SIDE)
              .exists(creation => ofThisJoin(creation.plan)) =>
            val creation = filter.getTagValue(HINTED_CREATION_SIDE).get
            val changed = !creation.plan.fastEquals(hintedSide)
            // Constant folding leaves a literal for the hash of a literal key.
            val value = filter.valueExpression
            lazy val key = currentKey(creation.key, creation.plan)
            lazy val rejection = key.filter(_ => changed)
              .flatMap(check.rejection(value, _, checkFunctions = false))
            if (misses) {
              drop(CannotFollow)
            } else if (key.isEmpty) {
              drop(NoHintedFilter)
            } else if (rejection.isDefined) {
              drop(rejection.get, asSource = true)
            } else {
              val current = creation.copy(key = key.get, plan = hintedSide)
              def bloomFilterOf(pruned: Boolean) = bloomFilters.getOrElseUpdate(
                (key.get.canonicalized, pruned), bloomFilter(value, current.copy(pruned = pruned)))
              // The subquery reads only the columns that it needs, unless it then misses the
              // pruning of a DPP filter within the hinted side: without a column of a broadcast
              // side, the subquery builds another broadcast than the one that the filter reuses.
              def subqueryMissesPruning(f: BloomFilterMightContain) =
                f.bloomFilterExpression.exists {
                  case s: ScalarSubquery => missesPruning(s.plan)
                  case _ => false
                }
              val built = bloomFilterOf(pruned = true) match {
                case Right(f) if subqueryMissesPruning(f) => bloomFilterOf(pruned = false)
                case other => other
              }
              built match {
                case Left(reason) => drop(reason)
                case Right(f) if !changed &&
                    f.getTagValue(HINTED_CREATION_SIDE).exists(_.pruned == creation.pruned) =>
                  keep(filter)
                case Right(f) if f.valueExpression.fastEquals(value) => keep(f)
                case Right(f) =>
                  val copy = f.copy(valueExpression = value)
                  copy.copyTagsFrom(f)
                  keep(copy)
              }
            }
          case dpp: DynamicPruningSubquery
              if dpp.getTagValue(DynamicPruning.HONORS_RUNTIME_FILTER_HINT).isDefined &&
                ofThisJoin(dpp.buildQuery) =>
            lazy val keys = dpp.buildKeys.map(currentKey(_, dpp.buildQuery))
            if (misses) {
              drop(CannotFollow)
            } else if (keys.exists(_.isEmpty)) {
              drop(NoHintedFilter)
            } else if (dpp.buildQuery.fastEquals(hintedSide)) {
              keep(dpp)
            } else {
              val buildKeys = keys.map(_.get)
              val key = buildKeys(dpp.broadcastKeyIndices.head)
              // A DPP filter's subquery runs the hinted side as it is, functions included.
              val rejection = check.rejection(dpp.pruningKey, key, checkFunctions = false)
                .map(_ -> true)
                .orElse(check.rejection(dpp.pruningKey, key, checkFunctions = true).map(_ -> false))
              rejection match {
                case Some((reason, asSource)) => drop(reason, asSource)
                case None =>
                  val newDpp = dpp.copy(buildQuery = hintedSide, buildKeys = buildKeys)(
                    dpp.broadcastValueProjection)
                  newDpp.copyTagsFrom(dpp)
                  keep(newDpp)
              }
            }
          case other => Some(other)
        }
        // `condition` with this join's hinted filters settled, if that changes any of them.
        def settledCondition(condition: Expression): Option[Option[Expression]] = {
          val conjuncts = splitConjunctivePredicates(condition)
          val result = conjuncts.flatMap(settledConjunct)
          if (result.length == conjuncts.length &&
              result.zip(conjuncts).forall { case (a, b) => a eq b }) {
            None
          } else {
            Some(result.reduceOption(And))
          }
        }
        // Pushdown leaves a hinted filter in a filter, or in the condition of an inner join whose
        // sides its key both reads.
        val newPrunedSide = prunedSide transformUp {
          case f @ Filter(condition, child) =>
            settledCondition(condition).map(_.map(Filter(_, child)).getOrElse(child)).getOrElse(f)
          case j @ Join(_, _, _, Some(condition), _)
              if condition.exists(_.isInstanceOf[BloomFilterMightContain]) =>
            settledCondition(condition).map { newCondition =>
              val newJoin = j.copy(condition = newCondition)
              newJoin.copyTagsFrom(j)
              newJoin
            }.getOrElse(j)
        }
        (newPrunedSide, notApplied.filter(_ => kept == 0), declined && kept == 0)
      }
      // Settles the hinted joins of `plan`. The hinted filters of a join depend on the filters
      // that pushdown brings into its hinted side from the joins above it, and on those of the
      // joins within its hinted side. A filter that is dropped there leaves the hinted side as it
      // was. So a join is settled after the joins above it and those within its hinted side, and
      // before those within its pruned side.
      def settle(plan: LogicalPlan): LogicalPlan = plan match {
        case join: Join if hintedAndPrunedSide(join).isDefined =>
          val (hintedSide, prunedSide) = hintedAndPrunedSide(join).get
          val newHintedSide = settle(hintedSide)
          val (ownSettled, notApplied, declined) = settleFilters(join, newHintedSide, prunedSide)
          val newPrunedSide = settle(ownSettled)
          notApplied.foreach(reportHintNotApplied(join, _))
          if (newHintedSide.eq(hintedSide) && newPrunedSide.eq(prunedSide)) {
            join
          } else {
            val newJoin = join.withNewChildren(
              if (join.left eq hintedSide) Seq(newHintedSide, newPrunedSide)
              else Seq(newPrunedSide, newHintedSide)).asInstanceOf[Join]
            if (declined) withoutRuntimeFilterHint(newJoin) else newJoin
          }
        case _ => plan.mapChildren(settle)
      }
      def pushDown(plan: LogicalPlan): LogicalPlan = {
        var current = plan
        var pushed = PushDownPredicates(current)
        var iteration = 1
        while (!pushed.fastEquals(current) && iteration < conf.optimizerMaxIterations) {
          current = pushed
          pushed = PushDownPredicates(current)
          iteration += 1
        }
        pushed
      }
      var current = injected
      var settled = false
      var iteration = 0
      while (!settled && iteration < conf.optimizerMaxIterations) {
        val pushed = pushDown(current)
        current = settle(pushed)
        settled = current eq pushed
        iteration += 1
      }
      current
    }
    withSettledHintedFilters(plan transformUp {
      case join: Join if isRuntimeFilterHintDecided(join.hint) => join
      // Only a hinted join has anything to do with Bloom filters disabled or once the number of
      // filters is reached.
      case join: Join if !hasRuntimeFilterHint(join.hint) &&
          (!bloomFilterEnabled || filterCounter >= numFilterThreshold) => join
      case join @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, hint) =>
        var newLeft = left
        var newRight = right
        if (isRuntimeFilterHintAmbiguous(hint)) {
          reportHintNotApplied(join,
            "the runtime filter source is ambiguous as both join sides are hinted")
        }
        // A side hinted as the runtime filter source is the creation side, so the filter is
        // applied to the other side. An ambiguous hint is ignored, leaving the heuristics to
        // decide, and so is a hinted side that cannot be a runtime filter source on any join key.
        val hintedSource = runtimeFilterSourceSide(hint)
        lazy val hintCheck = hintedSource.map { side =>
          runtimeFilterHintCheck(if (side == BuildLeft) left else right, cteDefs)
        }
        // Why the hinted side cannot prune the other side on the key pair `l` and `r`.
        def hintRejection(l: Expression, r: Expression, checkFunctions: Boolean): Option[String] =
          hintCheck.flatMap { check =>
            if (hintedSource.contains(BuildRight)) check.rejection(l, r, checkFunctions)
            else check.rejection(r, l, checkFunctions)
          }
        // The DPP filters on each key pair that prune the left side and the right side.
        val keyFilters = leftKeys.lazyZip(rightKeys).map { (l, r) =>
          (l, r, dynamicPruningFilters(left, l, r), dynamicPruningFilters(right, r, l))
        }
        def honorHint(filters: Seq[DynamicPruningSubquery]): Boolean =
          filters.exists(_.getTagValue(DynamicPruning.HONORS_RUNTIME_FILTER_HINT).isDefined)
        // Why the hinted side cannot prune the other side on each key pair. A key whose DPP
        // filter honors the hint was accepted by [[PartitionPruning]]; a hinted side that a filter
        // pushed into it has changed since is checked when the DPP filter is built again from it.
        // A Bloom filter's subquery is checked for the functions it calls once it is built.
        val keyRejections = hintedSource.map { side =>
          keyFilters.map { case (l, r, leftFilters, rightFilters) =>
            if (honorHint(if (side == BuildRight) leftFilters else rightFilters)) None
            else hintRejection(l, r, checkFunctions = false)
          }
        }
        val declinedReason =
          keyRejections.flatMap(reasons => if (reasons.forall(_.isDefined)) reasons.head else None)
        // A hint that is left to the pass over the outer query is neither reported nor marked as
        // decided here, and the join gets no Bloom filter here: that pass decides the hint and
        // adds the join's filters.
        val leftToOuterQuery = keyRejections.exists(leavesRuntimeFilterHintToOuterQuery(plan, _))
        if (!leftToOuterQuery) declinedReason.foreach(reportHintNotApplied(join, _))
        val hinted = hintedSource.isDefined && declinedReason.isEmpty
        val injectLeftHinted = hinted && hintedSource.contains(BuildRight)
        val injectRightHinted = hinted && hintedSource.contains(BuildLeft)
        // The first reason the hint could not be applied on a key. The hinted side is the same
        // for every key, so the first reason is as representative as any.
        var notAppliedReason: Option[String] = None
        def hintBlocked(reason: => String): Unit = {
          if (notAppliedReason.isEmpty) notAppliedReason = Some(reason)
        }
        lazy val hasShuffle = isProbablyShuffleJoin(left, right, hint)
        // Tries to filter `applicationSide` with a filter built from `creationSide`. Returns the
        // filtered side, or `currentApplicationSide` when no filter was added, recording the
        // reason when this direction is the hinted one (`isHintedDirection`). Requirements:
        // 1. The application side is not a stream with a stateful operator (`isStatefulStream`)
        // 2. The join type supports pruning the application side
        // 3. The application side is not the hinted source (`isHintedSource`), which is never
        //    itself filtered
        // 4. The join is a shuffle join, or a broadcast join with a shuffle below it -- an
        //    estimate of whether the filter pays off, so a hint waives it
        // 5. There is no Bloom filter on the application side's key yet, unless the creation side
        //    is hinted and the filter on the key was built from another creation side
        def tryInject(
            applicationSide: LogicalPlan,
            currentApplicationSide: LogicalPlan,
            applicationSideKey: Expression,
            creationSide: LogicalPlan,
            creationSideKey: Expression,
            canPrune: Boolean,
            isHintedDirection: Boolean,
            isHintedSource: Boolean,
            sideName: String,
            keyDpps: Seq[DynamicPruningSubquery]): LogicalPlan = {
          if (isStatefulStream(applicationSide)) {
            if (isHintedDirection) {
              hintBlocked(s"the $sideName side is a stream with a stateful operator")
            }
            currentApplicationSide
          } else if (!canPrune) {
            if (isHintedDirection) {
              // `ExistenceJoin.sql` throws, as the type never appears in SQL.
              val joinDescription = joinType match {
                case _: ExistenceJoin => "an existence"
                case _ => s"a ${joinType.sql.toLowerCase(Locale.ROOT)}"
              }
              hintBlocked(s"the $sideName side of $joinDescription join cannot be pruned")
            }
            currentApplicationSide
          } else if (isHintedDirection) {
            injectHintedFilter(applicationSide, currentApplicationSide, applicationSideKey,
              creationSide, creationSideKey, keyDpps)
          } else if (isHintedSource || !(hasShuffle || probablyHasShuffle(applicationSide)) ||
              hasBloomFilter(currentApplicationSide, applicationSideKey)) {
            currentApplicationSide
          } else {
            // The creation side leaves out the key's own DPP filters, which physical planning
            // drops, and every `onlyInBroadcast` DPP filter, e.g. one a join above pushed into it:
            // it finds no broadcast in the bloom filter's subquery and is `true` there. A DPP
            // filter that runs as its own subquery stays, and the search for the creation side
            // does not look at it: `isSimpleExpression` sees the plan of a subquery too, where an
            // expression such as a UDF or a `LIKE` would stop the search.
            // If the key has DPP filters, they are all dropped, so step 3 above applies. The
            // bloom filter evaluates its creation side again, apart from the join. Its subquery
            // can also read more of that side than the join does, since filters on other keys, DPP
            // filters or bloom filters, can narrow only the join's side. And the bloom filter
            // hashes the bytes of the key, so its type must compare by its bytes, which a collated
            // string does not.
            val creationPlan =
              withoutDppFilters(creationSide, dpp => dpp.onlyInBroadcast || keyDpps.contains(dpp))
            extractBeneficialFilterCreatePlan(
              applicationSide, creationPlan, applicationSideKey, creationSideKey)
              .filter(c => keyDpps.isEmpty ||
                (UnsafeRowUtils.isBinaryStable(applicationSideKey.dataType) &&
                  canPushDown(applicationSideKey, applicationSide) && canEvaluateAgain(c)))
              .map(injectFilter(applicationSideKey, currentApplicationSide, _))
              .getOrElse(currentApplicationSide)
          }
        }
        // Filters `applicationSide` with a Bloom filter built from the hinted `creationSide`.
        def injectHintedFilter(
            applicationSide: LogicalPlan,
            currentApplicationSide: LogicalPlan,
            applicationSideKey: Expression,
            creationSide: LogicalPlan,
            creationSideKey: Expression,
            keyDpps: Seq[DynamicPruningSubquery]): LogicalPlan = {
          lazy val creation = FilterCreationSide(
            creationSideKey, creationSide, useMaterializedThreshold = false, hinted = true)
          if (!UnsafeRowUtils.isBinaryStable(applicationSideKey.dataType) ||
              !UnsafeRowUtils.isBinaryStable(creationSideKey.dataType)) {
            // The Bloom filter hashes the bytes of a key, which a collation can compare as equal
            // to other bytes (SPARK-58486).
            hintBlocked("the join key has a collation the Bloom filter hash does not follow")
            currentApplicationSide
          } else if (hasBloomFilter(currentApplicationSide, applicationSideKey) &&
              !hasBloomFilter(applicationSide, applicationSideKey)) {
            // Unlike the heuristics, a hinted side adds its own filter even on a key that already
            // has one, but only one: this key got it on another key pair of this join.
            currentApplicationSide
          } else if (keyDpps.nonEmpty && !canPushDown(applicationSideKey, applicationSide)) {
            // The key's DPP filters, which [[PartitionPruning]] added without the hint, are all
            // dropped, so step 3 above applies to the Bloom filter as without the hint.
            hintBlocked("a Bloom filter pushed down on the join key could fail or change a " +
              "broadcast that a DPP filter reuses")
            currentApplicationSide
          } else if (!hintedKeyCannotFail(applicationSideKey, applicationSide)) {
            hintBlocked("the join key could fail on rows the join does not read")
            currentApplicationSide
          } else {
            hintCheck.flatMap(_.rejection(applicationSideKey, creationSideKey,
                checkFunctions = false)) match {
              case Some(reason) =>
                hintBlocked(reason)
                currentApplicationSide
              case None =>
                bloomFilter(new XxHash64(Seq(applicationSideKey)), creation) match {
                  case Right(filter) => Filter(filter, currentApplicationSide)
                  case Left(reason) =>
                    hintBlocked(reason)
                    currentApplicationSide
                }
            }
          }
        }
        // A DPP filter prunes by whole partitions rather than by rows, so it is preferred: a key
        // it prunes gets no Bloom filter, while the other keys still may. The hint is honored by
        // a DPP filter that [[PartitionPruning]] marked as honoring it. A hinted side that cannot
        // honor the hint on a key gets DPP from its own estimates, as without the hint, so such a
        // DPP filter does not count as the hint applied.
        val prunedKeys = keyFilters.map { case (l, r, leftFilters, rightFilters) =>
          val filters = if (hinted) {
            if (injectLeftHinted) leftFilters else rightFilters
          } else {
            leftFilters ++ rightFilters
          }
          (l, r, filters, hinted && honorHint(filters))
        }
        var appliedHint = prunedKeys.exists(_._4)
        (if (leftToOuterQuery) Nil else prunedKeys).foreach { case (l, r, keyDpps, honorsHint) =>
          // Physical planning can still drop the key's DPP filters, and a key whose DPP filters
          // are all dropped can get a Bloom filter.
          if (keyDpps.exists(isKept)) {
            // Already pruned by partition pruning: no Bloom filter for the key.
            if (hinted && !honorsHint) {
              val (prunedKey, prunedSide) = if (injectLeftHinted) (l, left) else (r, right)
              hintRejection(l, r, checkFunctions = true)
                .orElse(Option.when(!hintedKeyCannotFail(prunedKey, prunedSide)) {
                  "the join key could fail on rows the join does not read"
                }).foreach(hintBlocked(_))
            }
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
            newLeft = tryInject(left, newLeft, l, right, r, canPruneLeft(joinType),
              injectLeftHinted, injectRightHinted, "left", keyDpps)
            // Did we actually inject on the left? If not, try on the right
            if (newLeft.fastEquals(oldLeft)) {
              newRight = tryInject(right, newRight, r, left, l, canPruneRight(joinType),
                injectRightHinted, injectLeftHinted, "right", keyDpps)
            }
            if (!newLeft.fastEquals(oldLeft) || !newRight.fastEquals(oldRight)) {
              filterCounter = filterCounter + 1
              appliedHint = appliedHint || hinted
            }
          }
        }
        if (hinted && !appliedHint) {
          reportHintNotApplied(join, notAppliedReason.getOrElse(NoHintedFilter))
        }
        val result = join.withNewChildren(Seq(newLeft, newRight)).asInstanceOf[Join]
        if (leftToOuterQuery || !hasRuntimeFilterHint(hint)) {
          result
        } else if (hinted) {
          markRuntimeFilterHintDecided(result)
        } else {
          // Ambiguous, or declined on every key.
          withoutRuntimeFilterHint(result)
        }
      case join @ Join(_, _, _, _, hint) if hasRuntimeFilterHint(hint) =>
        // A runtime filter is built from the join keys, so a join without equi-join keys has
        // nothing to build from.
        reportHintNotApplied(join, if (isRuntimeFilterHintAmbiguous(hint)) {
          "the runtime filter source is ambiguous as both join sides are hinted"
        } else {
          "no equi-join keys"
        })
        withoutRuntimeFilterHint(join)
    })
  }

  private def hasRuntimeFilterHint(hint: JoinHint): Boolean = {
    hintToRuntimeFilterSourceLeft(hint) || hintToRuntimeFilterSourceRight(hint)
  }

  // A HintInfo carries no relation name, so the join is identified by the hinted side and its
  // condition. Only the runtime filter facet is reported.
  private def reportHintNotApplied(join: Join, reason: String): Unit = {
    val side = (hintToRuntimeFilterSourceLeft(join.hint),
      hintToRuntimeFilterSourceRight(join.hint)) match {
      case (true, true) => "both"
      case (true, false) => "left"
      case _ => "right"
    }
    val condition = join.condition.map(_.toString).getOrElse("none")
    hintErrorHandler.joinHintNotSupported(HintInfo(runtimeFilterSource = true),
      s"$reason (hinted side: $side, join condition: $condition)")
  }

  private def hasRuntimeFilterHint(plan: LogicalPlan): Boolean = plan.exists {
    case Join(_, _, _, _, hint) => hasRuntimeFilterHint(hint)
    case _ => false
  }

  // With Bloom filters disabled the rule still runs over a plan with a runtime filter hint, so the
  // hint is credited to a DPP filter or reported as not applied.
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // A correlated subquery is decorrelated into a join of the main plan later, where the rule
    // visits its joins, hinted ones included.
    case s: Subquery if s.correlated => plan
    case _ if !conf.runtimeFilterBloomFilterEnabled && !hasRuntimeFilterHint(plan) => plan
    case _ => tryInjectRuntimeFilter(plan)
  }

}

/**
 * Pushes down the Bloom filters [[InjectRuntimeFilter]] adds on top of a join side. A no-op on a
 * plan without a Bloom filter, so a query without a runtime filter pays no extra pushdown pass.
 */
object PushDownRuntimeFilters extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    if (plan.containsPattern(BLOOM_FILTER_MIGHT_CONTAIN)) PushDownPredicates(plan) else plan
}
