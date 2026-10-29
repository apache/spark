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
import scala.util.Try

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{INVOKE, JSON_TO_STRUCT, LEFT_SEMI_OR_ANTI_JOIN, LIKE_FAMLIY, PYTHON_UDF, REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE, SCALA_UDF}
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.connector.read.SupportsReportPartitioning
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder}

/**
 * Insert a runtime filter on one side of the join (we call this side the application side) if
 * we can extract a runtime filter from the other side (creation side). A simple case is that
 * the creation side is a table scan with a selective filter.
 * The runtime filter is logically an IN subquery with the join keys. Currently it's always
 * bloom filter but we may add other physical implementations in the future.
 */
object InjectRuntimeFilter extends Rule[LogicalPlan] with PredicateHelper with JoinSelectionHelper {

  /**
   * The metadata key of the bloom filter column of a per-partition bloom filter's aggregate. Its
   * value is the exprId of the subquery of the global bloom filter that the per-partition one can
   * replace, or -1 when there is none.
   */
  val PER_PARTITION_BLOOM_FILTER = "__per_partition_bloom_filter"

  /**
   * For the condition of a semi join that `injectPerPartitionBloomFilter` builds, the bloom filter
   * column and the value it carries under [[PER_PARTITION_BLOOM_FILTER]].
   */
  def perPartitionBloomFilter(condition: Expression): Option[(Attribute, Long)] =
    splitConjunctivePredicates(condition).collectFirst {
      case BloomFilterMightContain(bloomFilter: Attribute, _)
          if bloomFilter.metadata.contains(PER_PARTITION_BLOOM_FILTER) =>
        (bloomFilter, bloomFilter.metadata.getLong(PER_PARTITION_BLOOM_FILTER))
    }

  /**
   * Whether `e` checks the bloom filter of the subquery with the exprId `global`, also once
   * `MergeSubplans` merged that subquery with others.
   */
  def isGlobalCheck(global: Long)(e: Expression): Boolean = e match {
    case b: BloomFilterMightContain => b.bloomFilterExpression.exists {
      case subquery: PlanExpression[_] => subquery.exprId.id == global
      case _ => false
    }
    case _ => false
  }

  private case class FilterCreationSide(
      key: Expression,
      plan: LogicalPlan,
      useMaterializedThreshold: Boolean,
      materializedRowCount: Option[BigInt] = None,
      materializedSizeInBytes: Option[BigInt] = None) {

    // The creation side's row count, when known. A count of 0 is taken as unknown.
    def rowCount: Option[BigInt] =
      materializedRowCount.orElse(plan.stats.rowCount).filter(_ > 0)

    // The creation side's size, and the threshold the input of one bloom filter must not exceed.
    def sizeInBytes: BigInt = materializedSizeInBytes.getOrElse(plan.stats.sizeInBytes)
    def sizeThreshold: Long = if (useMaterializedThreshold) {
      conf.runtimeFilterMaterializedCreationSideThreshold
    } else {
      conf.runtimeFilterCreationSideThreshold
    }
  }

  private def injectFilter(
      filterApplicationSideKey: Expression,
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSide: FilterCreationSide): LogicalPlan = {
    val planWithBloomFilter =
      injectBloomFilter(filterApplicationSideKey, filterApplicationSidePlan, filterCreationSide)
    // The subquery of the bloom filter just added, if any.
    val globalBloomFilter = planWithBloomFilter match {
      case Filter(BloomFilterMightContain(subquery: ScalarSubquery, _), child)
          if child eq filterApplicationSidePlan => Some(subquery.exprId)
      case _ => None
    }
    injectPerPartitionBloomFilter(
      filterApplicationSideKey, planWithBloomFilter, globalBloomFilter, filterCreationSide)
  }

  /**
   * Adds a left semi join of the application side with one bloom filter per partition key of the
   * creation side, when a V2 scan under each side reports a keyed partitioning by the same
   * transform of its join key. The condition matches the partition keys of the two sides, so each
   * row is checked against the bloom filter of its own partition key. The application side gets
   * the transform as a column to join on, and loses it again above the semi join.
   *
   * The bloom filter `injectBloomFilter` added to `planWithBloomFilter`, if any, stays.
   * `ChoosePerPartitionBloomFilters` keeps the semi join only when the semi join costs the
   * application side nothing. It then drops that bloom filter.
   *
   * A key gets one per-partition bloom filter at most, so a later join on the same key adds none.
   *
   * The creation side's size threshold applies to one partition of it, since each bloom filter
   * holds the keys of one partition and is built in its own task.
   *
   * The bloom filter column carries [[PER_PARTITION_BLOOM_FILTER]] in its metadata, which is how
   * `ChoosePerPartitionBloomFilters` tells this semi join from one a query wrote.
   */
  private def injectPerPartitionBloomFilter(
      filterApplicationSideKey: Expression,
      planWithBloomFilter: LogicalPlan,
      globalBloomFilter: Option[ExprId],
      filterCreationSide: FilterCreationSide): LogicalPlan = {
    if (!conf.runtimeBloomFilterPerPartitionEnabled) {
      return planWithBloomFilter
    }
    val transforms = for {
      (application, _) <- partitionTransform(filterApplicationSideKey, planWithBloomFilter)
      (creation, scan) <- partitionTransform(filterCreationSide.key, filterCreationSide.plan)
      if application.isSameFunction(creation)
    } yield (application, creation, partitionsOf(creation, scan))
    transforms match {
      case Some((application, creation, numPartitions))
          if filterCreationSide.sizeInBytes / numPartitions <= filterCreationSide.sizeThreshold &&
            !hasPerPartitionBloomFilter(planWithBloomFilter, filterApplicationSideKey) =>
        val creationPartitionKey = Alias(creation, "partitionKey")()
        val bloomFilter = Alias(new BloomFilterAggregate(
          new XxHash64(Seq(filterCreationSide.key)),
          perPartitionNumItems(filterCreationSide, numPartitions)
        ).toAggregateExpression(), "bloomFilter")(explicitMetadata = Some(new MetadataBuilder()
          .putLong(PER_PARTITION_BLOOM_FILTER, globalBloomFilter.map(_.id).getOrElse(-1L))
          .build()))
        val aggregate = ColumnPruning(PullOutGroupingExpressions(Aggregate(
          Seq(creation), Seq(creationPartitionKey, bloomFilter), filterCreationSide.plan)))
        if (aggregate.containsPattern(PYTHON_UDF)) {
          planWithBloomFilter
        } else {
          perPartitionSemiJoin(planWithBloomFilter, filterApplicationSideKey, application,
            aggregate, creationPartitionKey.toAttribute, bloomFilter.toAttribute)
        }
      case _ => planWithBloomFilter
    }
  }

  /**
   * `plan` in a left semi join with `creation`. The join checks each row's `key` against the bloom
   * filter of the row's partition key. `transform` of `key` gives that partition key, and
   * `creation` gives the bloom filter `bloomFilter` of each partition key `creationPartitionKey`.
   */
  private def perPartitionSemiJoin(
      plan: LogicalPlan,
      key: Expression,
      transform: TransformExpression,
      creation: LogicalPlan,
      creationPartitionKey: Attribute,
      bloomFilter: Attribute): LogicalPlan = {
    val partitionKey = Alias(transform.copy(children = Seq(key)), "partitionKey")()
    val condition = And(
      EqualTo(partitionKey.toAttribute, creationPartitionKey),
      BloomFilterMightContain(bloomFilter, new XxHash64(Seq(key))))
    val semiJoin = Join(
      Project(plan.output :+ partitionKey, plan),
      creation,
      LeftSemi,
      Some(condition),
      JoinHint(None, Some(HintInfo(strategy = Some(SHUFFLE_HASH)))))
    Project(plan.output, semiJoin)
  }

  /**
   * An upper bound of the number of partition keys the creation side's rows spread over. The number
   * of buckets of a `bucket` transform bounds it, and so does the number of partitions the scan
   * reports, since partitions may share a key. This takes the smaller of the two.
   */
  private def partitionsOf(transform: TransformExpression, scan: DataSourceV2ScanRelation): Int = {
    val reported = scan.scan match {
      case s: SupportsReportPartitioning => s.outputPartitioning() match {
        case k: KeyGroupedPartitioning => Some(k.numPartitions())
        case _ => None
      }
      case _ => None
    }
    (transform.numBucketsOpt ++ reported).filter(_ > 0).minOption.getOrElse(1)
  }

  /** The expected number of items of one partition's bloom filter. */
  private def perPartitionNumItems(
      filterCreationSide: FilterCreationSide,
      numPartitions: Int): Long = {
    val numItems = filterCreationSide.rowCount
      .map(_.longValue)
      .getOrElse(conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_EXPECTED_NUM_ITEMS))
    math.max(1L, (numItems + numPartitions - 1) / numPartitions)
  }

  /**
   * The partition transform of the V2 scan `key` comes from under `plan`, rebuilt over `key`, with
   * the scan. Defined when:
   *  - V2 bucketing is on;
   *  - `key` is one of the scan's columns, followed down through `plan`;
   *  - the scan reports a keyed partitioning by a single transform of that column;
   *  - the transform is deterministic, since both sides of the semi join evaluate it;
   *  - Spark can evaluate the transform as is.
   *
   * The semi join is correct whatever lies between `plan` and the scan, since it checks each row
   * against the bloom filter of its own partition key. It matches the partition keys with `=`, so
   * it relies on the transform returning null only for a null key, which the join above never
   * matches. Whether it pays off is up to `ChoosePerPartitionBloomFilters`.
   */
  private def partitionTransform(
      key: Expression,
      plan: LogicalPlan): Option[(TransformExpression, DataSourceV2ScanRelation)] = {
    if (!conf.v2BucketingEnabled) {
      None
    } else {
      findExpressionAndTrackLineageDown(key, plan).flatMap {
        case (column: Attribute, scan: DataSourceV2ScanRelation) =>
          scan.keyGroupedPartitioning.collect {
            case Seq(t @ TransformExpression(_, Seq(child), _, _))
                if child.semanticEquals(column) && t.function.isDeterministic &&
                  evaluableAsIs(t) =>
              (t.copy(children = Seq(key)), scan)
          }
        case _ => None
      }
    }
  }

  // Whether Spark can build `t`'s function call and evaluate it as is. Nothing casts the
  // arguments, so the function must ask for exactly their types.
  private def evaluableAsIs(t: TransformExpression): Boolean = {
    val argumentTypes = t.numBucketsOpt.map(_ => IntegerType).toSeq ++ t.children.map(_.dataType)
    t.function.inputTypes().toSeq == argumentTypes &&
      Try(t.resolvedFunction).toOption.flatten.isDefined
  }

  private def injectBloomFilter(
      filterApplicationSideKey: Expression,
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSide: FilterCreationSide): LogicalPlan = {
    val filterCreationSideKey = filterCreationSide.key
    val filterCreationSidePlan = filterCreationSide.plan
    // Skip if the filter creation side is too big
    if (filterCreationSide.sizeInBytes > filterCreationSide.sizeThreshold) {
      return filterApplicationSidePlan
    }
    val hash = new XxHash64(Seq(filterCreationSideKey))
    val bloomFilterAgg = filterCreationSide.rowCount
      .map(rowCount => new BloomFilterAggregate(hash, rowCount.longValue))
      .getOrElse(new BloomFilterAggregate(hash))

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

  private def checksBloomFilter(condition: Expression, key: Expression): Boolean =
    splitConjunctivePredicates(condition).exists {
      case BloomFilterMightContain(_, XxHash64(Seq(valueExpression), _))
        if valueExpression.fastEquals(key) => true
      case _ => false
    }

  private def hasBloomFilter(plan: LogicalPlan, key: Expression): Boolean = plan.exists {
    case Filter(condition, _) => checksBloomFilter(condition, key)
    case _ => false
  }

  // Whether `plan` already holds a semi join with a per-partition bloom filter of `key`.
  private def hasPerPartitionBloomFilter(plan: LogicalPlan, key: Expression): Boolean =
    plan.containsPattern(LEFT_SEMI_OR_ANTI_JOIN) && plan.exists {
      case Join(_, _, LeftSemi, Some(condition), _) => checksBloomFilter(condition, key)
      case _ => false
    }

  private def tryInjectRuntimeFilter(plan: LogicalPlan): LogicalPlan = {
    var filterCounter = 0
    val numFilterThreshold = conf.getConf(SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD)
    plan transformUp {
      case join @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, hint) =>
        var newLeft = left
        var newRight = right
        leftKeys.lazyZip(rightKeys).foreach((l, r) => {
          // Check if:
          // 1. There is already a DPP filter on the key
          // 2. The keys are simple cheap expressions
          if (filterCounter < numFilterThreshold &&
            !hasDynamicPruningSubquery(left, right, l, r) &&
            isSimpleExpression(l) && isSimpleExpression(r)) {
            val oldLeft = newLeft
            val oldRight = newRight
            // Check if:
            // 1. The current join type supports prune the left side with runtime filter
            // 2. The current join is a shuffle join or a broadcast join that
            //    has a shuffle below it
            // 3. There is no bloom filter on the left key yet
            val hasShuffle = isProbablyShuffleJoin(left, right, hint)
            if (canPruneLeft(joinType) && (hasShuffle || probablyHasShuffle(left)) &&
              !hasBloomFilter(newLeft, l)) {
              extractBeneficialFilterCreatePlan(left, right, l, r).foreach { creationSide =>
                newLeft = injectFilter(l, newLeft, creationSide)
              }
            }
            // Did we actually inject a bloom filter of the whole creation side on the left? If not,
            // try on the right
            // Check if:
            // 1. The current join type supports prune the right side with runtime filter
            // 2. The current join is a shuffle join or a broadcast join that
            //    has a shuffle below it
            // 3. There is no bloom filter on the right key yet
            if ((newLeft.fastEquals(oldLeft) ||
                (conf.runtimeBloomFilterPerPartitionEnabled && !hasBloomFilter(newLeft, l))) &&
              canPruneRight(joinType) &&
              (hasShuffle || probablyHasShuffle(right)) && !hasBloomFilter(newRight, r)) {
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
