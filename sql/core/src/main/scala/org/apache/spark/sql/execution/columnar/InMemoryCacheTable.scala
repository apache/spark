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

package org.apache.spark.sql.execution.columnar

import java.util
import java.util.OptionalLong

import org.apache.spark.sql.catalyst.expressions.{
  Ascending, Attribute, AttributeReference, Descending, NullsFirst, NullsLast,
  SortOrder => CatalystSortOrder
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{
  Expression => V2Expression, FieldReference, NamedReference,
  NullOrdering => V2NullOrdering, SortDirection => V2SortDirection,
  SortOrder => V2SortOrder, SortValue
}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.connector.read.{
  Scan, ScanBuilder, Statistics => V2Statistics, SupportsPushDownLimit,
  SupportsPushDownRequiredColumns, SupportsPushDownV2Filters, SupportsReportOrdering,
  SupportsReportPartitioning, SupportsReportStatistics, SupportsRuntimeV2Filtering
}
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics
import org.apache.spark.sql.connector.read.partitioning.{
  KeyGroupedPartitioning, Partitioning, UnknownPartitioning
}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation, SupportsPrePushdownStats}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A DSv2 [[Table]] wrapper around [[InMemoryRelation]] that routes cached DataFrames through
 * the [[V2ScanRelationPushDown]] optimizer batch.
 *
 * The pre-DSv2 [[InMemoryScans]] planner strategy already supports column pruning, filter
 * pushdown, and sort-order propagation via [[pruneFilterProject]]. This wrapper adds two
 * capabilities that are not available in the existing path:
 *  - Dynamic Partition Pruning via [[SupportsRuntimeV2Filtering]]
 *  - Per-partition LIMIT pushdown via [[SupportsPushDownLimit]]
 *
 * Implements [[SupportsPrePushdownStats]]: statistics come from the underlying
 * [[InMemoryRelation]] (the materialized cache, or the stats of the plan to cache before
 * materialization), so they are accurate even before pushdown. Optimizer rules that run
 * before [[V2ScanRelationPushDown]] (e.g. PushDownLeftSemiAntiJoin's broadcast check) may
 * therefore safely read stats from the wrapping [[DataSourceV2Relation]], matching the
 * behavior of the legacy path where InMemoryRelation reported its own stats directly.
 *
 * Invariant: the relation's output column names are unique ([[CacheManager]] only creates
 * this wrapper via [[InMemoryCacheTable.supportsOutput]]). The DSv2 contract identifies
 * columns by name (column pruning via [[StructType]], [[V2Statistics]] keyed by
 * [[NamedReference]], and attribute reconstruction in PushDownUtils.toOutputAttrs), so a
 * query output with duplicate names - which, unlike a table schema, can legitimately occur,
 * e.g. after a join - cannot round-trip through this wrapper and stays on the legacy
 * InMemoryRelation path instead. The name-based lookups below rely on this invariant.
 */
private[sql] class InMemoryCacheTable(val relation: InMemoryRelation)
    extends Table with SupportsRead with SupportsPrePushdownStats {

  // Two InMemoryCacheTable instances wrapping the same CachedRDDBuilder are equal.
  // All InMemoryRelation copies from the same CachedData share the same cacheBuilder by reference.
  override def equals(other: Any): Boolean = other match {
    case t: InMemoryCacheTable => relation.cacheBuilder eq t.relation.cacheBuilder
    case _ => false
  }
  override def hashCode(): Int = System.identityHashCode(relation.cacheBuilder)

  override def name(): String = relation.cacheBuilder.cachedName

  override def schema(): StructType = DataTypeUtils.fromAttributes(relation.output)

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): InMemoryScanBuilder =
    new InMemoryScanBuilder(relation)
}

private[sql] object InMemoryCacheTable {
  /**
   * Whether a cached plan's output can be represented by this DSv2 wrapper. The wrapper
   * (and the generic DSv2 machinery around it) identifies columns by name, so the output
   * names must be unique; callers must fall back to the legacy [[InMemoryRelation]] path
   * otherwise.
   */
  def supportsOutput(output: Seq[Attribute]): Boolean = {
    val names = output.map(_.name)
    names.distinct.length == names.length
  }
}

/**
 * DSv2 [[ScanBuilder]] for [[InMemoryRelation]].
 *
 * Implements [[SupportsPushDownRequiredColumns]] and [[SupportsPushDownV2Filters]] so that
 * [[V2ScanRelationPushDown]] records the pruned columns and predicates before building the scan.
 * The pre-DSv2 path ([[InMemoryScans]]) already performs the same pruning via
 * [[pruneFilterProject]], so these interfaces mirror existing behavior under the new path.
 * [[SupportsPushDownLimit]] is the new addition: it enables per-partition LIMIT that is not
 * available in [[InMemoryScans]].
 */
private[sql] class InMemoryScanBuilder(relation: InMemoryRelation)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownV2Filters
    with SupportsPushDownLimit {

  private var requiredSchema: StructType = DataTypeUtils.fromAttributes(relation.output)
  private var _pushedPredicates: Array[V2Predicate] = Array.empty
  private var _pushedLimit: Option[Int] = None

  override def pruneColumns(required: StructType): Unit = {
    requiredSchema = required
  }

  /**
   * Records predicates so Spark adds a post-scan [[FilterExec]] for row-level evaluation.
   * Batch-level min/max pruning is handled at physical planning: [[DataSourceV2Strategy]]
   * passes the Catalyst [[FilterExec]] expressions extracted by [[PhysicalOperation]] directly
   * to [[InMemoryTableScanExec]], which forwards them to [[CachedBatchSerializer.buildFilter]].
   * The V2 [[Predicate]]s stored here are not used for batch pruning.
   */
  override def pushPredicates(predicates: Array[V2Predicate]): Array[V2Predicate] = {
    _pushedPredicates = predicates
    predicates
  }

  override def pushedPredicates(): Array[V2Predicate] = _pushedPredicates

  /**
   * Pushes a LIMIT down into the scan. Returns true to indicate the limit was accepted.
   * Because caching may interleave data across partitions, this is always a partial push:
   * Spark will still apply a LocalLimit on top to enforce the exact count.
   */
  override def pushLimit(limit: Int): Boolean = {
    _pushedLimit = Some(limit)
    true
  }

  /** Always partially pushed: Spark applies a LocalLimit on top. */
  override def isPartiallyPushed(): Boolean = true

  override def build(): InMemoryCacheScan = {
    val requiredFieldNames = requiredSchema.fieldNames.toSet
    val prunedAttrs =
      if (requiredFieldNames == relation.output.map(_.name).toSet) relation.output
      else relation.output.filter(a => requiredFieldNames.contains(a.name))
    new InMemoryCacheScan(relation, prunedAttrs, _pushedLimit)
  }
}

/**
 * DSv2 [[Scan]] for [[InMemoryRelation]].
 *
 * Physical execution is handled by [[InMemoryTableScanExec]] via [[DataSourceV2Strategy]]
 * rather than [[Batch]]/[[InputPartition]] to preserve the existing efficient columnar path.
 *
 * Reports:
 *  - Ordering ([[SupportsReportOrdering]]): propagates the ordering of the original cached plan
 *    so the optimizer can eliminate redundant sorts on top of the cache.
 *  - Statistics ([[SupportsReportStatistics]]): exposes accurate row count and size from
 *    accumulated scan metrics once the cache is materialized, feeding AQE decisions.
 *  - Partitioning ([[SupportsReportPartitioning]]): reports [[KeyGroupedPartitioning]] when
 *    the cached plan was hash-partitioned on explicit columns, allowing the optimizer to
 *    skip shuffles for downstream joins/aggregates on the same key.
 *  - Runtime filtering ([[SupportsRuntimeV2Filtering]]): enables Dynamic Partition Pruning
 *    on cached scans; [[DynamicPruning]] expressions are passed via [[InMemoryTableScanExec]]
 *    for batch-level min/max pruning.
 */
private[sql] class InMemoryCacheScan(
    val relation: InMemoryRelation,
    val prunedAttrs: Seq[Attribute],
    val pushedLimit: Option[Int] = None)
    extends Scan
    with SupportsReportOrdering
    with SupportsReportStatistics
    with SupportsReportPartitioning
    with SupportsRuntimeV2Filtering {

  override def readSchema(): StructType = DataTypeUtils.fromAttributes(prunedAttrs)

  /**
   * Converts the Catalyst sort ordering of the cached plan to V2 [[SortOrder]]s.
   * The longest valid prefix is emitted: we stop at the first key that is either not an
   * [[AttributeReference]] or was pruned from the output. Skipping a key in the middle and
   * emitting later keys would report a non-existent ordering and allow the optimizer to
   * eliminate required sorts.
   */
  override def outputOrdering(): Array[V2SortOrder] = {
    val prunedNames = prunedAttrs.map(_.name).toSet
    // Map each key to its V2 form, stopping at the first key that is either not an
    // AttributeReference or was pruned (the takeWhile on the longest defined prefix).
    relation.outputOrdering.iterator.map {
      case CatalystSortOrder(attr: AttributeReference, direction, nullOrdering, _)
          if prunedNames.contains(attr.name) =>
        val v2Dir = direction match {
          case Ascending => V2SortDirection.ASCENDING
          case Descending => V2SortDirection.DESCENDING
        }
        val v2Nulls = nullOrdering match {
          case NullsFirst => V2NullOrdering.NULLS_FIRST
          case NullsLast => V2NullOrdering.NULLS_LAST
        }
        Some(SortValue(FieldReference.column(attr.name), v2Dir, v2Nulls): V2SortOrder)
      case _ => None
    }.takeWhile(_.isDefined).map(_.get).toArray
  }

  /**
   * Reports the output partitioning of the cached plan so the optimizer can skip
   * shuffles for downstream operations on the same partitioning key.
   */
  override def outputPartitioning(): Partitioning = {
    relation.cachedPlan.outputPartitioning match {
      case HashPartitioning(expressions, numPartitions) =>
        val keys = expressions.collect { case a: AttributeReference =>
          FieldReference.column(a.name).asInstanceOf[V2Expression]
        }
        if (keys.size == expressions.size) {
          new KeyGroupedPartitioning(keys.toArray, numPartitions)
        } else {
          new UnknownPartitioning(numPartitions)
        }
      case other => new UnknownPartitioning(other.numPartitions)
    }
  }

  /**
   * Exposes hash-partitioning key columns for Dynamic Partition Pruning.
   * Spark will inject runtime IN-list filters on these attributes when it can
   * derive them from a broadcast side of a join.
   */
  override def filterAttributes(): Array[NamedReference] = {
    relation.cachedPlan.outputPartitioning match {
      case HashPartitioning(exprs, _) =>
        exprs.collect { case a: AttributeReference =>
          FieldReference.column(a.name).asInstanceOf[NamedReference]
        }.toArray
      case _ => Array.empty
    }
  }

  /**
   * Intentional no-op. The V2 contract expects `filter()` to prune [[InputPartition]]s before
   * `Batch.planInputPartitions()`, but [[InMemoryCacheScan]] never goes through [[BatchScanExec]].
   * [[DataSourceV2Strategy]] special-cases [[InMemoryCacheScan]] and creates
   * [[InMemoryTableScanExec]] directly, passing [[DynamicPruning]] expressions via
   * `runtimeFilters`. We implement [[SupportsRuntimeV2Filtering]] only to expose
   * [[filterAttributes]], which lets the optimizer inject DPP filters into the plan.
   *
   * Because `filter()` accepts nothing, `pushedPredicates()` correctly uses the inherited
   * empty default; we intentionally do not expose the compile-time predicates recorded by
   * [[InMemoryScanBuilder]] here, as those are a different concept from runtime-pushed ones.
   */
  override def filter(predicates: Array[V2Predicate]): Unit = {}

  override def estimateStatistics(): V2Statistics = {
    val stats = relation.computeStats()
    // Scale sizeInBytes proportionally to the number of columns actually read.
    // This gives the optimizer an accurate size estimate after column pruning.
    val scaledSize: Long =
      if (relation.output.nonEmpty && prunedAttrs.size < relation.output.size) {
        (stats.sizeInBytes * prunedAttrs.size / relation.output.size).toLong.max(1)
      } else {
        stats.sizeInBytes.toLong
      }
    // Only report column stats for pruned (selected) attributes.
    val prunedNames = prunedAttrs.map(_.name).toSet
    val v2ColStats = new util.HashMap[NamedReference, ColumnStatistics]()
    stats.attributeStats
      .filter { case (attr, _) => prunedNames.contains(attr.name) }
      .foreach { case (attr, colStat) =>
        val cs = new ColumnStatistics {
          override def distinctCount(): OptionalLong =
            colStat.distinctCount
              .map(v => OptionalLong.of(v.toLong)).getOrElse(OptionalLong.empty())
          override def min(): util.Optional[Object] =
            colStat.min.map(v => util.Optional.of(v.asInstanceOf[Object]))
              .getOrElse(util.Optional.empty[Object]())
          override def max(): util.Optional[Object] =
            colStat.max.map(v => util.Optional.of(v.asInstanceOf[Object]))
              .getOrElse(util.Optional.empty[Object]())
          override def nullCount(): OptionalLong =
            colStat.nullCount.map(v => OptionalLong.of(v.toLong)).getOrElse(OptionalLong.empty())
          override def avgLen(): OptionalLong =
            colStat.avgLen.map(OptionalLong.of).getOrElse(OptionalLong.empty())
          override def maxLen(): OptionalLong =
            colStat.maxLen.map(OptionalLong.of).getOrElse(OptionalLong.empty())
        }
        v2ColStats.put(FieldReference.column(attr.name), cs)
      }
    new V2Statistics {
      override def sizeInBytes(): OptionalLong = OptionalLong.of(scaledSize)
      override def numRows(): OptionalLong =
        stats.rowCount.map(c => OptionalLong.of(c.toLong)).getOrElse(OptionalLong.empty())
      override def columnStats(): util.Map[NamedReference, ColumnStatistics] = v2ColStats
    }
  }
}

/**
 * Extractor that matches any in-plan representation of a cached DataFrame and returns its
 * underlying [[InMemoryRelation]].
 *
 * Three forms appear depending on the query stage:
 *  - [[InMemoryRelation]] - the direct node (e.g. as stored in [[CachedData]]).
 *  - [[DataSourceV2Relation]] backed by [[InMemoryCacheTable]] - produced by [[CacheManager]]
 *    in `useCachedData`, visible in `QueryExecution.withCachedData`.
 *  - [[DataSourceV2ScanRelation]] backed by [[InMemoryCacheScan]] - after
 *    [[V2ScanRelationPushDown]] optimizes the above, visible in `QueryExecution.optimizedPlan`.
 */
object CachedRelation {
  def unapply(plan: LogicalPlan): Option[InMemoryRelation] = plan match {
    case mem: InMemoryRelation => Some(mem)
    case DataSourceV2Relation(table: InMemoryCacheTable, _, _, _, _, _) => Some(table.relation)
    case DataSourceV2ScanRelation(_, scan: InMemoryCacheScan, _, _, _, _) => Some(scan.relation)
    case _ => None
  }
}
