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
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{
  FieldReference, NamedReference, NullOrdering => V2NullOrdering,
  SortDirection => V2SortDirection, SortOrder => V2SortOrder, SortValue
}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{
  Scan, ScanBuilder, Statistics => V2Statistics, SupportsPushDownRequiredColumns,
  SupportsPushDownV2Filters, SupportsReportOrdering, SupportsReportStatistics
}
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A DSv2 [[Table]] wrapper around [[InMemoryRelation]], enabling [[V2ScanRelationPushDown]]
 * optimizer rules to apply column pruning, filter pushdown, and ordering/statistics reporting
 * to cached DataFrames.
 */
private[sql] class InMemoryCacheTable(val relation: InMemoryRelation)
    extends Table with SupportsRead {

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

/**
 * DSv2 [[ScanBuilder]] for [[InMemoryRelation]].
 *
 * - Column pruning via [[SupportsPushDownRequiredColumns]]: only requested columns are
 *   passed to [[InMemoryTableScanExec]], reducing deserialization work.
 * - Filter pushdown via [[SupportsPushDownV2Filters]]: predicates are recorded for
 *   batch-level pruning using per-batch min/max statistics, but all predicates are
 *   returned (category-2: still need post-scan row-level re-evaluation).
 */
private[sql] class InMemoryScanBuilder(relation: InMemoryRelation)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownV2Filters {

  private var requiredSchema: StructType = DataTypeUtils.fromAttributes(relation.output)
  private var _pushedPredicates: Array[Predicate] = Array.empty

  override def pruneColumns(required: StructType): Unit = {
    requiredSchema = required
  }

  /**
   * Accepts all predicates for batch-level min/max pruning via
   * [[CachedBatchSerializer.buildFilter]], but returns them unchanged so Spark
   * adds a post-scan [[FilterExec]] for row-level evaluation.
   */
  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    _pushedPredicates = predicates
    predicates
  }

  override def pushedPredicates(): Array[Predicate] = _pushedPredicates

  override def build(): InMemoryCacheScan = {
    val requiredFieldNames = requiredSchema.fieldNames.toSet
    val prunedAttrs =
      if (requiredFieldNames == relation.output.map(_.name).toSet) relation.output
      else relation.output.filter(a => requiredFieldNames.contains(a.name))
    new InMemoryCacheScan(relation, prunedAttrs, _pushedPredicates)
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
 */
private[sql] class InMemoryCacheScan(
    val relation: InMemoryRelation,
    val prunedAttrs: Seq[Attribute],
    val pushedPredicates: Array[Predicate])
    extends Scan
    with SupportsReportOrdering
    with SupportsReportStatistics {

  override def readSchema(): StructType = DataTypeUtils.fromAttributes(prunedAttrs)

  /**
   * Converts the Catalyst sort ordering of the cached plan to V2 [[SortOrder]]s.
   * Only attribute-reference based orderings are converted; complex expressions are skipped.
   */
  override def outputOrdering(): Array[V2SortOrder] =
    relation.outputOrdering.flatMap {
      case CatalystSortOrder(attr: AttributeReference, direction, nullOrdering, _) =>
        val v2Dir = direction match {
          case Ascending => V2SortDirection.ASCENDING
          case Descending => V2SortDirection.DESCENDING
        }
        val v2Nulls = nullOrdering match {
          case NullsFirst => V2NullOrdering.NULLS_FIRST
          case NullsLast => V2NullOrdering.NULLS_LAST
        }
        Some(SortValue(FieldReference.column(attr.name), v2Dir, v2Nulls))
      case _ => None
    }.toArray

  override def estimateStatistics(): V2Statistics = {
    val stats = relation.computeStats()
    val v2ColStats = new util.HashMap[NamedReference, ColumnStatistics]()
    stats.attributeStats.foreach { case (attr, colStat) =>
      val cs = new ColumnStatistics {
        override def distinctCount(): OptionalLong =
          colStat.distinctCount.map(v => OptionalLong.of(v.toLong)).getOrElse(OptionalLong.empty())
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
      override def sizeInBytes(): OptionalLong = OptionalLong.of(stats.sizeInBytes.toLong)
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
    case DataSourceV2ScanRelation(_, scan: InMemoryCacheScan, _, _, _) => Some(scan.relation)
    case _ => None
  }
}
