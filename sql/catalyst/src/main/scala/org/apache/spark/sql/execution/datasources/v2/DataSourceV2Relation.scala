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

package org.apache.spark.sql.execution.datasources.v2

import java.util.{Collections, Optional, OptionalLong}

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation, TimeTravelSpec}
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, AttributeSet, Expression, SortOrder, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, ExposesMetadataColumns, Histogram, HistogramBin, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.streaming.{StreamingSourceIdentifyingName, Unassigned}
import org.apache.spark.sql.catalyst.trees.TreePattern.{DATA_SOURCE_V2_RELATION, DATA_SOURCE_V2_SCAN_RELATION, TreePattern}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.{removeInternalMetadata, truncatedString, CharVarcharUtils}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, FunctionCatalog, Identifier, SupportsMetadataColumns, Table, TableCapability, TableCatalog, V2TableUtil}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.read.{Scan, Statistics => V2Statistics, SupportsReportStatistics, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.connector.read.colstats.{ColumnStatistics, Histogram => V2Histogram, HistogramBin => V2HistogramBin}
import org.apache.spark.sql.connector.read.streaming.{Offset, SparkDataStream}
import org.apache.spark.sql.internal.connector.{SupportsRuntimeCatalystFiltering, V2StatisticsUtils}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * A logical plan representing a data source v2 table.
 *
 * @param table  The table that this relation represents.
 * @param output The output attributes of this relation.
 * @param catalog catalogPlugin for the table. None if no catalog is specified.
 * @param identifier The identifier for the table. None if no identifier is defined.
 * @param options The options for this table operation. It's used to create fresh
 *                [[org.apache.spark.sql.connector.read.ScanBuilder]] and
 *                [[org.apache.spark.sql.connector.write.WriteBuilder]].
 */
abstract class DataSourceV2RelationBase(
    table: Table,
    output: Seq[AttributeReference],
    catalog: Option[CatalogPlugin],
    identifier: Option[Identifier],
    options: CaseInsensitiveStringMap,
    timeTravelSpec: Option[TimeTravelSpec] = None)
  extends LeafNode with MultiInstanceRelation with NamedRelation {

  import DataSourceV2Implicits._

  lazy val funCatalog: Option[FunctionCatalog] = catalog.collect {
    case c: FunctionCatalog => c
  }

  override def name: String = {
    (catalog, identifier) match {
      case (Some(cat), Some(ident)) => V2TableUtil.toQualifiedName(cat, ident)
      case _ => table.name()
    }
  }

  override def skipSchemaResolution: Boolean = table.supports(TableCapability.ACCEPT_ANY_SCHEMA)

  override def simpleString(maxFields: Int): String = {
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    val nameWithTimeTravelSpec = timeTravelSpec match {
      case Some(spec) => s"$name $spec"
      case _ => name
    }
    s"RelationV2$outputString $nameWithTimeTravelSpec"
  }

  override def computeStats(): Statistics = {
    if (Utils.isTesting) {
      // when testing, throw an exception if this computeStats method is called because stats should
      // not be accessed before pushing the projection and filters to create a scan. otherwise, the
      // stats are not accurate because they are based on a full table scan of all columns.
      throw SparkException.internalError(
        s"BUG: computeStats called before pushdown on DSv2 relation: $name")
    } else {
      // when not testing, return stats because bad stats are better than failing a query
      table.asReadable.newScanBuilder(options).build() match {
        case r: SupportsReportStatistics =>
          val statistics = r.estimateStatistics()
          DataSourceV2Relation.transformV2Stats(statistics, conf.defaultSizeInBytes, output)
        case _ =>
          Statistics(sizeInBytes = conf.defaultSizeInBytes)
      }
    }
  }
}

/**
 * A specialization of [[DataSourceV2RelationBase]] that supports batch scan.
 */
case class DataSourceV2Relation(
    table: Table,
    override val output: Seq[AttributeReference],
    catalog: Option[CatalogPlugin],
    identifier: Option[Identifier],
    options: CaseInsensitiveStringMap,
    timeTravelSpec: Option[TimeTravelSpec] = None)
  extends DataSourceV2RelationBase(table, output, catalog, identifier, options, timeTravelSpec)
  with ExposesMetadataColumns {

  import DataSourceV2Implicits._

  override def newInstance(): DataSourceV2Relation = {
    copy(output = output.map(_.newInstance()))
  }

  override lazy val metadataOutput: Seq[AttributeReference] = table match {
    case hasMeta: SupportsMetadataColumns =>
      metadataOutputWithOutConflicts(
        hasMeta.metadataColumns.toAttributes, hasMeta.canRenameConflictingMetadataColumns)
    case _ =>
      Nil
  }

  def withMetadataColumns(): DataSourceV2Relation = {
    val newMetadata = metadataOutput.filterNot(outputSet.contains)
    if (newMetadata.nonEmpty) {
      copy(output = output ++ newMetadata)
    } else {
      this
    }
  }

  def autoSchemaEvolution: Boolean =
    table.capabilities.contains(TableCapability.AUTOMATIC_SCHEMA_EVOLUTION)

  def isVersioned: Boolean = table.version != null

  override val nodePatterns: Seq[TreePattern] = Seq(DATA_SOURCE_V2_RELATION)
}

/**
 * A logical plan for a DSv2 table with a scan already created.
 *
 * This is used in the optimizer to push filters and projection down before conversion to physical
 * plan. This ensures that the stats that are used by the optimizer account for the filters and
 * projection that will be pushed down.
 *
 * @param relation a [[DataSourceV2Relation]]
 * @param scan a DSv2 [[Scan]]
 * @param output the output attributes of this relation
 * @param keyGroupedPartitioning if set, the partitioning expressions that are used to split the
 *                               rows in the scan across different partitions
 * @param ordering if set, the ordering provided by the scan
 * @param pushedFilters Catalyst expressions for filters that were fully pushed to the data source
 *                      and do not appear as post-scan filters. These reference the relation's
 *                      (pre-pruning) output, so they may reference columns pruned out of `output`
 *                      (e.g. an unselected partition column the source enforces internally). This
 *                      complete set is what lets `PlanMerger` soundly compare and re-enforce a
 *                      scan's filters when fusing two scans via a Spark-side scan merge
 *                      (`TableCapability.SCAN_MERGING`).
 * @param mergeableScan whether this scan may be fused with an equivalent scan by a Spark-side scan
 *                      merge (see `TableCapability.SCAN_MERGING`).
 *                      Default false (not mergeable): only the plain column-pruning + filter
 *                      pushdown path in `V2ScanRelationPushDown` sets this true, and only when the
 *                      scan carries nothing a rebuilt scan cannot reproduce. A scan with a
 *                      non-reproducible pushdown (aggregate, join, variant extraction, limit,
 *                      offset, top-N, sample) or by any other rule stays not-mergeable by default,
 *                      so merging is safe by construction -- a new scan-relation build site need
 *                      not opt out.
 */
case class DataSourceV2ScanRelation(
    relation: DataSourceV2Relation,
    scan: Scan,
    output: Seq[AttributeReference],
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    ordering: Option[Seq[SortOrder]] = None,
    pushedFilters: Seq[Expression] = Seq.empty,
    mergeableScan: Boolean = false) extends LeafNode with NamedRelation {

  // TODO: Override validConstraints to return ExpressionSet(pushedFilters) so that pushed
  // filters participate in constraint propagation (InferFiltersFromConstraints, PruneFilters).
  // Note: pushedFilters may reference columns pruned out of `output`, so constraint use must first
  // intersect with `outputSet` (a constraint has to reference the node's output).
  // This changes which filters InferFiltersFromConstraints adds or removes (e.g., it may
  // skip adding IsNotNull when the scan already implies it, or infer new filters across
  // joins), so plan stability testing is needed first.

  /**
   * Resolved attributes that the scan declares for runtime filtering via
   * [[SupportsRuntimeV2Filtering.filterAttributes]] or
   * [[SupportsRuntimeCatalystFiltering.filterAttributes]]. Empty when the scan
   * implements neither interface or exposes no attributes.
   */
  lazy val runtimeFilterAttrs: AttributeSet = {
    checkRuntimeFilteringInterfaces()
    val filterAttrs = scan match {
      case s: SupportsRuntimeV2Filtering => s.filterAttributes
      case s: SupportsRuntimeCatalystFiltering => s.filterAttributes()
      case _ => Array.empty[NamedReference]
    }
    AttributeSet(V2ExpressionUtils.resolveRefs[Attribute](
      filterAttrs.toImmutableArraySeq, this))
  }

  /**
   * Resolved attributes for which a Catalyst runtime-filtering scan fully evaluates predicates.
   * Empty for a [[SupportsRuntimeV2Filtering]] scan, which keeps its post-scan filters.
   */
  lazy val fullyPushedRuntimeFilterAttrs: AttributeSet = {
    checkRuntimeFilteringInterfaces()
    val filterAttrs = scan match {
      case s: SupportsRuntimeCatalystFiltering => s.fullyPushedFilterAttributes()
      case _ => Array.empty[NamedReference]
    }
    AttributeSet(V2ExpressionUtils.resolveRefs[Attribute](
      filterAttrs.toImmutableArraySeq, this))
  }

  override val nodePatterns: Seq[TreePattern] = Seq(DATA_SOURCE_V2_SCAN_RELATION)

  override def name: String = relation.name

  // A leaf relation references no upstream attributes. `pushedFilters` (and, for that matter,
  // partitioning/ordering) are scan metadata, not references to resolve, and `pushedFilters` may
  // reference columns pruned out of `output` (e.g. an unselected partition column). Without this
  // override those would surface as `missingInput`, which the optimizer's plan-change validation
  // flags as dangling references. `mapExpressions`/`transformExpressions` still rewrite the
  // metadata expressions -- they iterate the product directly, independent of `references`.
  override def references: AttributeSet = AttributeSet.empty

  override def simpleString(maxFields: Int): String = {
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    val nameWithTimeTravelSpec = relation.timeTravelSpec match {
      case Some(spec) => s"$name $spec"
      case _ => name
    }
    s"RelationV2$outputString $nameWithTimeTravelSpec"
  }

  override def computeStats(): Statistics = {
    if (conf.cboEnabled || conf.planStatsEnabled) {
      computeFullStats()
    } else {
      computeSizeOnlyStats()
    }
  }

  private def computeFullStats(): Statistics = {
    V2StatisticsUtils.computeStats(scan) match {
      case Some(v2Stats) =>
        DataSourceV2Relation.transformV2Stats(v2Stats, conf.defaultSizeInBytes, output)
      case _ => defaultSizeOnlyStats
    }
  }

  private def computeSizeOnlyStats(): Statistics = {
    V2StatisticsUtils.computeSizeInBytes(scan, EstimationUtils.getSizePerRow(output)) match {
      case Some(sizeInBytes) => Statistics(sizeInBytes = sizeInBytes)
      case _ => defaultSizeOnlyStats
    }
  }

  private def defaultSizeOnlyStats: Statistics = {
    Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }

  private def checkRuntimeFilteringInterfaces(): Unit = scan match {
    case _: SupportsRuntimeV2Filtering with SupportsRuntimeCatalystFiltering =>
      throw SparkException.internalError(
        "A scan must not implement both SupportsRuntimeV2Filtering and " +
        s"SupportsRuntimeCatalystFiltering, but ${scan.getClass.getName} implements both.")
    case _ =>
  }

  override def doCanonicalize(): DataSourceV2ScanRelation = {
    this.copy(
      relation = this.relation.copy(
        output = this.relation.output.map(QueryPlan.normalizeExpressions(_, this.relation.output))
      ),
      output = this.output.map(QueryPlan.normalizeExpressions(_, this.output)),
      keyGroupedPartitioning = keyGroupedPartitioning.map(
        _.map(QueryPlan.normalizeExpressions(_, output))
      ),
      ordering = ordering.map(
        _.map(o => o.copy(child = QueryPlan.normalizeExpressions(o.child, output)))
      ),
      // pushedFilters may reference columns pruned out of `output` (see the field doc), so they are
      // normalized against the relation's full output rather than `output`.
      pushedFilters = pushedFilters.map(QueryPlan.normalizeExpressions(_, relation.output))
    )
  }
}

/**
 * A specialization of [[DataSourceV2RelationBase]] that supports streaming scan.
 * It will be transformed to [[StreamingDataSourceV2ScanRelation]] during the planning phase of
 * [[MicrobatchExecution]].
 */
case class StreamingDataSourceV2Relation(
    table: Table,
    override val output: Seq[AttributeReference],
    catalog: Option[CatalogPlugin],
    identifier: Option[Identifier],
    options: CaseInsensitiveStringMap,
    metadataPath: String,
    realTimeModeDuration: Option[Long] = None,
    sourceIdentifyingName: StreamingSourceIdentifyingName = Unassigned)
  extends DataSourceV2RelationBase(table, output, catalog, identifier, options) {

  override def isStreaming: Boolean = true

  override def newInstance(): StreamingDataSourceV2Relation = {
    copy(output = output.map(_.newInstance()))
  }
}
/**
 * A specialization of [[DataSourceV2ScanRelation]] with the streaming bit set to true, as well
 * as start and end offsets for Microbatch processing.
 */
case class StreamingDataSourceV2ScanRelation(
    relation: StreamingDataSourceV2Relation,
    scan: Scan,
    output: Seq[AttributeReference],
    stream: SparkDataStream,
    startOffset: Option[Offset] = None,
    endOffset: Option[Offset] = None)
  extends LeafNode with MultiInstanceRelation with NamedRelation  {

  val (catalog, identifier) = (relation.catalog, relation.identifier)

  override def name: String = relation.table.name()

  override def simpleString(maxFields: Int): String = {
    statePrefix + "StreamingDataSourceV2ScanRelation" +
      s"${truncatedString(output, "[", ", ", "]", maxFields)} $name"
  }

  override def isStreaming: Boolean = true

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  override def computeStats(): Statistics = scan match {
    case r: SupportsReportStatistics =>
      val statistics = r.estimateStatistics()
      DataSourceV2Relation.transformV2Stats(statistics, conf.defaultSizeInBytes, output)
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }

  private val stringArgsVal: Seq[Any] = {
    val qualifiedTableName = (catalog, identifier) match {
      case (Some(cat), Some(ident)) => Some(s"${cat.name()}.${ident.toString}")
      case _ => None
    }

    Seq(output, qualifiedTableName, scan, stream, startOffset, endOffset)
  }

  override protected def stringArgs: Iterator[Any] = stringArgsVal.iterator
}

object ExtractV2Table {
  def unapply(relation: DataSourceV2Relation): Option[Table] = Some(relation.table)
}

object ExtractV2CatalogAndIdentifier {
  def unapply(relation: DataSourceV2Relation): Option[(TableCatalog, Identifier)] = {
    relation match {
      case DataSourceV2Relation(_, _, Some(catalog), Some(identifier), _, _) =>
        Some((catalog.asTableCatalog, identifier))
      case _ =>
        None
    }
  }
}

object ExtractV2Scan {
  def unapply(scanRelation: DataSourceV2ScanRelation): Option[Scan] =
    Some(scanRelation.scan)
}

object ExtractV2ScanInfo {
  def unapply(scanRelation: DataSourceV2ScanRelation)
      : Option[(DataSourceV2Relation, Scan, Seq[AttributeReference])] =
    Some((scanRelation.relation, scanRelation.scan, scanRelation.output))
}

object DataSourceV2Relation {

  private val EMPTY_V2_COLUMN_STATS =
    Collections.emptyMap[NamedReference, ColumnStatistics]()

  def create(
      table: Table,
      catalog: Option[CatalogPlugin],
      identifier: Option[Identifier],
      options: CaseInsensitiveStringMap,
      timeTravelSpec: Option[TimeTravelSpec] = None): DataSourceV2Relation = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    // The v2 source may return schema containing char/varchar type. We replace char/varchar
    // with "annotated" string type here as the query engine doesn't support char/varchar yet.
    // We also strip internal metadata that may have leaked onto the table columns, so it does
    // not surface on the relation's output. Column IDs (FIELD_ID_METADATA_KEY) are an exception:
    // although the key is listed in INTERNAL_METADATA_KEYS so that other paths drop it, the
    // column-ID feature deliberately surfaces field IDs on the relation's output, so we keep them.
    val schema = removeInternalMetadata(
      CharVarcharUtils.replaceCharVarcharWithStringInSchema(table.columns.asSchema),
      keepFieldIds = true)
    DataSourceV2Relation(table, toAttributes(schema), catalog, identifier, options, timeTravelSpec)
  }

  def create(
      table: Table,
      catalog: Option[CatalogPlugin],
      identifier: Option[Identifier]): DataSourceV2Relation =
    create(table, catalog, identifier, CaseInsensitiveStringMap.empty)

  /**
   * This is used to transform catalog statistics to data source v2 statistics.
   */
  def v1StatsToV2Stats(
      v1Statistics: CatalogStatistics,
      schema: StructType): V2Statistics = {
    val typeMap = schema.fields.map(f => f.name -> f.dataType).toMap
    val colStatsMap: Map[NamedReference, ColumnStatistics] =
      v1Statistics.colStats.flatMap { case (name, stat) =>
        typeMap.get(name).map { dt =>
          FieldReference.column(name) -> v1ColStatToV2ColStat(stat, name, dt)
        }
      }

    val v2SizeInBytes = OptionalLong.of(v1Statistics.sizeInBytes.longValue)
    val v2NumRows = v1Statistics.rowCount
      .map(v => OptionalLong.of(v.longValue)).getOrElse(OptionalLong.empty())
    val v2ColStats = new java.util.HashMap[NamedReference, ColumnStatistics]()
    colStatsMap.foreach { case (k, v) => v2ColStats.put(k, v) }

    new V2Statistics {
      override def sizeInBytes(): OptionalLong = v2SizeInBytes
      override def numRows(): OptionalLong = v2NumRows
      override def columnStats(): java.util.Map[NamedReference, ColumnStatistics] = v2ColStats
    }
  }

  private def v1ColStatToV2ColStat(
      stat: CatalogColumnStat,
      colName: String,
      dataType: DataType): ColumnStatistics = {
    val parsedMin = stat.min.map(
      CatalogColumnStat.fromExternalString(_, colName, dataType, stat.version))
    val parsedMax = stat.max.map(
      CatalogColumnStat.fromExternalString(_, colName, dataType, stat.version))
    val v2DistinctCount =
      stat.distinctCount.map(v => OptionalLong.of(v.longValue)).getOrElse(OptionalLong.empty())
    val v2NullCount =
      stat.nullCount.map(v => OptionalLong.of(v.longValue)).getOrElse(OptionalLong.empty())
    val v2AvgLen = stat.avgLen.map(OptionalLong.of).getOrElse(OptionalLong.empty())
    val v2MaxLen = stat.maxLen.map(OptionalLong.of).getOrElse(OptionalLong.empty())
    val v2Histogram: Optional[V2Histogram] = stat.histogram match {
      case Some(h) =>
        val v2Bins: Array[V2HistogramBin] = h.bins.map { bin =>
          new V2HistogramBin {
            override def lo(): Double = bin.lo
            override def hi(): Double = bin.hi
            override def ndv(): Long = bin.ndv
          }
        }
        Optional.of(new V2Histogram {
          override def height(): Double = h.height
          override def bins(): Array[V2HistogramBin] = v2Bins
        })
      case None => Optional.empty()
    }
    new ColumnStatistics {
      override def distinctCount(): OptionalLong = v2DistinctCount
      override def min(): Optional[Object] = Optional.ofNullable(
        parsedMin.map(_.asInstanceOf[Object]).orNull)
      override def max(): Optional[Object] = Optional.ofNullable(
        parsedMax.map(_.asInstanceOf[Object]).orNull)
      override def nullCount(): OptionalLong = v2NullCount
      override def avgLen(): OptionalLong = v2AvgLen
      override def maxLen(): OptionalLong = v2MaxLen
      override def histogram(): Optional[V2Histogram] = v2Histogram
    }
  }

  /**
   * This is used to transform data source v2 statistics to logical.Statistics.
   */
  def transformV2Stats(
      v2Statistics: V2Statistics,
      defaultSizeInBytes: Long,
      output: Seq[Attribute] = Seq.empty): Statistics = {
    val numRows: Option[BigInt] = if (v2Statistics.numRows().isPresent) {
      Some(v2Statistics.numRows().getAsLong)
    } else {
      None
    }

    var colStats: Seq[(Attribute, ColumnStat)] = Seq.empty[(Attribute, ColumnStat)]
    // columnStats() may be null even when numRows/sizeInBytes are present, so normalize it to an
    // empty map before conversion to avoid an NPE.
    val v2ColumnStats = Option(v2Statistics.columnStats()).getOrElse(EMPTY_V2_COLUMN_STATS)
    if (!v2ColumnStats.isEmpty) {
      val keys = v2ColumnStats.keySet()

      keys.forEach(key => {
        val colStat = v2ColumnStats.get(key)
        val distinct: Option[BigInt] =
          if (colStat.distinctCount().isPresent) Some(colStat.distinctCount().getAsLong) else None
        val min: Option[Any] = if (colStat.min().isPresent) Some(colStat.min().get) else None
        val max: Option[Any] = if (colStat.max().isPresent) Some(colStat.max().get) else None
        val nullCount: Option[BigInt] =
          if (colStat.nullCount().isPresent) Some(colStat.nullCount().getAsLong) else None
        val avgLen: Option[Long] =
          if (colStat.avgLen().isPresent) Some(colStat.avgLen().getAsLong) else None
        val maxLen: Option[Long] =
          if (colStat.maxLen().isPresent) Some(colStat.maxLen().getAsLong) else None
        val histogram = if (colStat.histogram().isPresent) {
          val v2Histogram = colStat.histogram().get()
          val bins = v2Histogram.bins()
          Some(Histogram(v2Histogram.height(),
            bins.map(bin => HistogramBin(bin.lo, bin.hi, bin.ndv))))
        } else {
          None
        }

        val catalystColStat = ColumnStat(distinct, min, max, nullCount, avgLen, maxLen, histogram)

        output.foreach(attribute => {
          if (attribute.name.equals(key.describe())) {
            colStats = colStats :+ (attribute -> catalystColStat)
          }
        })
      })
    }
    val attributeStats = AttributeMap(colStats)
    // Prefer the source-reported size. Otherwise infer a projection-aware size from the row count
    // (numRows * outputRowSize via getOutputSize). Fall back to the default size when neither is
    // available.
    val sizeInBytes = if (v2Statistics.sizeInBytes().isPresent) {
      BigInt(v2Statistics.sizeInBytes().getAsLong)
    } else if (numRows.isDefined) {
      EstimationUtils.getOutputSize(output, numRows.get, attributeStats)
    } else {
      BigInt(defaultSizeInBytes)
    }
    Statistics(
      sizeInBytes = sizeInBytes,
      rowCount = numRows,
      attributeStats = attributeStats)
  }
}
