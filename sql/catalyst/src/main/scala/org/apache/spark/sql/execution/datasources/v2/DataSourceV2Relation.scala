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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, ExposesMetadataColumns, Histogram, HistogramBin, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, truncatedString, CharVarcharUtils}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, FunctionCatalog, Identifier, SupportsMetadataColumns, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, Statistics => V2Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.{Offset, SparkDataStream}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
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
    options: CaseInsensitiveStringMap)
  extends LeafNode with MultiInstanceRelation with NamedRelation {

  import DataSourceV2Implicits._

  lazy val funCatalog: Option[FunctionCatalog] = catalog.collect {
    case c: FunctionCatalog => c
  }

  override def name: String = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    (catalog, identifier) match {
      case (Some(cat), Some(ident)) => s"${quoteIfNeeded(cat.name())}.${ident.quoted}"
      case _ => table.name()
    }
  }

  override def skipSchemaResolution: Boolean = table.supports(TableCapability.ACCEPT_ANY_SCHEMA)

  override def simpleString(maxFields: Int): String = {
    val qualifiedTableName = (catalog, identifier) match {
      case (Some(cat), Some(ident)) => s"${cat.name()}.${ident.toString}"
      case _ => ""
    }
    s"RelationV2${truncatedString(output, "[", ", ", "]", maxFields)} $qualifiedTableName $name"
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
          DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes, output)
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
    options: CaseInsensitiveStringMap)
  extends DataSourceV2RelationBase(table, output, catalog, identifier, options)
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
      DataSourceV2Relation(table, output ++ newMetadata, catalog, identifier, options)
    } else {
      this
    }
  }
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
 */
case class DataSourceV2ScanRelation(
    relation: DataSourceV2Relation,
    scan: Scan,
    output: Seq[AttributeReference],
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    ordering: Option[Seq[SortOrder]] = None) extends LeafNode with NamedRelation {

  override def name: String = relation.name

  override def simpleString(maxFields: Int): String = {
    s"RelationV2${truncatedString(output, "[", ", ", "]", maxFields)} $name"
  }

  override def computeStats(): Statistics = {
    scan match {
      case r: SupportsReportStatistics =>
        val statistics = r.estimateStatistics()
        DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes, output)
      case _ =>
        Statistics(sizeInBytes = conf.defaultSizeInBytes)
    }
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
    metadataPath: String)
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
      DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes, output)
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

object DataSourceV2Relation {
  def create(
      table: Table,
      catalog: Option[CatalogPlugin],
      identifier: Option[Identifier],
      options: CaseInsensitiveStringMap): DataSourceV2Relation = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    // The v2 source may return schema containing char/varchar type. We replace char/varchar
    // with "annotated" string type here as the query engine doesn't support char/varchar yet.
    val schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(table.columns.asSchema)
    DataSourceV2Relation(table, toAttributes(schema), catalog, identifier, options)
  }

  def create(
      table: Table,
      catalog: Option[CatalogPlugin],
      identifier: Option[Identifier]): DataSourceV2Relation =
    create(table, catalog, identifier, CaseInsensitiveStringMap.empty)

  /**
   * This is used to transform data source v2 statistics to logical.Statistics.
   */
  def transformV2Stats(
      v2Statistics: V2Statistics,
      defaultRowCount: Option[BigInt],
      defaultSizeInBytes: Long,
      output: Seq[Attribute] = Seq.empty): Statistics = {
    val numRows: Option[BigInt] = if (v2Statistics.numRows().isPresent) {
      Some(v2Statistics.numRows().getAsLong)
    } else {
      defaultRowCount
    }

    var colStats: Seq[(Attribute, ColumnStat)] = Seq.empty[(Attribute, ColumnStat)]
    if (!v2Statistics.columnStats().isEmpty) {
      val v2ColumnStat = v2Statistics.columnStats()
      val keys = v2ColumnStat.keySet()

      keys.forEach(key => {
        val colStat = v2ColumnStat.get(key)
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
    Statistics(
      sizeInBytes = v2Statistics.sizeInBytes().orElse(defaultSizeInBytes),
      rowCount = numRows,
      attributeStats = AttributeMap(colStats))
  }
}
