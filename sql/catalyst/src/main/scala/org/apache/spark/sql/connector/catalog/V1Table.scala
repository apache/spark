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

package org.apache.spark.sql.connector.catalog

import java.util

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils, ClusterBySpec}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.V1Table.addV2TableProperties
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, Transform}
import org.apache.spark.sql.types.StructType

/**
 * An implementation of catalog v2 `Table` to expose v1 table metadata.
 */
private[sql] case class V1Table(v1Table: CatalogTable) extends Table {

  def catalogTable: CatalogTable = v1Table

  lazy val options: Map[String, String] = {
    v1Table.storage.locationUri match {
      case Some(uri) =>
        v1Table.storage.properties + ("path" -> CatalogUtils.URIToString(uri))
      case _ =>
        v1Table.storage.properties
    }
  }

  override lazy val properties: util.Map[String, String] = addV2TableProperties(v1Table).asJava

  override lazy val schema: StructType = v1Table.schema

  override lazy val partitioning: Array[Transform] = {
    val partitions = new mutable.ArrayBuffer[Transform]()

    v1Table.partitionColumnNames.foreach { col =>
      partitions += LogicalExpressions.identity(LogicalExpressions.reference(Seq(col)))
    }

    v1Table.bucketSpec.foreach { spec =>
      partitions += spec.asTransform
    }

    v1Table.clusterBySpec.foreach { spec =>
      partitions += spec.asTransform
    }

    partitions.toArray
  }

  override def name: String = v1Table.identifier.quoted

  override def capabilities: util.Set[TableCapability] =
    util.EnumSet.noneOf(classOf[TableCapability])

  override def toString: String = s"V1Table($name)"
}

private[sql] object V1Table {
  def addV2TableProperties(v1Table: CatalogTable): Map[String, String] = {
    val external = v1Table.tableType == CatalogTableType.EXTERNAL
    val managed = v1Table.tableType == CatalogTableType.MANAGED
    val tableTypeProperties: Option[(String, String)] = getV2TableType(v1Table)
        .map(tableType => TableCatalog.PROP_TABLE_TYPE -> tableType)

    v1Table.properties ++
      v1Table.storage.properties.map { case (key, value) =>
        TableCatalog.OPTION_PREFIX + key -> value } ++
      v1Table.provider.map(TableCatalog.PROP_PROVIDER -> _) ++
      v1Table.comment.map(TableCatalog.PROP_COMMENT -> _) ++
      v1Table.collation.map(TableCatalog.PROP_COLLATION -> _) ++
      v1Table.storage.locationUri.map { loc =>
        TableCatalog.PROP_LOCATION -> CatalogUtils.URIToString(loc)
      } ++
      (if (managed) Some(TableCatalog.PROP_IS_MANAGED_LOCATION -> "true") else None) ++
      (if (external) Some(TableCatalog.PROP_EXTERNAL -> "true") else None) ++
      tableTypeProperties ++
      Some(TableCatalog.PROP_OWNER -> v1Table.owner)
  }

  /**
   * Returns v2 table type that should be part of v2 table properties.
   * If there is no mapping between v1 table type and v2 table type, then None is returned.
   */
  private def getV2TableType(v1Table: CatalogTable): Option[String] = {
    v1Table.tableType match {
      case CatalogTableType.EXTERNAL => Some(TableSummary.EXTERNAL_TABLE_TYPE)
      case CatalogTableType.MANAGED => Some(TableSummary.MANAGED_TABLE_TYPE)
      case CatalogTableType.VIEW => Some(TableSummary.VIEW_TABLE_TYPE)
      case _ => None
    }
  }

  def toCatalogTable(
      catalog: CatalogPlugin,
      ident: Identifier,
      t: MetadataOnlyTable): CatalogTable = t.getTableInfo match {
    case viewInfo: ViewInfo => toCatalogTable(catalog, ident, viewInfo)
    case tableInfo => toCatalogTable(catalog, ident, tableInfo)
  }

  private def toCatalogTable(
      catalog: CatalogPlugin,
      ident: Identifier,
      info: TableInfo): CatalogTable = {
    val props = info.properties.asScala.toMap
    // PROP_TABLE_TYPE is advisory on the v2 side: it may be absent or carry a value that has no
    // v1 mapping (e.g. TableSummary.FOREIGN_TABLE_TYPE). v1 only has EXTERNAL/MANAGED, so
    // anything other than the explicit MANAGED mapping falls back to EXTERNAL for the v1
    // representation -- the same default v1 uses when the value is missing. VIEW is reached
    // only through the ViewInfo branch above.
    val tableType = props.get(TableCatalog.PROP_TABLE_TYPE) match {
      case Some(TableSummary.MANAGED_TABLE_TYPE) => CatalogTableType.MANAGED
      case _ => CatalogTableType.EXTERNAL
    }
    // Reserved keys are promoted to first-class CatalogTable fields; strip them from the
    // user-visible properties map so they're not double-persisted or leaked into the serde bag.
    val userProps = props -- CatalogV2Util.TABLE_RESERVED_PROPERTIES
    val (serdeProps, tableProps) = userProps.toSeq
      .partition(_._1.startsWith(TableCatalog.OPTION_PREFIX))
    val tablePropsMap = tableProps.toMap
    val (partCols, bucketSpec, clusterBySpec) = info.partitions.toSeq.convertTransforms
    CatalogTable(
      // `asLegacyTableIdentifier` collapses multi-part namespaces to their last segment (v1
      // limitation). We record the full multi-part form in `multipartIdentifier` below;
      // callers needing the real fully-qualified name should read `CatalogTable.fullIdent`.
      identifier = ident.asLegacyTableIdentifier(catalog.name()),
      tableType = tableType,
      storage = CatalogStorageFormat.empty.copy(
        locationUri = props.get(TableCatalog.PROP_LOCATION).map(CatalogUtils.stringToURI),
        // v2 table properties should be put into the serde properties as well in case
        // they contain data source options.
        properties = tablePropsMap ++ serdeProps.map {
          case (k, v) => k.drop(TableCatalog.OPTION_PREFIX.length) -> v
        }
      ),
      schema = CatalogV2Util.v2ColumnsToStructType(info.columns),
      provider = props.get(TableCatalog.PROP_PROVIDER),
      partitionColumnNames = partCols,
      bucketSpec = bucketSpec,
      owner = props.getOrElse(TableCatalog.PROP_OWNER, ""),
      comment = props.get(TableCatalog.PROP_COMMENT),
      collation = props.get(TableCatalog.PROP_COLLATION),
      properties = tablePropsMap ++
        clusterBySpec.map(ClusterBySpec.toPropertyWithoutValidation),
      multipartIdentifier = Some(catalog.name() +: ident.asMultipartIdentifier)
    )
  }

  def toCatalogTable(
      catalog: CatalogPlugin,
      ident: Identifier,
      info: ViewInfo): CatalogTable = {
    val props = info.properties.asScala.toMap
    val userProps = props -- CatalogV2Util.TABLE_RESERVED_PROPERTIES
    // Serde/OPTION properties only apply to data-source tables; views' user properties are a
    // plain TBLPROPERTIES bag.
    val tablePropsMap = userProps
    val viewContextProps = if (info.currentCatalog != null && info.currentCatalog.nonEmpty) {
      CatalogTable.catalogAndNamespaceToProps(
        info.currentCatalog, info.currentNamespace.toSeq)
    } else {
      Map.empty[String, String]
    }
    val sqlConfigProps = info.sqlConfigs.asScala.map {
      case (k, v) => s"${CatalogTable.VIEW_SQL_CONFIG_PREFIX}$k" -> v
    }.toMap
    val queryOutputProps = if (info.queryColumnNames.isEmpty) {
      Map.empty[String, String]
    } else {
      val numCols = info.queryColumnNames.length
      val perColProps = info.queryColumnNames.zipWithIndex.map { case (name, idx) =>
        s"${CatalogTable.VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX}$idx" -> name
      }.toMap
      perColProps + (CatalogTable.VIEW_QUERY_OUTPUT_NUM_COLUMNS -> numCols.toString)
    }
    val schemaModeProps = Option(info.schemaMode)
      .map(m => Map(CatalogTable.VIEW_SCHEMA_MODE -> m))
      .getOrElse(Map.empty)
    CatalogTable(
      identifier = ident.asLegacyTableIdentifier(catalog.name()),
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = CatalogV2Util.v2ColumnsToStructType(info.columns),
      owner = props.getOrElse(TableCatalog.PROP_OWNER, ""),
      viewText = Some(info.queryText),
      viewOriginalText = Some(info.queryText),
      comment = props.get(TableCatalog.PROP_COMMENT),
      collation = props.get(TableCatalog.PROP_COLLATION),
      properties = tablePropsMap ++ viewContextProps ++ sqlConfigProps ++
        queryOutputProps ++ schemaModeProps,
      multipartIdentifier = Some(catalog.name() +: ident.asMultipartIdentifier)
    )
  }
}

/**
 * A V2 table with V1 fallback support. This is used to fallback to V1 table when the V2 one
 * doesn't implement specific capabilities but V1 already has.
 */
private[sql] trait V2TableWithV1Fallback extends Table {
  def v1Table: CatalogTable
}
