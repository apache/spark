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

import org.apache.spark.sql.catalyst.TableIdentifier
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
    util.EnumSet.of(TableCapability.GENERAL_TABLE)

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

  def toCatalogTable(catalog: CatalogPlugin, ident: Identifier, t: Table): CatalogTable = {
    if (t.isInstanceOf[V1Table]) {
      return t.asInstanceOf[V1Table].v1Table
    }
    assert(t.capabilities().contains(TableCapability.GENERAL_TABLE))
    val tableType = t.properties().get(TableCatalog.PROP_TABLE_TYPE) match {
      case TableSummary.VIEW_TABLE_TYPE => CatalogTableType.VIEW
      case TableSummary.MANAGED_TABLE_TYPE => CatalogTableType.MANAGED
      case _ => CatalogTableType.EXTERNAL
    }
    val location = Option(t.properties().get(TableCatalog.PROP_LOCATION))
    val viewText = Option(t.properties().get(TableCatalog.PROP_VIEW_TEXT))
    val (serdeProps, tableProps) = t.properties().asScala
      .partition(_._1.startsWith(TableCatalog.OPTION_PREFIX))
    val (partCols, bucketSpec, clusterBySpec) = t.partitioning().toSeq.convertTransforms
    CatalogTable(
      identifier = TableIdentifier(
        table = ident.name(),
        database = Some(ident.namespace().lastOption.getOrElse("root")),
        catalog = Some(catalog.name())),
      tableType = tableType,
      storage = CatalogStorageFormat.empty.copy(
        locationUri = location.map(CatalogUtils.stringToURI),
        // v2 table properties should be put into the serde properties as well in case
        // it contains data source options.
        properties = tableProps.toMap ++ serdeProps.map {
          case (k, v) => k.drop(TableCatalog.OPTION_PREFIX.length) -> v
        }
      ),
      schema = CatalogV2Util.v2ColumnsToStructType(t.columns()),
      provider = Option(t.properties().get(TableCatalog.PROP_PROVIDER)),
      partitionColumnNames = partCols,
      bucketSpec = bucketSpec,
      owner = Option(t.properties().get(TableCatalog.PROP_OWNER)).getOrElse("unknown"),
      viewText = viewText,
      viewOriginalText = viewText,
      comment = Option(t.properties().get(TableCatalog.PROP_COMMENT)),
      collation = Option(t.properties().get(TableCatalog.PROP_COLLATION)),
      properties = tableProps.toMap ++ clusterBySpec.map(ClusterBySpec.toPropertyWithoutValidation)
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
