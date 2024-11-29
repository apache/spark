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

import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.TableIdentifierHelper
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
    import CatalogV2Implicits._
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

    v1Table.properties ++
      v1Table.storage.properties.map { case (key, value) =>
        TableCatalog.OPTION_PREFIX + key -> value } ++
      v1Table.provider.map(TableCatalog.PROP_PROVIDER -> _) ++
      v1Table.comment.map(TableCatalog.PROP_COMMENT -> _) ++
      v1Table.storage.locationUri.map { loc =>
        TableCatalog.PROP_LOCATION -> CatalogUtils.URIToString(loc)
      } ++
      (if (managed) Some(TableCatalog.PROP_IS_MANAGED_LOCATION -> "true") else None) ++
      (if (external) Some(TableCatalog.PROP_EXTERNAL -> "true") else None) ++
      Some(TableCatalog.PROP_OWNER -> v1Table.owner)
  }
}

/**
 * A V2 table with V1 fallback support. This is used to fallback to V1 table when the V2 one
 * doesn't implement specific capabilities but V1 already has.
 */
private[sql] trait V2TableWithV1Fallback extends Table {
  def v1Table: CatalogTable
}
