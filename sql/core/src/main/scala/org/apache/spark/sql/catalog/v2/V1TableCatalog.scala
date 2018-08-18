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

package org.apache.spark.sql.catalog.v2

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalog.v2.PartitionTransforms.{Bucket, Identity}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.sources.v2.{ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

/**
 * A [[TableCatalog]] that translates calls to a v1 SessionCatalog.
 */
class V1TableCatalog(sessionState: SessionState) extends TableCatalog {

  private lazy val catalog: SessionCatalog = sessionState.catalog

  override def loadTable(
      ident: TableIdentifier): Table = {
    val catalogTable = catalog.getTableMetadata(ident)

    catalogTable.provider match {
      case Some(provider) =>
        DataSource.lookupDataSource(provider, sessionState.conf).newInstance() match {
          case v2Source: ReadSupport if v2Source.isInstanceOf[WriteSupport] =>
            new V1MetadataTable(catalogTable, Some(v2Source))
                with DelegateReadSupport with DelegateWriteSupport

          case v2Source: ReadSupport =>
            new V1MetadataTable(catalogTable, Some(v2Source)) with DelegateReadSupport

          case v2Source: WriteSupport =>
            new V1MetadataTable(catalogTable, Some(v2Source)) with DelegateWriteSupport

          case _ =>
            new V1MetadataTable(catalogTable, None)
        }

      case _ =>
        new V1MetadataTable(catalogTable, None)
    }
  }

  override def createTable(ident: TableIdentifier,
      schema: StructType,
      partitions: util.List[PartitionTransform],
      properties: util.Map[String, String]): Table = {
    val (partitionColumns, maybeBucketSpec) = convertTransforms(partitions.asScala)
    val source = properties.getOrDefault("provider", sessionState.conf.defaultDataSourceName)
    val tableProperties = properties.asScala
    val storage = DataSource.buildStorageFormatFromOptions(tableProperties.toMap)

    val tableDesc = CatalogTable(
      identifier = ident.copy(
        database = Some(ident.database.getOrElse(sessionState.catalog.getCurrentDatabase))),
      tableType = CatalogTableType.MANAGED,
      storage = storage,
      schema = schema,
      provider = Some(source),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = tableProperties.toMap,
      tracksPartitionsInCatalog = sessionState.conf.manageFilesourcePartitions)

    catalog.createTable(tableDesc, ignoreIfExists = false, validateLocation = false)

    loadTable(ident)
  }

  override def alterTable(ident: TableIdentifier,
      changes: util.List[TableChange]): Table = {
    throw new UnsupportedOperationException("Alter table is not supported for this source")
  }

  /**
   * Drop a table in the catalog.
   *
   * @param ident a table identifier
   * @return true if a table was deleted, false if no table exists for the identifier
   */
  override def dropTable(ident: TableIdentifier): Boolean = {
    try {
      if (loadTable(ident) != null) {
        catalog.dropTable(ident, ignoreIfNotExists = true, purge = true /* skip HDFS trash */)
        true
      } else {
        false
      }
    } catch {
      case _: NoSuchTableException =>
        false
    }
  }

  override def initialize(options: CaseInsensitiveStringMap): Unit = {
    // do nothing.
  }

  private def convertTransforms(
      partitions: Seq[PartitionTransform]): (Seq[String], Option[BucketSpec]) = {
    val (identityTransforms, bucketTransforms) = partitions.partition(_.isInstanceOf[Identity])

    val nonBucketTransforms = bucketTransforms.filterNot(_.isInstanceOf[Bucket])
    if (nonBucketTransforms.nonEmpty) {
      throw new UnsupportedOperationException("SessionCatalog does not support partition " +
          s"transforms: ${nonBucketTransforms.mkString(", ")}")
    }

    val bucketSpec = bucketTransforms.size match {
      case 0 =>
        None
      case 1 =>
        val bucket = bucketTransforms.head.asInstanceOf[Bucket]
        Some(BucketSpec(bucket.numBuckets, bucket.references, Nil))
      case _ =>
        throw new UnsupportedOperationException("SessionCatalog does not support multiple " +
            s"clusterings: ${bucketTransforms.mkString(", ")}")
    }

    val identityCols = identityTransforms.map(_.references.head)

    (identityCols, bucketSpec)
  }
}
