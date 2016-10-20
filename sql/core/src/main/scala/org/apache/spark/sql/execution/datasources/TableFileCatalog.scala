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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType


/**
 * A [[FileCatalog]] for a metastore catalog table.
 *
 * @param sparkSession a [[SparkSession]]
 * @param db the table's database name
 * @param table the table's (unqualified) name
 * @param partitionSchema the schema of a partitioned table's partition columns
 * @param sizeInBytes the table's data size in bytes
 * @param fileStatusCache optional cache implementation to use for file listing
 */
class TableFileCatalog(
    sparkSession: SparkSession,
    val db: String,
    val table: String,
    override val partitionSchema: StructType,
    override val sizeInBytes: Long) extends FileCatalog {

  protected val hadoopConf = sparkSession.sessionState.newHadoopConf

  private val fileStatusCache = FileStatusCache.getOrInitializeShared(new Object(), sparkSession)

  private val externalCatalog = sparkSession.sharedState.externalCatalog

  private val catalogTable = externalCatalog.getTable(db, table)

  private val baseLocation = if (catalogTable.provider == Some("hive")) {
    catalogTable.storage.locationUri
  } else {
    new CaseInsensitiveMap(catalogTable.storage.properties).get("path")
  }

  override def rootPaths: Seq[Path] = baseLocation.map(new Path(_)).toSeq

  override def listFiles(filters: Seq[Expression]): Seq[PartitionDirectory] = {
    filterPartitions(filters).listFiles(Nil)
  }

  override def refresh(): Unit = fileStatusCache.invalidateAll()

  /**
   * Returns a [[ListingFileCatalog]] for this table restricted to the subset of partitions
   * specified by the given partition-pruning filters.
   *
   * @param filters partition-pruning filters
   */
  def filterPartitions(filters: Seq[Expression]): ListingFileCatalog = {
    val parameters = baseLocation
      .map(loc => Map(PartitioningAwareFileCatalog.BASE_PATH_PARAM -> loc))
      .getOrElse(Map.empty)
    partitionSchema match {
      case schema if schema.nonEmpty =>
        val selectedPartitions = externalCatalog.listPartitionsByFilter(db, table, filters)
        val partitions = selectedPartitions.map { p =>
          PartitionPath(p.toRow(schema), p.storage.locationUri.get)
        }
        val partitionSpec = PartitionSpec(schema, partitions)
        new PrunedTableFileCatalog(
          sparkSession, new Path(baseLocation.get), fileStatusCache, partitionSpec)
      case _ =>
        new ListingFileCatalog(sparkSession, rootPaths, parameters, None, fileStatusCache)
    }
  }

  override def equals(o: Any): Boolean = o match {
    case other: TableFileCatalog => this.db == other.db && this.table == other.table
    case _ => false
  }

  override def hashCode(): Int = 31 * db.hashCode + table.hashCode

  override def inputFiles: Array[String] = filterPartitions(Nil).inputFiles
}

/**
 * An override of the standard HDFS listing based catalog, that overrides the partition spec with
 * the information from the metastore.
 *
 * @param tableBasePath The default base path of the Hive metastore table
 * @param partitionSpec The partition specifications from Hive metastore
 */
private class PrunedTableFileCatalog(
    sparkSession: SparkSession,
    tableBasePath: Path,
    fileStatusCache: FileStatusCache,
    override val partitionSpec: PartitionSpec)
  extends ListingFileCatalog(
    sparkSession,
    partitionSpec.partitions.map(_.path),
    Map.empty,
    Some(partitionSpec.partitionColumns),
    fileStatusCache)
