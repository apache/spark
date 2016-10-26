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
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._


/**
 * A [[FileCatalog]] for a metastore catalog table.
 *
 * @param sparkSession a [[SparkSession]]
 * @param table the metadata of the table
 * @param sizeInBytes the table's data size in bytes
 */
class TableFileCatalog(
    sparkSession: SparkSession,
    val table: CatalogTable,
    override val sizeInBytes: Long) extends FileCatalog {

  protected val hadoopConf = sparkSession.sessionState.newHadoopConf

  private val fileStatusCache = FileStatusCache.newCache(sparkSession)

  assert(table.identifier.database.isDefined,
    "The table identifier must be qualified in TableFileCatalog")

  private val baseLocation = table.storage.locationUri

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
    if (table.partitionColumnNames.nonEmpty) {
      val selectedPartitions = sparkSession.sessionState.catalog.listPartitionsByFilter(
        table.identifier, filters)
      val partitionSchema = table.partitionSchema
      val partitions = selectedPartitions.map { p =>
        PartitionPath(p.toRow(partitionSchema), p.storage.locationUri.get)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      new PrunedTableFileCatalog(
        sparkSession, new Path(baseLocation.get), fileStatusCache, partitionSpec)
    } else {
      new ListingFileCatalog(sparkSession, rootPaths, table.storage.properties, None)
    }
  }

  override def inputFiles: Array[String] = filterPartitions(Nil).inputFiles

  // `TableFileCatalog` may be a member of `HadoopFsRelation`, `HadoopFsRelation` may be a member
  // of `LogicalRelation`, and `LogicalRelation` may be used as the cache key. So we need to
  // implement `equals` and `hashCode` here, to make it work with cache lookup.
  override def equals(o: Any): Boolean = o match {
    case other: TableFileCatalog => this.table.identifier == other.table.identifier
    case _ => false
  }

  override def hashCode(): Int = table.identifier.hashCode()
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
