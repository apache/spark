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
 * A [[BasicFileCatalog]] for a metastore catalog table.
 *
 * @param sparkSession a [[SparkSession]]
 * @param db the table's database name
 * @param table the table's (unqualified) name
 * @param partitionSchema the schema of a partitioned table's partition columns
 * @param sizeInBytes the table's data size in bytes
 */
class TableFileCatalog(
    sparkSession: SparkSession,
    db: String,
    table: String,
    partitionSchema: Option[StructType],
    override val sizeInBytes: Long)
  extends SessionFileCatalog(sparkSession) {

  override protected val hadoopConf = sparkSession.sessionState.newHadoopConf

  private val externalCatalog = sparkSession.sharedState.externalCatalog

  private val catalogTable = externalCatalog.getTable(db, table)

  private val baseLocation = catalogTable.storage.locationUri

  // Populated on-demand by calls to cachedAllPartitions
  private var allPartitions: ListingFileCatalog = null

  override def rootPaths: Seq[Path] = baseLocation.map(new Path(_)).toSeq

  override def listFiles(filters: Seq[Expression]): Seq[Partition] = {
    filterPartitions(filters).listFiles(Nil)
  }

  override def refresh(): Unit = synchronized {
    allPartitions = null
  }

  /**
   * Returns a [[ListingFileCatalog]] for this table restricted to the subset of partitions
   * specified by the given partition-pruning filters.
   *
   * @param filters partition-pruning filters
   */
  def filterPartitions(filters: Seq[Expression]): ListingFileCatalog = {
    if (filters.isEmpty) {
      cachedAllPartitions
    } else {
      filterPartitions0(filters)
    }
  }

  private def filterPartitions0(filters: Seq[Expression]): ListingFileCatalog = {
    val parameters = baseLocation
      .map(loc => Map(PartitioningAwareFileCatalog.BASE_PATH_PARAM -> loc))
      .getOrElse(Map.empty)
    partitionSchema match {
      case Some(schema) =>
        val selectedPartitions = externalCatalog.listPartitionsByFilter(db, table, filters)
        val partitions = selectedPartitions.map { p =>
          PartitionDirectory(p.toRow(schema), p.storage.locationUri.get)
        }
        val partitionSpec = PartitionSpec(schema, partitions)
        new PrunedTableFileCatalog(
          sparkSession, new Path(baseLocation.get), partitionSpec)
      case None =>
        new ListingFileCatalog(sparkSession, rootPaths, parameters, None)
    }
  }

  // Not used in the hot path of queries when metastore partition pruning is enabled
  def cachedAllPartitions: ListingFileCatalog = synchronized {
    if (allPartitions == null) {
      allPartitions = filterPartitions0(Nil)
    }
    allPartitions
  }

  override def inputFiles: Array[String] = cachedAllPartitions.inputFiles
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
    override val partitionSpec: PartitionSpec)
  extends ListingFileCatalog(
    sparkSession,
    partitionSpec.partitions.map(_.path),
    Map.empty,
    Some(partitionSpec.partitionColumns))
