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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StructField, StructType}


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

  override def rootPaths: Seq[Path] = baseLocation.map(new Path(_)).toSeq

  override def listFiles(filters: Seq[Expression]): Seq[Partition] = partitionSchema match {
    case Some(partitionSchema) =>
      externalCatalog.listPartitionsByFilter(db, table, filters).flatMap {
        case CatalogTablePartition(spec, storage, _) =>
          storage.locationUri.map(new Path(_)).map { path =>
            val files = listDataLeafFiles(path :: Nil).toSeq
            val values =
              InternalRow.fromSeq(partitionSchema.map { case StructField(name, dataType, _, _) =>
                Cast(Literal(spec(name)), dataType).eval()
              })
            Partition(values, files)
          }
      }
    case None =>
      Partition(InternalRow.empty, listDataLeafFiles(rootPaths).toSeq) :: Nil
  }

  override def refresh(): Unit = {}


  /**
   * Returns a [[ListingFileCatalog]] for this table restricted to the subset of partitions
   * specified by the given partition-pruning filters.
   *
   * @param filters partition-pruning filters
   */
  def filterPartitions(filters: Seq[Expression]): ListingFileCatalog = {
    val rootPaths = partitionSchema match {
      case Some(_) =>
        externalCatalog
          .listPartitionsByFilter(db, table, filters)
          .flatMap(_.storage.locationUri)
          .map(new Path(_))
      case None =>
        this.rootPaths
    }
    val parameters =
      baseLocation
        .map(loc => Map(PartitioningAwareFileCatalog.BASE_PATH_PARAM -> loc))
        .getOrElse(Map.empty)

    new ListingFileCatalog(sparkSession, rootPaths, parameters, partitionSchema)
  }

  private def listDataLeafFiles(paths: Seq[Path]) =
    listLeafFiles(paths).filter(f => isDataPath(f.getPath))
}
