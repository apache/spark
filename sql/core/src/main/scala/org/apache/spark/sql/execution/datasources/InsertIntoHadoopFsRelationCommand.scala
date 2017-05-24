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

import java.io.IOException

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._

/**
 * A command for writing data to a [[HadoopFsRelation]].  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.
 *
 * @param staticPartitions partial partitioning spec for write. This defines the scope of partition
 *                         overwrites: when the spec is empty, all partitions are overwritten.
 *                         When it covers a prefix of the partition keys, only partitions matching
 *                         the prefix are overwritten.
 * @param ifPartitionNotExists If true, only write if the partition does not exist.
 *                             Only valid for static partitions.
 */
case class InsertIntoHadoopFsRelationCommand(
    outputPath: Path,
    staticPartitions: TablePartitionSpec,
    ifPartitionNotExists: Boolean,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    options: Map[String, String],
    query: LogicalPlan,
    mode: SaveMode,
    catalogTable: Option[CatalogTable],
    fileIndex: Option[FileIndex])
  extends RunnableCommand {

  import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName

  override protected def innerChildren: Seq[LogicalPlan] = query :: Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Most formats don't do well with duplicate columns, so lets not allow that
    if (query.schema.fieldNames.length != query.schema.fieldNames.distinct.length) {
      val duplicateColumns = query.schema.fieldNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }.mkString(", ")
      throw new AnalysisException(s"Duplicate column(s): $duplicateColumns found, " +
        "cannot save to file.")
    }

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val partitionsTrackedByCatalog = sparkSession.sessionState.conf.manageFilesourcePartitions &&
      catalogTable.isDefined &&
      catalogTable.get.partitionColumnNames.nonEmpty &&
      catalogTable.get.tracksPartitionsInCatalog

    var initialMatchingPartitions: Seq[TablePartitionSpec] = Nil
    var customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty
    var matchingPartitions: Seq[CatalogTablePartition] = Seq.empty

    // When partitions are tracked by the catalog, compute all custom partition locations that
    // may be relevant to the insertion job.
    if (partitionsTrackedByCatalog) {
      matchingPartitions = sparkSession.sessionState.catalog.listPartitions(
        catalogTable.get.identifier, Some(staticPartitions))
      initialMatchingPartitions = matchingPartitions.map(_.spec)
      customPartitionLocations = getCustomPartitionLocations(
        fs, catalogTable.get, qualifiedOutputPath, matchingPartitions)
    }

    val pathExists = fs.exists(qualifiedOutputPath)
    // If we are appending data to an existing dir.
    val isAppend = pathExists && (mode == SaveMode.Append)

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = outputPath.toString,
      isAppend = isAppend)

    val doInsertion = (mode, pathExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        throw new AnalysisException(s"path $qualifiedOutputPath already exists.")
      case (SaveMode.Overwrite, true) =>
        if (ifPartitionNotExists && matchingPartitions.nonEmpty) {
          false
        } else {
          deleteMatchingPartitions(fs, qualifiedOutputPath, customPartitionLocations, committer)
          true
        }
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        true
      case (SaveMode.Ignore, exists) =>
        !exists
      case (s, exists) =>
        throw new IllegalStateException(s"unsupported save mode $s ($exists)")
    }

    if (doInsertion) {

      // Callback for updating metastore partition metadata after the insertion job completes.
      def refreshPartitionsCallback(updatedPartitions: Seq[TablePartitionSpec]): Unit = {
        if (partitionsTrackedByCatalog) {
          val newPartitions = updatedPartitions.toSet -- initialMatchingPartitions
          if (newPartitions.nonEmpty) {
            AlterTableAddPartitionCommand(
              catalogTable.get.identifier, newPartitions.toSeq.map(p => (p, None)),
              ifNotExists = true).run(sparkSession)
          }
          if (mode == SaveMode.Overwrite) {
            val deletedPartitions = initialMatchingPartitions.toSet -- updatedPartitions
            if (deletedPartitions.nonEmpty) {
              AlterTableDropPartitionCommand(
                catalogTable.get.identifier, deletedPartitions.toSeq,
                ifExists = true, purge = false,
                retainData = true /* already deleted */).run(sparkSession)
            }
          }
        }
      }

      FileFormatWriter.write(
        sparkSession = sparkSession,
        queryExecution = Dataset.ofRows(sparkSession, query).queryExecution,
        fileFormat = fileFormat,
        committer = committer,
        outputSpec = FileFormatWriter.OutputSpec(
          qualifiedOutputPath.toString, customPartitionLocations),
        hadoopConf = hadoopConf,
        partitionColumns = partitionColumns,
        bucketSpec = bucketSpec,
        refreshFunction = refreshPartitionsCallback,
        options = options)

      // refresh cached files in FileIndex
      fileIndex.foreach(_.refresh())
      // refresh data cache if table is cached
      sparkSession.catalog.refreshByPath(outputPath.toString)
    } else {
      logInfo("Skipping insertion into a relation that already exists.")
    }

    Seq.empty[Row]
  }

  /**
   * Deletes all partition files that match the specified static prefix. Partitions with custom
   * locations are also cleared based on the custom locations map given to this class.
   */
  private def deleteMatchingPartitions(
      fs: FileSystem,
      qualifiedOutputPath: Path,
      customPartitionLocations: Map[TablePartitionSpec, String],
      committer: FileCommitProtocol): Unit = {
    val staticPartitionPrefix = if (staticPartitions.nonEmpty) {
      "/" + partitionColumns.flatMap { p =>
        staticPartitions.get(p.name) match {
          case Some(value) =>
            Some(escapePathName(p.name) + "=" + escapePathName(value))
          case None =>
            None
        }
      }.mkString("/")
    } else {
      ""
    }
    // first clear the path determined by the static partition keys (e.g. /table/foo=1)
    val staticPrefixPath = qualifiedOutputPath.suffix(staticPartitionPrefix)
    if (fs.exists(staticPrefixPath) && !committer.deleteWithJob(fs, staticPrefixPath, true)) {
      throw new IOException(s"Unable to clear output " +
        s"directory $staticPrefixPath prior to writing to it")
    }
    // now clear all custom partition locations (e.g. /custom/dir/where/foo=2/bar=4)
    for ((spec, customLoc) <- customPartitionLocations) {
      assert(
        (staticPartitions.toSet -- spec).isEmpty,
        "Custom partition location did not match static partitioning keys")
      val path = new Path(customLoc)
      if (fs.exists(path) && !committer.deleteWithJob(fs, path, true)) {
        throw new IOException(s"Unable to clear partition " +
          s"directory $path prior to writing to it")
      }
    }
  }

  /**
   * Given a set of input partitions, returns those that have locations that differ from the
   * Hive default (e.g. /k1=v1/k2=v2). These partitions were manually assigned locations by
   * the user.
   *
   * @return a mapping from partition specs to their custom locations
   */
  private def getCustomPartitionLocations(
      fs: FileSystem,
      table: CatalogTable,
      qualifiedOutputPath: Path,
      partitions: Seq[CatalogTablePartition]): Map[TablePartitionSpec, String] = {
    partitions.flatMap { p =>
      val defaultLocation = qualifiedOutputPath.suffix(
        "/" + PartitioningUtils.getPathFragment(p.spec, table.partitionSchema)).toString
      val catalogLocation = new Path(p.location).makeQualified(
        fs.getUri, fs.getWorkingDirectory).toString
      if (catalogLocation != defaultLocation) {
        Some(p.spec -> catalogLocation)
      } else {
        None
      }
    }.toMap
  }
}
