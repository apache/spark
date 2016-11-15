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
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand

/**
 * A command for writing data to a [[HadoopFsRelation]].  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.
 *
 * @param staticPartitionKeys partial partitioning spec for write. This defines the scope of
 *                            partition overwrites: when the spec is empty, all partitions are
 *                            overwritten. When it covers a prefix of the partition keys, only
 *                            partitions matching the prefix are overwritten.
 * @param customPartitionLocations mapping of partition specs to their custom locations. The
 *                                 caller should guarantee that exactly those table partitions
 *                                 falling under the specified static partition keys are contained
 *                                 in this map, and that no other partitions are.
 */
case class InsertIntoHadoopFsRelationCommand(
    outputPath: Path,
    staticPartitionKeys: TablePartitionSpec,
    customPartitionLocations: Map[TablePartitionSpec, String],
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    refreshFunction: Seq[TablePartitionSpec] => Unit,
    options: Map[String, String],
    @transient query: LogicalPlan,
    mode: SaveMode,
    catalogTable: Option[CatalogTable])
  extends RunnableCommand {

  import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName

  override protected def innerChildren: Seq[LogicalPlan] = query :: Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Most formats don't do well with duplicate columns, so lets not allow that
    if (query.schema.fieldNames.length != query.schema.fieldNames.distinct.length) {
      val duplicateColumns = query.schema.fieldNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }.mkString(", ")
      throw new AnalysisException(s"Duplicate column(s) : $duplicateColumns found, " +
          s"cannot save to file.")
    }

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val pathExists = fs.exists(qualifiedOutputPath)
    val doInsertion = (mode, pathExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        throw new AnalysisException(s"path $qualifiedOutputPath already exists.")
      case (SaveMode.Overwrite, true) =>
        deleteMatchingPartitions(fs, qualifiedOutputPath)
        true
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        true
      case (SaveMode.Ignore, exists) =>
        !exists
      case (s, exists) =>
        throw new IllegalStateException(s"unsupported save mode $s ($exists)")
    }
    // If we are appending data to an existing dir.
    val isAppend = pathExists && (mode == SaveMode.Append)

    if (doInsertion) {
      val committer = FileCommitProtocol.instantiate(
        sparkSession.sessionState.conf.fileCommitProtocolClass,
        jobId = java.util.UUID.randomUUID().toString,
        outputPath = outputPath.toString,
        isAppend = isAppend)

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
        refreshFunction = refreshFunction,
        options = options)
    } else {
      logInfo("Skipping insertion into a relation that already exists.")
    }

    Seq.empty[Row]
  }

  /**
   * Deletes all partition files that match the specified static prefix. Partitions with custom
   * locations are also cleared based on the custom locations map given to this class.
   */
  private def deleteMatchingPartitions(fs: FileSystem, qualifiedOutputPath: Path): Unit = {
    val staticPartitionPrefix = if (staticPartitionKeys.nonEmpty) {
      "/" + partitionColumns.flatMap { p =>
        staticPartitionKeys.get(p.name) match {
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
    if (fs.exists(staticPrefixPath) && !fs.delete(staticPrefixPath, true /* recursively */)) {
      throw new IOException(s"Unable to clear output " +
        s"directory $staticPrefixPath prior to writing to it")
    }
    // now clear all custom partition locations (e.g. /custom/dir/where/foo=2/bar=4)
    for ((spec, customLoc) <- customPartitionLocations) {
      assert(
        (staticPartitionKeys.toSet -- spec).isEmpty,
        "Custom partition location did not match static partitioning keys")
      val path = new Path(customLoc)
      if (fs.exists(path) && !fs.delete(path, true)) {
        throw new IOException(s"Unable to clear partition " +
          s"directory $path prior to writing to it")
      }
    }
  }
}
