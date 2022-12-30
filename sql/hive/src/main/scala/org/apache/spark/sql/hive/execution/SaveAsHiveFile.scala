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

package org.apache.spark.sql.hive.execution

import java.io.IOException

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.{FileFormat, FileFormatWriter}

// Base trait from which all hive insert statement physical execution extends.
private[hive] trait SaveAsHiveFile extends DataWritingCommand with V1WritesHiveUtils {

  protected def saveAsHiveFile(
      sparkSession: SparkSession,
      plan: SparkPlan,
      hadoopConf: Configuration,
      fileFormat: FileFormat,
      outputLocation: String,
      customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty,
      partitionAttributes: Seq[Attribute] = Nil,
      bucketSpec: Option[BucketSpec] = None,
      options: Map[String, String] = Map.empty): Set[String] = {

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = outputLocation)

    FileFormatWriter.write(
      sparkSession = sparkSession,
      plan = plan,
      fileFormat = fileFormat,
      committer = committer,
      outputSpec =
        FileFormatWriter.OutputSpec(outputLocation, customPartitionLocations, outputColumns),
      hadoopConf = hadoopConf,
      partitionColumns = partitionAttributes,
      bucketSpec = bucketSpec,
      statsTrackers = Seq(basicWriteJobStatsTracker(hadoopConf)),
      options = options)
  }

  protected def deleteExternalTmpPath(dir: Path, hadoopConf: Configuration) : Unit = {
    // Attempt to delete the staging directory and the inclusive files. If failed, the files are
    // expected to be dropped at the normal termination of VM since deleteOnExit is used.
    try {
      val fs = dir.getFileSystem(hadoopConf)
      if (fs.delete(dir, true)) {
        // If we successfully delete the staging directory, remove it from FileSystem's cache.
        fs.cancelDeleteOnExit(dir)
      }
    } catch {
      case NonFatal(e) =>
        val stagingDir = hadoopConf.get("hive.exec.stagingdir", ".hive-staging")
        logWarning(s"Unable to delete staging directory: $stagingDir.\n" + e)
    }
  }

  protected def createExternalTmpPath(dir: Path, hadoopConf: Configuration): Unit = {
    val fs: FileSystem = dir.getFileSystem(hadoopConf)
    try {
      if (!FileUtils.mkdir(fs, dir, true, hadoopConf)) {
        throw new IllegalStateException("Cannot create staging directory  '" + dir.toString + "'")
      }
      fs.deleteOnExit(dir)
    } catch {
      case e: IOException =>
        throw QueryExecutionErrors.cannotCreateStagingDirError(
          s"'${dir.toString}': ${e.getMessage}", e)
    }
  }
}

