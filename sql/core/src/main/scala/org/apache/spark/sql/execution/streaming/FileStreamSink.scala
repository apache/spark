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

package org.apache.spark.sql.execution.streaming

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormat, FileFormatWriter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SerializableConfiguration

object FileStreamSink extends Logging {
  // The name of the subdirectory that is used to store metadata about which files are valid.
  val metadataDir = "_spark_metadata"

  /**
   * Returns true if there is a single path that has a metadata log indicating which files should
   * be read.
   */
  def hasMetadata(path: Seq[String], hadoopConf: Configuration): Boolean = {
    path match {
      case Seq(singlePath) =>
        val hdfsPath = new Path(singlePath)
        val fs = hdfsPath.getFileSystem(hadoopConf)
        if (fs.isDirectory(hdfsPath)) {
          val metadataPath = new Path(hdfsPath, metadataDir)
          checkEscapedMetadataPath(fs, metadataPath)
          fs.exists(metadataPath)
        } else {
          false
        }
      case _ => false
    }
  }

  def checkEscapedMetadataPath(fs: FileSystem, metadataPath: Path): Unit = {
    if (metadataPath.toUri != new Path(metadataPath.toUri.toString).toUri) {
      val oldMetadataPath = new Path(metadataPath.toUri.toString)
      if (fs.exists(oldMetadataPath)) {
        throw new SparkException(s"Found $oldMetadataPath. In Spark 2.4 or prior, the " +
          s"file sink metadata will be written to $oldMetadataPath when using " +
          s"$metadataPath as the output path. We detected that $oldMetadataPath exists " +
          s"but not sure whether we should resume your query using this metadata path. " +
          s"If you would like to resume from $oldMetadataPath, please *move* all files " +
          s"in $oldMetadataPath to $metadataPath and rerun your codes. If " +
          s"$oldMetadataPath is not related to the query, you can set SQL conf " +
          s"'${SQLConf.STREAMING_CHECKPOINT_ESCAPED_PATH_CHECK_ENABLED.key}' to false to " +
          s"turn off this check, or delete $oldMetadataPath.")
      }
    }
  }

  /**
   * Returns true if the path is the metadata dir or its ancestor is the metadata dir.
   * E.g.:
   *  - ancestorIsMetadataDirectory(/.../_spark_metadata) => true
   *  - ancestorIsMetadataDirectory(/.../_spark_metadata/0) => true
   *  - ancestorIsMetadataDirectory(/a/b/c) => false
   */
  def ancestorIsMetadataDirectory(path: Path, hadoopConf: Configuration): Boolean = {
    val fs = path.getFileSystem(hadoopConf)
    var currentPath = path.makeQualified(fs.getUri, fs.getWorkingDirectory)
    while (currentPath != null) {
      if (currentPath.getName == FileStreamSink.metadataDir) {
        return true
      } else {
        currentPath = currentPath.getParent
      }
    }
    return false
  }
}

/**
 * A sink that writes out results to parquet files.  Each batch is written out to a unique
 * directory. After all of the files in a batch have been successfully written, the list of
 * file paths is appended to the log atomically. In the case of partial failures, some duplicate
 * data may be present in the target directory, but only one copy of each file will be present
 * in the log.
 */
class FileStreamSink(
    sparkSession: SparkSession,
    path: String,
    fileFormat: FileFormat,
    partitionColumnNames: Seq[String],
    options: Map[String, String]) extends Sink with Logging {

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()
  private val basePath = new Path(path)
  private val logPath = {
    val metadataDir = new Path(basePath, FileStreamSink.metadataDir)
    if (metadataDir.toUri != new Path(metadataDir.toUri.toString).toUri) {
      val oldMetadataDir = new Path(metadataDir.toUri.toString)
      val fs = oldMetadataDir.getFileSystem(sparkSession.sessionState.newHadoopConf())
      if (fs.exists(oldMetadataDir)) {
        throw new SparkException(s"Found $oldMetadataDir. In Spark 2.4 or prior, the " +
          s"file sink metadata will be written to $oldMetadataDir when using $metadataDir as " +
          s"the output path. We detected that $oldMetadataDir exists but not sure " +
          s"whether we should resume your query using this metadata path. If you would like to " +
          s"resume from $oldMetadataDir, please *move* all files in $oldMetadataDir to " +
          s"$metadataDir and rerun your codes. If $oldMetadataDir is not related to the " +
          s"query, you can set SQL conf " +
          s"'${SQLConf.STREAMING_CHECKPOINT_ESCAPED_PATH_CHECK_ENABLED.key}' to false to turn " +
          s"off this check, or delete $oldMetadataDir.")
      }
    }
    metadataDir
  }
  private val fileLog =
    new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, logPath.toString)

  private def basicWriteJobStatsTracker: BasicWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    new BasicWriteJobStatsTracker(serializableHadoopConf, BasicWriteJobStatsTracker.metrics)
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val committer = FileCommitProtocol.instantiate(
        className = sparkSession.sessionState.conf.streamingFileCommitProtocolClass,
        jobId = batchId.toString,
        outputPath = path)

      committer match {
        case manifestCommitter: ManifestFileCommitProtocol =>
          manifestCommitter.setupManifestOptions(fileLog, batchId)
        case _ =>  // Do nothing
      }

      // Get the actual partition columns as attributes after matching them by name with
      // the given columns names.
      val partitionColumns: Seq[Attribute] = partitionColumnNames.map { col =>
        val nameEquality = data.sparkSession.sessionState.conf.resolver
        data.logicalPlan.output.find(f => nameEquality(f.name, col)).getOrElse {
          throw new RuntimeException(s"Partition column $col not found in schema ${data.schema}")
        }
      }
      val qe = data.queryExecution

      FileFormatWriter.write(
        sparkSession = sparkSession,
        plan = qe.executedPlan,
        fileFormat = fileFormat,
        committer = committer,
        outputSpec = FileFormatWriter.OutputSpec(path, Map.empty, qe.analyzed.output),
        hadoopConf = hadoopConf,
        partitionColumns = partitionColumns,
        bucketSpec = None,
        statsTrackers = Seq(basicWriteJobStatsTracker),
        options = options)
    }
  }

  override def toString: String = s"FileSink[$path]"
}
