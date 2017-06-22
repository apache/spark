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
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.{FileFormat, FileFormatWriter}

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
        try {
          val hdfsPath = new Path(singlePath)
          val fs = hdfsPath.getFileSystem(hadoopConf)
          val metadataPath = new Path(hdfsPath, metadataDir)
          val res = fs.exists(metadataPath)
          res
        } catch {
          case NonFatal(e) =>
            logWarning(s"Error while looking for metadata directory.")
            false
        }
      case _ => false
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

  private val basePath = new Path(path)
  private val logPath = new Path(basePath, FileStreamSink.metadataDir)
  private val fileLog =
    new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, logPath.toUri.toString)
  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val committer = FileCommitProtocol.instantiate(
        className = sparkSession.sessionState.conf.streamingFileCommitProtocolClass,
        jobId = batchId.toString,
        outputPath = path,
        isAppend = false)

      committer match {
        case manifestCommitter: ManifestFileCommitProtocol =>
          manifestCommitter.setupManifestOptions(fileLog, batchId)
        case _ =>  // Do nothing
      }

      FileFormatWriter.write(
        sparkSession = sparkSession,
        queryExecution = data.queryExecution,
        fileFormat = fileFormat,
        committer = committer,
        outputSpec = FileFormatWriter.OutputSpec(path, Map.empty),
        hadoopConf = hadoopConf,
        partitionColumnNames = partitionColumnNames,
        bucketSpec = None,
        refreshFunction = _ => (),
        options = options)
    }
  }

  override def toString: String = s"FileSink[$path]"
}
