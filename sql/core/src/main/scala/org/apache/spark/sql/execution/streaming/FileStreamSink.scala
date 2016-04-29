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

import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.FileFormat

object FileStreamSink {
  // The name of the subdirectory that is used to store metadata about which files are valid.
  val metadataDir = "_spark_metadata"
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
    fileFormat: FileFormat) extends Sink with Logging {

  private val basePath = new Path(path)
  private val logPath = new Path(basePath, FileStreamSink.metadataDir)
  private val fileLog = new FileStreamSinkLog(sparkSession, logPath.toUri.toString)
  private val fs = basePath.getFileSystem(sparkSession.sessionState.newHadoopConf())

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val files = fs.listStatus(writeFiles(data)).map { f =>
        SinkFileStatus(
          path = f.getPath.toUri.toString,
          size = f.getLen,
          isDir = f.isDirectory,
          modificationTime = f.getModificationTime,
          blockReplication = f.getReplication,
          blockSize = f.getBlockSize,
          action = FileStreamSinkLog.ADD_ACTION)
      }
      if (fileLog.add(batchId, files)) {
        logInfo(s"Committed batch $batchId")
      } else {
        throw new IllegalStateException(s"Race while writing batch $batchId")
      }
    }
  }

  /** Writes the [[DataFrame]] to a UUID-named dir, returning the list of files paths. */
  private def writeFiles(data: DataFrame): Array[Path] = {
    val file = new Path(basePath, UUID.randomUUID().toString).toUri.toString
    data.write.parquet(file)
    sparkSession.read
        .schema(data.schema)
        .parquet(file)
        .inputFiles
        .map(new Path(_))
        .filterNot(_.getName.startsWith("_"))
  }

  override def toString: String = s"FileSink[$path]"
}
