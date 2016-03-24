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
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources.FileFormat

object FileStreamSink {
  // The name of the subdirectory that is used to store metadata about which files are valid.
  val metadataDir = "_spark_metadata"
}

/**
 * A sink that writes out results to parquet files.  Each batch is written out to a unique
 * directory. After all of the files in a batch have been succesfully written, the list of
 * file paths is appended to the log atomically. In the case of partial failures, some duplicate
 * data may be present in the target directory, but only one copy of each file will be present
 * in the log.
 */
class FileStreamSink(
    sqlContext: SQLContext,
    path: String,
    fileFormat: FileFormat) extends Sink with Logging {

  private val basePath = new Path(path)
  private val logPath = new Path(basePath, FileStreamSink.metadataDir)
  private val fileLog = new HDFSMetadataLog[Seq[String]](sqlContext, logPath.toUri.toString)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (fileLog.get(batchId).isDefined) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val files = writeFiles(data)
      if (fileLog.add(batchId, files)) {
        logInfo(s"Committed batch $batchId")
      } else {
        logWarning(s"Race while writing batch $batchId")
      }
    }
  }

  /** Writes the [[DataFrame]] to a UUID-named dir, returning the list of files paths. */
  private def writeFiles(data: DataFrame): Seq[String] = {
    val ctx = sqlContext
    val outputDir = path
    val format = fileFormat
    val schema = data.schema

    val file = new Path(basePath, UUID.randomUUID().toString).toUri.toString
    data.write.parquet(file)
    sqlContext.read
        .schema(data.schema)
        .parquet(file)
        .inputFiles
        .map(new Path(_))
        .filterNot(_.getName.startsWith("_"))
        .map(_.toUri.toString)
  }

  override def toString: String = s"FileSink[$path]"
}
