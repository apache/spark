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

package org.apache.spark.sql.parquet

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter

import parquet.Log
import parquet.hadoop.util.ContextUtil
import parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetOutputCommitter, ParquetOutputFormat}

private[parquet] class DirectParquetOutputCommitter(outputPath: Path, context: TaskAttemptContext)
  extends ParquetOutputCommitter(outputPath, context) {
  val LOG = Log.getLog(classOf[ParquetOutputCommitter])

  override def getWorkPath(): Path = outputPath
  override def abortTask(taskContext: TaskAttemptContext): Unit = {}
  override def commitTask(taskContext: TaskAttemptContext): Unit = {}
  override def needsTaskCommit(taskContext: TaskAttemptContext): Boolean = true
  override def setupJob(jobContext: JobContext): Unit = {}
  override def setupTask(taskContext: TaskAttemptContext): Unit = {}

  override def commitJob(jobContext: JobContext) {
    val configuration = ContextUtil.getConfiguration(jobContext)
    val fileSystem = outputPath.getFileSystem(configuration)

    if (configuration.getBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, true)) {
      try {
        val outputStatus = fileSystem.getFileStatus(outputPath)
        val footers = ParquetFileReader.readAllFootersInParallel(configuration, outputStatus)
        try {
          ParquetFileWriter.writeMetadataFile(configuration, outputPath, footers)
        } catch {
          case e: Exception => {
            LOG.warn("could not write summary file for " + outputPath, e)
            val metadataPath = new Path(outputPath, ParquetFileWriter.PARQUET_METADATA_FILE)
            if (fileSystem.exists(metadataPath)) {
              fileSystem.delete(metadataPath, true)
            }
          }
        }
      } catch {
        case e: Exception => LOG.warn("could not write summary file for " + outputPath, e)
      }
    }

    if (configuration.getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true)) {
      try {
        val successPath = new Path(outputPath, FileOutputCommitter.SUCCEEDED_FILE_NAME)
        fileSystem.create(successPath).close()
      } catch {
        case e: Exception => LOG.warn("could not write success file for " + outputPath, e)
      }
    }
  }
}

