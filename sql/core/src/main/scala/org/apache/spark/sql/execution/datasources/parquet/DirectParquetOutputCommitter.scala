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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.parquet.Log
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetOutputCommitter, ParquetOutputFormat}

/**
 * An output committer for writing Parquet files.  In stead of writing to the `_temporary` folder
 * like what [[ParquetOutputCommitter]] does, this output committer writes data directly to the
 * destination folder.  This can be useful for data stored in S3, where directory operations are
 * relatively expensive.
 *
 * To enable this output committer, users may set the "spark.sql.parquet.output.committer.class"
 * property via Hadoop [[Configuration]].  Not that this property overrides
 * "spark.sql.sources.outputCommitterClass".
 *
 * *NOTE*
 *
 *   NEVER use [[DirectParquetOutputCommitter]] when appending data, because currently there's
 *   no safe way undo a failed appending job (that's why both `abortTask()` and `abortJob()` are
 *   left * empty).
 */
private[parquet] class DirectParquetOutputCommitter(outputPath: Path, context: TaskAttemptContext)
  extends ParquetOutputCommitter(outputPath, context) {
  val LOG = Log.getLog(classOf[ParquetOutputCommitter])

  override def getWorkPath: Path = outputPath
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
        } catch { case e: Exception =>
          LOG.warn("could not write summary file for " + outputPath, e)
          val metadataPath = new Path(outputPath, ParquetFileWriter.PARQUET_METADATA_FILE)
          if (fileSystem.exists(metadataPath)) {
            fileSystem.delete(metadataPath, true)
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

