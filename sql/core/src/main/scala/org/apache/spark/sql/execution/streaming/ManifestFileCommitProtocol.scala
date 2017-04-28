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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage

/**
 * A [[FileCommitProtocol]] that tracks the list of valid files in a manifest file, used in
 * structured streaming.
 *
 * @param path path to write the final output to.
 */
class ManifestFileCommitProtocol(jobId: String, path: String)
  extends FileCommitProtocol with Serializable with Logging {

  // Track the list of files added by a task, only used on the executors.
  @transient private var addedFiles: ArrayBuffer[String] = _

  @transient private var fileLog: FileStreamSinkLog = _
  private var batchId: Long = _

  /**
   * Sets up the manifest log output and the batch id for this job.
   * Must be called before any other function.
   */
  def setupManifestOptions(fileLog: FileStreamSinkLog, batchId: Long): Unit = {
    this.fileLog = fileLog
    this.batchId = batchId
  }

  override def setupJob(jobContext: JobContext): Unit = {
    require(fileLog != null, "setupManifestOptions must be called before this function")
    // Do nothing
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    require(fileLog != null, "setupManifestOptions must be called before this function")
    val fileStatuses = taskCommits.flatMap(_.obj.asInstanceOf[Seq[SinkFileStatus]]).toArray

    if (fileLog.add(batchId, fileStatuses)) {
      logInfo(s"Committed batch $batchId")
    } else {
      throw new IllegalStateException(s"Race while writing batch $batchId")
    }
  }

  override def abortJob(jobContext: JobContext): Unit = {
    require(fileLog != null, "setupManifestOptions must be called before this function")
    // Do nothing
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    addedFiles = new ArrayBuffer[String]
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    // The file name looks like part-r-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003.gz.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    val uuid = UUID.randomUUID.toString
    val filename = f"part-$split%05d-$uuid$ext"

    val file = dir.map { d =>
      new Path(new Path(path, d), filename).toString
    }.getOrElse {
      new Path(path, filename).toString
    }

    addedFiles += file
    file
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    throw new UnsupportedOperationException(
      s"$this does not support adding files with an absolute path")
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    if (addedFiles.nonEmpty) {
      val fs = new Path(addedFiles.head).getFileSystem(taskContext.getConfiguration)
      val statuses: Seq[SinkFileStatus] =
        addedFiles.map(f => SinkFileStatus(fs.getFileStatus(new Path(f))))
      new TaskCommitMessage(statuses)
    } else {
      new TaskCommitMessage(Seq.empty[SinkFileStatus])
    }
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    // Do nothing
    // TODO: we can also try delete the addedFiles as a best-effort cleanup.
  }
}
