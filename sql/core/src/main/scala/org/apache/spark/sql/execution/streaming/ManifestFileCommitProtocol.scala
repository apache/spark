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

import java.io.IOException
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.PATH
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.errors.QueryExecutionErrors

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

  @transient private var pendingCommitFiles: ArrayBuffer[Path] = _

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
    pendingCommitFiles = new ArrayBuffer[Path]
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    require(fileLog != null, "setupManifestOptions must be called before this function")
    val fileStatuses = taskCommits.flatMap(_.obj.asInstanceOf[Seq[SinkFileStatus]]).toArray

    // We shouldn't remove the files if they're written to the metadata:
    // `fileLog.add(batchId, fileStatuses)` could fail AFTER writing files to the metadata
    // as well as there could be race
    // so for the safety we clean up the list before calling anything incurs exception.
    // The case is uncommon and we do best effort instead of guarantee, so the simplicity of
    // logic here would be OK, and safe for dealing with unexpected situations.
    pendingCommitFiles.clear()

    if (fileLog.add(batchId, fileStatuses)) {
      logInfo(s"Committed batch $batchId")
    } else {
      throw new IllegalStateException(s"Race while writing batch $batchId")
    }
  }

  override def abortJob(jobContext: JobContext): Unit = {
    require(fileLog != null, "setupManifestOptions must be called before this function")
    // Best effort cleanup of complete files from failed job.
    // Since the file has UUID in its filename, we are safe to try deleting them
    // as the file will not conflict with file with another attempt on the same task.
    if (pendingCommitFiles.nonEmpty) {
      pendingCommitFiles.foreach { path =>
        try {
          val fs = path.getFileSystem(jobContext.getConfiguration)
          // this is to make sure the file can be seen from driver as well
          if (fs.exists(path)) {
            fs.delete(path, false)
          }
        } catch {
          case e: IOException =>
            logWarning(log"Fail to remove temporary file ${MDC(PATH, path)}, " +
              log"continue removing next.", e)
        }
      }
      pendingCommitFiles.clear()
    }
  }

  override def onTaskCommit(taskCommit: TaskCommitMessage): Unit = {
    pendingCommitFiles ++= taskCommit.obj.asInstanceOf[Seq[SinkFileStatus]]
      .map(_.toFileStatus.getPath)
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
    throw QueryExecutionErrors.addFilesWithAbsolutePathUnsupportedError(this.toString)
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    if (addedFiles.nonEmpty) {
      val fs = new Path(addedFiles.head).getFileSystem(taskContext.getConfiguration)
      val statuses: Seq[SinkFileStatus] =
        addedFiles.map(f => SinkFileStatus(fs.getFileStatus(new Path(f)))).toSeq
      new TaskCommitMessage(statuses)
    } else {
      new TaskCommitMessage(Seq.empty[SinkFileStatus])
    }
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    // best effort cleanup of incomplete files
    if (addedFiles.nonEmpty) {
      val fs = new Path(addedFiles.head).getFileSystem(taskContext.getConfiguration)
      addedFiles.foreach { file => fs.delete(new Path(file), false) }
    }
  }
}
