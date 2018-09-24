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

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileContext, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage

class StagingFileCommitProtocol(jobId: String, path: String)
  extends FileCommitProtocol with Serializable with Logging
  with ManifestCommitProtocol {
  private var stagingDir: Option[Path] = None


  def jobStagingDir: Path = {
    new Path(new Path(path, "staging"), s"job-$jobId")
  }

  override def setupJob(jobContext: JobContext): Unit = {
    jobStagingDir.getFileSystem(jobContext.getConfiguration).delete(jobStagingDir, true)
    logInfo(s"Job $jobId set up")
  }


  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    stagingDir = Some(new Path(jobStagingDir, s"partition-${partition(taskContext)}"))
    stagingDir.get.getFileSystem(taskContext.getConfiguration).delete(stagingDir.get, true)
    logInfo(s"Task set up to handle partition ${partition(taskContext)} in job $jobId")

  }

  private def partition(taskContext: TaskAttemptContext) = {
    taskContext.getConfiguration.getInt("mapreduce.task.partition", -1)
  }


  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    val fs = jobStagingDir.getFileSystem(jobContext.getConfiguration)
    val fileCtx = FileContext.getFileContext

    def moveIfPossible(next: Path, target: Path) = {
      try {
        fileCtx.rename(next, target)
      } catch {
        case _: FileAlreadyExistsException =>
          val status = fileCtx.getFileStatus(target)
          logWarning(s"File ${target.toUri.toASCIIString} has already been generated " +
            s"earlier (${status.toString}), deleting instead of moving " +
            s"recently generated file: ${fileCtx.getFileStatus(target).toString}")
          fileCtx.delete(next, false)
      }
    }

    def moveEach(from: Path, to: String) = {
      val files = fs.listFiles(from, true)
      val statuses = Array.newBuilder[SinkFileStatus]
      while (files.hasNext) {
        val next = files.next().getPath
        val target = if (next.getParent.getName.startsWith(outputPartitionPrefix)) {
          val subdir = next.getParent.getName.substring(outputPartitionPrefix.length)
              .replaceAll(subdirEscapeSequence, "/")
          val outputPartition = new Path(to, subdir)
          fs.mkdirs(outputPartition)
          new Path(outputPartition, next.getName)
        } else {
          new Path(to, next.getName)
        }
        moveIfPossible(next, target)
        statuses += SinkFileStatus(fs.getFileStatus(target))
      }
      if (fileLog.add(batchId, statuses.result)) {
        logInfo(s"Job $jobId committed")
      } else {
        throw new IllegalStateException(s"Race while writing batch $batchId")
      }
    }

    moveEach(jobStagingDir, path)

    Seq()
  }

  override def abortJob(jobContext: JobContext): Unit = {}


  private var fileCounter: Int = -1

  private def nextCounter: Int = {
    fileCounter += 1
    fileCounter
  }

  private val outputPartitionPrefix = "part_prefix_"

  private val subdirEscapeSequence = "___per___"

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val staging = stagingDir.getOrElse(
      throw new IllegalStateException("Staging dir needs to be initilized in setupTask()"))
    val targetDir = dir.map(d => new Path(staging, stagingReplacementDir(d))).getOrElse(staging)
    val res = new Path(targetDir, s"part-j$jobId-p${partition(taskContext)}-c$nextCounter$ext")
      .toString
    logInfo(s"New file generated $res")
    res
  }

  private def stagingReplacementDir(d: String) = {
    outputPartitionPrefix + d.replaceAll("/", subdirEscapeSequence)
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    throw new UnsupportedOperationException(
      s"$this does not support adding files with an absolute path")
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    new TaskCommitMessage(None)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {}
}
