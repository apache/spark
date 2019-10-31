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

package org.apache.spark.internal.io

import java.io.IOException
import java.util.UUID

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, JobStatus, OutputCommitter, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage

class SparkStagingOutputCommitter(
    sparkJobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends OutputCommitter with Logging {

  /**
   * Checks whether there are files to be committed to a valid output location.
   *
   * As committing and aborting a job occurs on driver, where `addedAbsPathFiles` is always null,
   * it is necessary to check whether a valid output path is specified.
   * [[SparkStagingOutputCommitter#path]] need not be a valid [[org.apache.hadoop.fs.Path]] for
   * committers not writing to distributed file systems.
   */
  private val hasValidPath = Try { new Path(path) }.isSuccess

  /**
   * The staging directory of this write job. Spark uses it to deal with files with absolute output
   * path, or writing data into partitioned directory with dynamicPartitionOverwrite=true.
   */
  private var stagingDir: Path = _

  /**
   * Tracks files staged by this task for absolute output paths. These outputs are not managed by
   * the Hadoop OutputCommitter, so we must move these to their final locations on job commit.
   *
   * The mapping is from the temp output path to the final desired output path of the file.
   */
  private var addedAbsPathFiles: mutable.Map[String, String] = null

  /**
   * Tracks partitions with default path that have new files written into them by this task,
   * e.g. a=1/b=2. Files under these partitions will be saved into staging directory and moved to
   * destination directory at the end, if `dynamicPartitionOverwrite` is true.
   */
  private var partitionPaths: mutable.Set[String] = null

  /**
   * Tracks the staging task file and partition paths with dynamicPartitionOverwrite=true.
   */
  private var dynamicStagingTaskFilePartitions: mutable.Map[Path, String] = null

  def getTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], filename: String): String = {
    assert(dir.isDefined,
      "The dataset to be written must be partitioned when dynamicPartitionOverwrite is true.")
    partitionPaths += dir.get
    val attemptID = taskContext.getTaskAttemptID.getId
    val tempFile = new Path(new Path(stagingDir, s"$attemptID${Path.SEPARATOR}${dir.get}"),
      filename)
    dynamicStagingTaskFilePartitions += tempFile -> dir.get
    tempFile.toString
  }

  def getTaskTempFileAbsPath(absoluteDir: String, filename: String): String = {
    val absOutputPath = new Path(absoluteDir, filename).toString

    // Include a UUID here to prevent file collisions for one task writing to different dirs.
    // In principle we could include hash(absoluteDir) instead but this is simpler.
    val tmpOutputPath = new Path(stagingDir, UUID.randomUUID().toString() + "-" + filename).toString

    addedAbsPathFiles(tmpOutputPath) = absOutputPath
    tmpOutputPath
  }

  override def setupJob(jobContext: JobContext): Unit = {
    if (hasValidPath) {
      stagingDir = new Path(path, ".spark-staging-" + sparkJobId)
      val fs = new Path(path).getFileSystem(jobContext.getConfiguration)
      if (!fs.mkdirs(stagingDir)) {
        logError(s"Mkdirs failed to create $stagingDir")
      } else {
        logWarning("Output path is null in setupJob()")
      }
    }
  }

  override def commitJob(jobContext: JobContext): Unit = {
    throw new UnsupportedOperationException("Not supported commitJob without TaskCommitMessages.")
  }

  def commitJobWithTaskCommits(
      jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    if (hasValidPath) {
      val (allAbsPathFiles, allPartitionPaths) =
        taskCommits.map(_.obj.asInstanceOf[(Map[String, String], Set[String])]).unzip
      val fs = stagingDir.getFileSystem(jobContext.getConfiguration)
      val filesToMove = allAbsPathFiles.foldLeft(Map[String, String]())(_ ++ _)
      logDebug(s"Committing files staged for absolute locations $filesToMove")
      if (dynamicPartitionOverwrite) {
        val absPartitionPaths = filesToMove.values.map(new Path(_).getParent).toSet
        logDebug(s"Clean up absolute partition directories for overwriting: $absPartitionPaths")
        absPartitionPaths.foreach(fs.delete(_, true))
      }
      for ((src, dst) <- filesToMove) {
        fs.rename(new Path(src), new Path(dst))
      }

      if (dynamicPartitionOverwrite) {
        val partitionPaths = allPartitionPaths.foldLeft(Set[String]())(_ ++ _)
        logDebug(s"Clean up default partition directories for overwriting: $partitionPaths")
        for (part <- partitionPaths) {
          val finalPartPath = new Path(path, part)
          if (!fs.delete(finalPartPath, true) && !fs.exists(finalPartPath.getParent)) {
            // According to the official hadoop FileSystem API spec, delete op should assume
            // the destination is no longer present regardless of return value, thus we do not
            // need to double check if finalPartPath exists before rename.
            // Also in our case, based on the spec, delete returns false only when finalPartPath
            // does not exist. When this happens, we need to take action if parent of finalPartPath
            // also does not exist(e.g. the scenario described on SPARK-23815), because
            // FileSystem API spec on rename op says the rename dest(finalPartPath) must have
            // a parent that exists, otherwise we may get unexpected result on the rename.
            fs.mkdirs(finalPartPath.getParent)
          }
          fs.rename(new Path(stagingDir, part), finalPartPath)
        }
      }
      fs.delete(stagingDir, true)
    }
  }

  override def abortJob(jobContext: JobContext, state: JobStatus.State): Unit = {
    if (hasValidPath) {
      val fs = stagingDir.getFileSystem(jobContext.getConfiguration)
      fs.delete(stagingDir, true)
    }
  }

  override def setupTask(taskAttemptContext: TaskAttemptContext): Unit = {
    addedAbsPathFiles = mutable.Map[String, String]()
    partitionPaths = mutable.Set[String]()
    dynamicStagingTaskFilePartitions = mutable.Map[Path, String]()
    stagingDir = new Path(path, ".spark-staging-" + sparkJobId)
  }

  override def commitTask(taskContext: TaskAttemptContext): Unit = {
    if (dynamicPartitionOverwrite) {
      val fs = stagingDir.getFileSystem(taskContext.getConfiguration)
      dynamicStagingTaskFilePartitions.foreach { case (stagingTaskFile, partitionPath) =>
        val fileName = stagingTaskFile.getName
        val finalFile = new Path(new Path(stagingDir, partitionPath), fileName)
        if (fs.exists(finalFile) && !fs.delete(finalFile, false)) {
          throw new IOException(s"Failed to delete existed $finalFile")
        }
        if (!fs.rename(stagingTaskFile, finalFile)) {
          throw new IOException(s"Failed to rename $stagingTaskFile to $finalFile")
        }
      }
    }
  }

  def getTaskCommitMessage: TaskCommitMessage = {
    new TaskCommitMessage(addedAbsPathFiles.toMap -> partitionPaths.toSet)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    // best effort cleanup of other staged files
    for ((src, _) <- addedAbsPathFiles) {
      val tmp = new Path(src)
      tmp.getFileSystem(taskContext.getConfiguration).delete(tmp, false)
    }

    for (tmp <- dynamicStagingTaskFilePartitions.keys) {
      tmp.getFileSystem(taskContext.getConfiguration).delete(tmp, false)
    }
  }

  override def needsTaskCommit(taskAttemptContext: TaskAttemptContext): Boolean = {
    true
  }
}
