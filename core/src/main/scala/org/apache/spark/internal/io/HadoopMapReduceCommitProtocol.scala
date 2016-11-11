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

import java.util.{Date, UUID}

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.SparkHadoopWriter
import org.apache.spark.internal.Logging
import org.apache.spark.mapred.SparkHadoopMapRedUtil

/**
 * An [[FileCommitProtocol]] implementation backed by an underlying Hadoop OutputCommitter
 * (from the newer mapreduce API, not the old mapred API).
 *
 * Unlike Hadoop's OutputCommitter, this implementation is serializable.
 */
class HadoopMapReduceCommitProtocol(jobId: String, path: String)
  extends FileCommitProtocol with Serializable with Logging {

  import FileCommitProtocol._

  /** OutputCommitter from Hadoop is not serializable so marking it transient. */
  @transient private var committer: OutputCommitter = _

  /**
   * Tracks files staged by this task for absolute output paths. These outputs are not managed by
   * the Hadoop OutputCommitter, so we must move these to their final locations on job commit.
   *
   * The mapping is from the temp output path to the final desired output path of the file.
   */
  @transient private var addedAbsPathFiles: mutable.Map[String, String] = null

  /**
   * The staging directory for all files committed with absolute output paths.
   */
  private def absPathStagingDir: Path = new Path(path, "_temporary-" + jobId)

  protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    context.getOutputFormatClass.newInstance().getOutputCommitter(context)
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val filename = getFilename(taskContext, ext)

    val stagingDir: String = committer match {
      // For FileOutputCommitter it has its own staging path called "work path".
      case f: FileOutputCommitter => Option(f.getWorkPath.toString).getOrElse(path)
      case _ => path
    }

    dir.map { d =>
      new Path(new Path(stagingDir, d), filename).toString
    }.getOrElse {
      new Path(stagingDir, filename).toString
    }
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    val filename = getFilename(taskContext, ext)
    val absOutputPath = new Path(absoluteDir, filename).toString

    // Include a UUID here to prevent file collisions for one task writing to different dirs.
    // In principle we could include hash(absoluteDir) instead but this is simpler.
    val tmpOutputPath = new Path(
      absPathStagingDir, UUID.randomUUID().toString() + "-" + filename).toString

    addedAbsPathFiles(tmpOutputPath) = absOutputPath
    tmpOutputPath
  }

  private def getFilename(taskContext: TaskAttemptContext, ext: String): String = {
    // The file name looks like part-r-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003.gz.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"part-$split%05d-$jobId$ext"
  }

  override def setupJob(jobContext: JobContext): Unit = {
    // Setup IDs
    val jobId = SparkHadoopWriter.createJobID(new Date, 0)
    val taskId = new TaskID(jobId, TaskType.MAP, 0)
    val taskAttemptId = new TaskAttemptID(taskId, 0)

    // Set up the configuration object
    jobContext.getConfiguration.set("mapred.job.id", jobId.toString)
    jobContext.getConfiguration.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
    jobContext.getConfiguration.set("mapred.task.id", taskAttemptId.toString)
    jobContext.getConfiguration.setBoolean("mapred.task.is.map", true)
    jobContext.getConfiguration.setInt("mapred.task.partition", 0)

    val taskAttemptContext = new TaskAttemptContextImpl(jobContext.getConfiguration, taskAttemptId)
    committer = setupCommitter(taskAttemptContext)
    committer.setupJob(jobContext)
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    committer.commitJob(jobContext)
    val filesToMove = taskCommits.map(_.obj.asInstanceOf[Map[String, String]])
      .foldLeft(Map[String, String]())(_ ++ _)
    logDebug(s"Committing files staged for absolute locations $filesToMove")
    val fs = absPathStagingDir.getFileSystem(jobContext.getConfiguration)
    for ((src, dst) <- filesToMove) {
      fs.rename(new Path(src), new Path(dst))
    }
    fs.delete(absPathStagingDir, true)
  }

  override def abortJob(jobContext: JobContext): Unit = {
    committer.abortJob(jobContext, JobStatus.State.FAILED)
    val fs = absPathStagingDir.getFileSystem(jobContext.getConfiguration)
    fs.delete(absPathStagingDir, true)
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    committer = setupCommitter(taskContext)
    committer.setupTask(taskContext)
    addedAbsPathFiles = mutable.Map[String, String]()
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    val attemptId = taskContext.getTaskAttemptID
    SparkHadoopMapRedUtil.commitTask(
      committer, taskContext, attemptId.getJobID.getId, attemptId.getTaskID.getId)
    new TaskCommitMessage(addedAbsPathFiles.toMap)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    committer.abortTask(taskContext)
    // best effort cleanup of other staged files
    for ((src, _) <- addedAbsPathFiles) {
      val tmp = new Path(src)
      tmp.getFileSystem(taskContext.getConfiguration).delete(tmp, false)
    }
  }
}
