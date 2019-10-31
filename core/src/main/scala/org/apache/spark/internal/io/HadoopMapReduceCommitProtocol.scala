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
import java.util.Date

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.internal.Logging
import org.apache.spark.mapred.SparkHadoopMapRedUtil

/**
 * An [[FileCommitProtocol]] implementation backed by an underlying Hadoop OutputCommitter
 * (from the newer mapreduce API, not the old mapred API).
 *
 * Unlike Hadoop's OutputCommitter, this implementation is serializable.
 *
 * @param jobId the job's or stage's id
 * @param path the job's output path, or null if committer acts as a noop
 * @param dynamicPartitionOverwrite If true, Spark will overwrite partition directories at runtime
 *                                  dynamically, i.e., we first write files under a staging
 *                                  directory with partition path, e.g.
 *                                  /path/to/staging/a=1/b=1/xxx.parquet. When committing the job,
 *                                  we first clean up the corresponding partition directories at
 *                                  destination path, e.g. /path/to/destination/a=1/b=1, and move
 *                                  files from staging directory to the corresponding partition
 *                                  directories under destination path.
 */
class HadoopMapReduceCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends FileCommitProtocol with Serializable with Logging {

  import FileCommitProtocol._

  /** OutputCommitter from Hadoop is not serializable so marking it transient. */
  @transient private var committer: OutputCommitter = _

  @transient private var sparkStagingCommitter: SparkStagingOutputCommitter = _

  protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    val format = context.getOutputFormatClass.getConstructor().newInstance()
    // If OutputFormat is Configurable, we should set conf to it.
    format match {
      case c: Configurable => c.setConf(context.getConfiguration)
      case _ => ()
    }
    format.getOutputCommitter(context)
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val filename = getFilename(taskContext, ext)

    if (dynamicPartitionOverwrite) {
      sparkStagingCommitter.getTaskTempFile(taskContext, dir, filename)
    } else {
      val stagingDir: Path = committer match {
        // For FileOutputCommitter it has its own staging path called "work path".
        case f: FileOutputCommitter =>
          new Path(Option(f.getWorkPath).map(_.toString).getOrElse(path))
        case _ => new Path(path)
      }

      dir.map { d =>
        new Path(new Path(stagingDir, d), filename).toString
      }.getOrElse {
        new Path(stagingDir, filename).toString
      }
    }
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    val filename = getFilename(taskContext, ext)
    sparkStagingCommitter.getTaskTempFileAbsPath(absoluteDir, filename)
  }

  protected def getFilename(taskContext: TaskAttemptContext, ext: String): String = {
    // The file name looks like part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"part-$split%05d-$jobId$ext"
  }

  override def setupJob(jobContext: JobContext): Unit = {
    // Setup IDs
    val mrJobId = SparkHadoopWriterUtils.createJobID(new Date, 0)
    val taskId = new TaskID(mrJobId, TaskType.MAP, 0)
    val taskAttemptId = new TaskAttemptID(taskId, 0)

    // Set up the configuration object
    jobContext.getConfiguration.set("mapreduce.job.id", mrJobId.toString)
    jobContext.getConfiguration.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    jobContext.getConfiguration.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    jobContext.getConfiguration.setBoolean("mapreduce.task.ismap", true)
    jobContext.getConfiguration.setInt("mapreduce.task.partition", 0)

    val taskAttemptContext = new TaskAttemptContextImpl(jobContext.getConfiguration, taskAttemptId)
    committer = setupCommitter(taskAttemptContext)
    committer.setupJob(jobContext)

    sparkStagingCommitter = new SparkStagingOutputCommitter(jobId, path, dynamicPartitionOverwrite)
    sparkStagingCommitter.setupJob(jobContext)
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    committer.commitJob(jobContext)
    sparkStagingCommitter.commitJobWithTaskCommits(jobContext, taskCommits)
  }

  /**
   * Abort the job; log and ignore any IO exception thrown.
   * This is invariably invoked in an exception handler; raising
   * an exception here will lose the root cause of the failure.
   *
   * @param jobContext job context
   */
  override def abortJob(jobContext: JobContext): Unit = {
    try {
      committer.abortJob(jobContext, JobStatus.State.FAILED)
      sparkStagingCommitter.abortJob(jobContext, JobStatus.State.FAILED)
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${jobContext.getJobID}", e)
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    committer = setupCommitter(taskContext)
    committer.setupTask(taskContext)
    sparkStagingCommitter = new SparkStagingOutputCommitter(jobId, path, dynamicPartitionOverwrite)
    sparkStagingCommitter.setupTask(taskContext)
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    val attemptId = taskContext.getTaskAttemptID
    logTrace(s"Commit task ${attemptId}")
    if (dynamicPartitionOverwrite) {
      SparkHadoopMapRedUtil.commitTask(
        sparkStagingCommitter, taskContext, attemptId.getJobID.getId, attemptId.getTaskID.getId)
    } else {
      SparkHadoopMapRedUtil.commitTask(
        committer, taskContext, attemptId.getJobID.getId, attemptId.getTaskID.getId)
    }
    sparkStagingCommitter.getTaskCommitMessage
  }

  /**
   * Abort the task; log and ignore any failure thrown.
   * This is invariably invoked in an exception handler; raising
   * an exception here will lose the root cause of the failure.
   *
   * @param taskContext context
   */
  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    try {
      committer.abortTask(taskContext)
      sparkStagingCommitter.abortTask(taskContext)
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${taskContext.getTaskAttemptID}", e)
    }
  }
}
