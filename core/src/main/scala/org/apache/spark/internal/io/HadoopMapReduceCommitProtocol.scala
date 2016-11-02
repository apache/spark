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

import java.util.Date

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

  protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    context.getOutputFormatClass.newInstance().getOutputCommitter(context)
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    // The file name looks like part-r-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003.gz.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    val filename = f"part-$split%05d-$jobId$ext"

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
  }

  override def abortJob(jobContext: JobContext): Unit = {
    committer.abortJob(jobContext, JobStatus.State.FAILED)
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    committer = setupCommitter(taskContext)
    committer.setupTask(taskContext)
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    val attemptId = taskContext.getTaskAttemptID
    SparkHadoopMapRedUtil.commitTask(
      committer, taskContext, attemptId.getJobID.getId, attemptId.getTaskID.getId)
    EmptyTaskCommitMessage
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    committer.abortTask(taskContext)
  }
}
