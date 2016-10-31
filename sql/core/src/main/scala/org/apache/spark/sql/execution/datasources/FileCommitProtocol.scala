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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter

import org.apache.spark.mapred.SparkHadoopMapRedUtil


object FileCommitProtocol {
  class TaskCommitMessage(obj: Any) extends Serializable

  object EmptyTaskCommitMessage extends TaskCommitMessage(Unit)
}


/**
 * An interface to define how a Spark job commits its outputs.
 *
 * The proper call sequence is:
 *
 * 1. Driver calls setupJob.
 * 2. As part of each task's execution, executor calls setupTask[] and then commitTask
 *    (or abortTask if task failed).
 * 3. When all necessary tasks completed successfully, the driver calls commitJob. If the job
 *    failed to execute (e.g. too many failed tasks), the job should call abortJob.
 */
abstract class FileCommitProtocol {
  import FileCommitProtocol._

  /**
   * The temporary location to write to, if available.
   *
   * If this function returns None, then Spark will always write directly to the final destination.
   */
  def stagingDir: Option[String]

  def setupJob(jobContext: JobContext): Unit

  def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit

  def abortJob(jobContext: JobContext): Unit

  def setupTask(taskContext: TaskAttemptContext): Unit

  def addTaskFile(taskContext: TaskAttemptContext, path: String): Unit

  def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage

  def abortTask(taskContext: TaskAttemptContext): Unit
}


/**
 * An [[FileCommitProtocol]] implementation backed by an underlying Hadoop OutputCommitter
 * (confusingly from the newer mapreduce API, not the old mapred API).
 */
class MapReduceFileCommitterProtocol(committer: OutputCommitter) extends FileCommitProtocol {
  import FileCommitProtocol._

  def this(outputFormatClass: Class[_ <: OutputFormat[_, _]], taskContext: TaskAttemptContext) = {
    this(outputFormatClass.newInstance().getOutputCommitter(taskContext))
  }

  override def stagingDir: Option[String] = committer match {
    // For FileOutputCommitter it has its own staging path called "work path".
    case f: FileOutputCommitter => Option(f.getWorkPath.toString)
    case _ => None
  }

  override def setupJob(jobContext: JobContext): Unit = {
    committer.setupJob(jobContext)
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    committer.commitJob(jobContext)
  }

  override def abortJob(jobContext: JobContext): Unit = {
    committer.abortJob(jobContext, JobStatus.State.FAILED)
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
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

  override def addTaskFile(taskContext: TaskAttemptContext, path: String): Unit = {
    // Do nothing
  }
}
