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

package org.apache.spark

import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.spark.util.SerializableConfiguration

/**
 * Internal helper class that saves an RDD using a Hadoop OutputFormat
 * (from the newer mapreduce API, not the old mapred API).
 *
 * Saves the RDD using a JobConf, which should contain an output key class, an output value class,
 * a filename to write to, etc, exactly like in a Hadoop MapReduce job.
 *
 * Use a [[HadoopMapReduceCommitProtocol]] to handle output commit, which, unlike Hadoop's
 * OutputCommitter, is serializable.
 */
private[spark]
class SparkNewHadoopWriter(
    jobConf: Configuration,
    committer: HadoopMapReduceCommitProtocol) extends Logging with Serializable {

  private val now = new Date()
  private val conf = new SerializableConfiguration(jobConf)

  private val jobtrackerID = SparkHadoopWriter.createJobTrackerID(new Date())
  private var jobId = 0
  private var splitId = 0
  private var attemptId = 0

  @transient private var writer: RecordWriter[AnyRef, AnyRef] = null
  @transient private var jobContext: JobContext = null
  @transient private var taskContext: TaskAttemptContext = null

  def setupJob(): Unit = {
    // Committer setup a job
    committer.setupJob(getJobContext)
  }

  def setupTask(context: TaskContext): Unit = {
    // Set jobID/taskID
    jobId = context.stageId
    splitId = context.partitionId
    attemptId = (context.taskAttemptId % Int.MaxValue).toInt
    // Committer setup a task
    committer.setupTask(getTaskContext(context))
  }

  def write(context: TaskContext, key: AnyRef, value: AnyRef): Unit = {
    getWriter(context).write(key, value)
  }

  def abortTask(context: TaskContext): Unit = {
    // Close writer
    getWriter(context).close(getTaskContext(context))
    // Committer abort a task
    committer.abortTask(getTaskContext(context))
  }

  def commitTask(context: TaskContext): Unit = {
    // Close writer
    getWriter(context).close(getTaskContext(context))
    // Committer commit a task
    committer.commitTask(getTaskContext(context))
  }

  def abortJob(): Unit = {
    committer.abortJob(getJobContext)
  }

  def commitJob() {
    committer.commitJob(getJobContext, Seq.empty)
  }

  // ********* Private Functions *********

  /*
   * Generate jobContext. Since jobContext is transient, it may be null after serialization.
   */
  private def getJobContext(): JobContext = {
    if (jobContext == null) {
      val jobAttemptId = new TaskAttemptID(jobtrackerID, jobId, TaskType.MAP, 0, 0)
      jobContext = new TaskAttemptContextImpl(conf.value, jobAttemptId)
    }
    jobContext
  }

  /*
   * Generate taskContext. Since taskContext is transient, it may be null after serialization.
   */
  private def getTaskContext(context: TaskContext): TaskAttemptContext = {
    if (taskContext == null) {
      val attemptId = new TaskAttemptID(jobtrackerID, jobId, TaskType.REDUCE, splitId,
        context.attemptNumber)
      taskContext = new TaskAttemptContextImpl(conf.value, attemptId)
    }
    taskContext
  }

  /*
   * Generate writer. Since writer is transient, it may be null after serialization.
   */
  private def getWriter(context: TaskContext): RecordWriter[AnyRef, AnyRef] = {
    if (writer == null) {
      val format = getJobContext.getOutputFormatClass.newInstance
      writer = format.getRecordWriter(getTaskContext(context))
        .asInstanceOf[RecordWriter[AnyRef, AnyRef]]
    }
    writer
  }
}
