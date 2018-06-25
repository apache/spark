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

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.reflect.ClassTag
import scala.util.DynamicVariable

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, JobID}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.{SparkConf, SparkException, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.OutputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * A helper object that saves an RDD using a Hadoop OutputFormat
 * (from the newer mapreduce API, not the old mapred API).
 */
private[spark]
object SparkHadoopMapReduceWriter extends Logging {

  /**
   * Basic work flow of this command is:
   * 1. Driver side setup, prepare the data source and hadoop configuration for the write job to
   *    be issued.
   * 2. Issues a write job consists of one or more executor side tasks, each of which writes all
   *    rows within an RDD partition.
   * 3. If no exception is thrown in a task, commits that task, otherwise aborts that task;  If any
   *    exception is thrown during task commitment, also aborts that task.
   * 4. If all tasks are committed, commit the job, otherwise aborts the job;  If any exception is
   *    thrown during job commitment, also aborts the job.
   */
  def write[K, V: ClassTag](
      rdd: RDD[(K, V)],
      hadoopConf: Configuration): Unit = {
    // Extract context and configuration from RDD.
    val sparkContext = rdd.context
    val commitJobId = rdd.id
    val sparkConf = rdd.conf
    val conf = new SerializableConfiguration(hadoopConf)

    // Set up a job.
    val jobTrackerId = SparkHadoopWriterUtils.createJobTrackerID(new Date())
    val jobAttemptId = new TaskAttemptID(jobTrackerId, commitJobId, TaskType.MAP, 0, 0)
    val jobContext = new TaskAttemptContextImpl(conf.value, jobAttemptId)
    val format = jobContext.getOutputFormatClass

    if (SparkHadoopWriterUtils.isOutputSpecValidationEnabled(sparkConf)) {
      // FileOutputFormat ignores the filesystem parameter
      val jobFormat = format.newInstance
      jobFormat.checkOutputSpecs(jobContext)
    }

    val committer = FileCommitProtocol.instantiate(
      className = classOf[HadoopMapReduceCommitProtocol].getName,
      jobId = commitJobId.toString,
      outputPath = conf.value.get("mapreduce.output.fileoutputformat.outputdir"),
      isAppend = false).asInstanceOf[HadoopMapReduceCommitProtocol]
    committer.setupJob(jobContext)

    // Try to write all RDD partitions as a Hadoop OutputFormat.
    try {
      val ret = sparkContext.runJob(rdd, (context: TaskContext, iter: Iterator[(K, V)]) => {
        // SPARK-24552: Generate a unique "attempt ID" based on the stage and task attempt numbers.
        // Assumes that there won't be more than Short.MaxValue attempts, at least not concurrently.
        val attemptId = (context.stageAttemptNumber << 16) | context.attemptNumber

        executeTask(
          context = context,
          jobTrackerId = jobTrackerId,
          commitJobId = commitJobId,
          sparkPartitionId = context.partitionId,
          sparkAttemptNumber = attemptId,
          committer = committer,
          hadoopConf = conf.value,
          outputFormat = format.asInstanceOf[Class[OutputFormat[K, V]]],
          iterator = iter)
      })

      committer.commitJob(jobContext, ret)
      logInfo(s"Job ${jobContext.getJobID} committed.")
    } catch {
      case cause: Throwable =>
        logError(s"Aborting job ${jobContext.getJobID}.", cause)
        committer.abortJob(jobContext)
        throw new SparkException("Job aborted.", cause)
    }
  }

  /** Write an RDD partition out in a single Spark task. */
  private def executeTask[K, V: ClassTag](
      context: TaskContext,
      jobTrackerId: String,
      commitJobId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      hadoopConf: Configuration,
      outputFormat: Class[_ <: OutputFormat[K, V]],
      iterator: Iterator[(K, V)]): TaskCommitMessage = {
    // Set up a task.
    val attemptId = new TaskAttemptID(jobTrackerId, commitJobId, TaskType.REDUCE,
      sparkPartitionId, sparkAttemptNumber)
    val taskContext = new TaskAttemptContextImpl(hadoopConf, attemptId)
    committer.setupTask(taskContext)

    val (outputMetrics, callback) = SparkHadoopWriterUtils.initHadoopOutputMetrics(context)

    // Initiate the writer.
    val taskFormat = outputFormat.newInstance()
    // If OutputFormat is Configurable, we should set conf to it.
    taskFormat match {
      case c: Configurable => c.setConf(hadoopConf)
      case _ => ()
    }
    var writer = taskFormat.getRecordWriter(taskContext)
      .asInstanceOf[RecordWriter[K, V]]
    require(writer != null, "Unable to obtain RecordWriter")
    var recordsWritten = 0L

    // Write all rows in RDD partition.
    try {
      val ret = Utils.tryWithSafeFinallyAndFailureCallbacks {
        // Write rows out, release resource and commit the task.
        while (iterator.hasNext) {
          val pair = iterator.next()
          writer.write(pair._1, pair._2)

          // Update bytes written metric every few records
          SparkHadoopWriterUtils.maybeUpdateOutputMetrics(outputMetrics, callback, recordsWritten)
          recordsWritten += 1
        }
        if (writer != null) {
          writer.close(taskContext)
          writer = null
        }
        committer.commitTask(taskContext)
      }(catchBlock = {
        // If there is an error, release resource and then abort the task.
        try {
          if (writer != null) {
            writer.close(taskContext)
            writer = null
          }
        } finally {
          committer.abortTask(taskContext)
          logError(s"Task ${taskContext.getTaskAttemptID} aborted.")
        }
      })

      outputMetrics.setBytesWritten(callback())
      outputMetrics.setRecordsWritten(recordsWritten)

      ret
    } catch {
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }
  }
}
