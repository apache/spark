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

import java.text.NumberFormat
import java.util.{Date, Locale}

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.TaskType

import org.apache.spark.executor.OutputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.{HadoopMapRedCommitProtocol, HadoopMapReduceCommitProtocol, FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{Utils, SerializableJobConf}

import scala.reflect.ClassTag


/**
 * A helper object that saves an RDD using a Hadoop OutputFormat
 * (from the old mapred API).
 */
private[spark]
object SparkHadoopMapRedWriter extends Logging {

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
      hadoopConf: JobConf): Unit = {
    // Extract context and configuration from RDD.
    val sparkContext = rdd.context
    val stageId = rdd.id
    val sparkConf = rdd.conf
    val conf = new SerializableJobConf(hadoopConf)

    // Set up a job.
    val jobTrackerId = SparkHadoopWriterUtils.createJobTrackerID(new Date())
    val jobAttemptId = new SerializableWritable[JobID](SparkHadoopWriterUtils.createJobID(new Date(), stageId))
    val jobContext = new JobContextImpl(conf.value, jobAttemptId.value)
    val format = conf.value.getOutputFormat.getClass

    if (SparkHadoopWriterUtils.isOutputSpecValidationEnabled(sparkConf)) {
      // FileOutputFormat ignores the filesystem parameter
      val ignoredFs = FileSystem.get(hadoopConf)
      format.newInstance().checkOutputSpecs(ignoredFs, hadoopConf)
    }

    val committer = FileCommitProtocol.instantiate(
      className = classOf[HadoopMapRedCommitProtocol].getName,
      jobId = stageId.toString,
      outputPath = conf.value.get("mapred.output.dir"),
      isAppend = false).asInstanceOf[HadoopMapReduceCommitProtocol]
    committer.setupJob(jobContext)

    // When speculation is on and output committer class name contains "Direct", we should warn
    // users that they may loss data if they are using a direct output committer.
    if (SparkHadoopWriterUtils.isSpeculationEnabled(sparkConf) && committer.isDirectOutput) {
      val warningMessage =
        s"$committer may be an output committer that writes data directly to " +
          "the final location. Because speculation is enabled, this output committer may " +
          "cause data loss (see the case in SPARK-10063). If possible, please use an output " +
          "committer that does not have this behavior (e.g. FileOutputCommitter)."
      logWarning(warningMessage)
    }

    // Try to write all RDD partitions as a Hadoop OutputFormat.
    try {
      val ret = sparkContext.runJob(rdd, (context: TaskContext, iter: Iterator[(K, V)]) => {
        executeTask(
          context = context,
          jobTrackerId = jobTrackerId,
          sparkStageId = context.stageId,
          sparkPartitionId = context.partitionId,
          sparkAttemptNumber = context.attemptNumber,
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

  /** Write a RDD partition out in a single Spark task. */
  private def executeTask[K, V: ClassTag](
      context: TaskContext,
      jobTrackerId: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      hadoopConf: JobConf,
      outputFormat: Class[_ <: OutputFormat[K, V]],
      iterator: Iterator[(K, V)]): TaskCommitMessage = {
    // Set up a task.

    // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
    // around by taking a mod. We expect that no task will be attempted 2 billion times.
    val taskAttemptId = (context.taskAttemptId % Int.MaxValue).toInt
    //String jtIdentifier, int jobId, TaskType type, int taskId, int id
    val attemptId = new TaskAttemptID(jobTrackerId, sparkStageId, TaskType.MAP, sparkPartitionId, taskAttemptId)
    val taskContext = new TaskAttemptContextImpl(hadoopConf, attemptId)
    committer.setupTask(taskContext)

    val outputMetricsAndBytesWrittenCallback: Option[(OutputMetrics, () => Long)] =
      SparkHadoopWriterUtils.initHadoopOutputMetrics(context)

    // Initiate the writer.
    val numfmt = NumberFormat.getInstance(Locale.US)
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    val outputName = "part-"  + numfmt.format(sparkPartitionId)
    val path = FileOutputFormat.getOutputPath(hadoopConf)
    val fs: FileSystem = {
      if (path != null) {
        path.getFileSystem(hadoopConf)
      } else {
        FileSystem.get(hadoopConf)
      }
    }

    val writer = hadoopConf.getOutputFormat.getRecordWriter(fs, hadoopConf, outputName, Reporter.NULL)
      .asInstanceOf[RecordWriter[K, V]]
    require(writer != null, "Unable to obtain RecordWriter")
    var recordsWritten = 0L

    // Write all rows in RDD partition.
    try {
      val ret = Utils.tryWithSafeFinallyAndFailureCallbacks {
        while (iterator.hasNext) {
          val pair = iterator.next()
          writer.write(pair._1, pair._2)

          // Update bytes written metric every few records
          SparkHadoopWriterUtils.maybeUpdateOutputMetrics(
            outputMetricsAndBytesWrittenCallback, recordsWritten)
          recordsWritten += 1
        }

        committer.commitTask(taskContext)
      }(catchBlock = {
        committer.abortTask(taskContext)
        logError(s"Task ${taskContext.getTaskAttemptID} aborted.")
      }, finallyBlock = writer.close(Reporter.NULL))

      outputMetricsAndBytesWrittenCallback.foreach {
        case (om, callback) =>
          om.setBytesWritten(callback())
          om.setRecordsWritten(recordsWritten)
      }

      ret
    } catch {
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }
  }
}
