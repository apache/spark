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

import java.text.NumberFormat
import java.util.{Date, Locale, UUID}

import scala.reflect.ClassTag

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.{JobContext => NewJobContext,
OutputFormat => NewOutputFormat, RecordWriter => NewRecordWriter,
TaskAttemptContext => NewTaskAttemptContext, TaskAttemptID => NewTaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.{TaskAttemptContextImpl => NewTaskAttemptContextImpl}

import org.apache.spark.{SerializableWritable, SparkConf, SparkException, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{JOB_ID, TASK_ATTEMPT_ID}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.util.{SerializableConfiguration, SerializableJobConf, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * A helper object that saves an RDD using a Hadoop OutputFormat.
 */
private[spark]
object SparkHadoopWriter extends Logging {
  import SparkHadoopWriterUtils._

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
      config: HadoopWriteConfigUtil[K, V]): Unit = {
    // Extract context and configuration from RDD.
    val sparkContext = rdd.context
    val commitJobId = rdd.id

    // Set up a job.
    val jobTrackerId = createJobTrackerID(new Date())
    val jobContext = config.createJobContext(jobTrackerId, commitJobId)
    config.initOutputFormat(jobContext)

    // Assert the output format/key/value class is set in JobConf.
    config.assertConf(jobContext, rdd.conf)

    // propagate the description UUID into the jobs, so that committers
    // get an ID guaranteed to be unique.
    jobContext.getConfiguration.set("spark.sql.sources.writeJobUUID",
      UUID.randomUUID.toString)

    val committer = config.createCommitter(commitJobId)
    committer.setupJob(jobContext)

    // Try to write all RDD partitions as a Hadoop OutputFormat.
    try {
      val ret = sparkContext.runJob(rdd, (context: TaskContext, iter: Iterator[(K, V)]) => {
        // SPARK-24552: Generate a unique "attempt ID" based on the stage and task attempt numbers.
        // Assumes that there won't be more than Short.MaxValue attempts, at least not concurrently.
        val attemptId = (context.stageAttemptNumber() << 16) | context.attemptNumber()

        executeTask(
          context = context,
          config = config,
          jobTrackerId = jobTrackerId,
          commitJobId = commitJobId,
          sparkPartitionId = context.partitionId(),
          sparkAttemptNumber = attemptId,
          committer = committer,
          iterator = iter)
      })

      logInfo(s"Start to commit write Job ${jobContext.getJobID}.")
      val (_, duration) = Utils
        .timeTakenMs { committer.commitJob(jobContext, ret.toImmutableArraySeq) }
      logInfo(s"Write Job ${jobContext.getJobID} committed. Elapsed time: $duration ms.")
    } catch {
      case cause: Throwable =>
        logError(log"Aborting job ${MDC(JOB_ID, jobContext.getJobID)}.", cause)
        committer.abortJob(jobContext)
        throw new SparkException("Job aborted.", cause)
    }
  }

  /** Write a RDD partition out in a single Spark task. */
  private def executeTask[K, V: ClassTag](
      context: TaskContext,
      config: HadoopWriteConfigUtil[K, V],
      jobTrackerId: String,
      commitJobId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[(K, V)]): TaskCommitMessage = {
    // Set up a task.
    val taskContext = config.createTaskAttemptContext(
      jobTrackerId, commitJobId, sparkPartitionId, sparkAttemptNumber)
    committer.setupTask(taskContext)

    // Initiate the writer.
    config.initWriter(taskContext, sparkPartitionId)
    var recordsWritten = 0L

    // We must initialize the callback for calculating bytes written after the statistic table
    // is initialized in FileSystem which is happened in initWriter.
    val (outputMetrics, callback) = initHadoopOutputMetrics(context)

    // Write all rows in RDD partition.
    try {
      val ret = Utils.tryWithSafeFinallyAndFailureCallbacks {
        while (iterator.hasNext) {
          val pair = iterator.next()
          config.write(pair)

          // Update bytes written metric every few records
          maybeUpdateOutputMetrics(outputMetrics, callback, recordsWritten)
          recordsWritten += 1
        }

        config.closeWriter(taskContext)
        committer.commitTask(taskContext)
      }(catchBlock = {
        // If there is an error, release resource and then abort the task.
        try {
          config.closeWriter(taskContext)
        } finally {
          committer.abortTask(taskContext)
          logError(log"Task ${MDC(TASK_ATTEMPT_ID, taskContext.getTaskAttemptID)} aborted.")
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

/**
 * A helper class that reads JobConf from older mapred API, creates output Format/Committer/Writer.
 */
private[spark]
class HadoopMapRedWriteConfigUtil[K, V: ClassTag](conf: SerializableJobConf)
  extends HadoopWriteConfigUtil[K, V] with Logging {

  private var outputFormat: Class[_ <: OutputFormat[K, V]] = null
  private var writer: RecordWriter[K, V] = null

  private def getConf: JobConf = conf.value

  // --------------------------------------------------------------------------
  // Create JobContext/TaskAttemptContext
  // --------------------------------------------------------------------------

  override def createJobContext(jobTrackerId: String, jobId: Int): NewJobContext = {
    val jobAttemptId = new SerializableWritable(new JobID(jobTrackerId, jobId))
    new JobContextImpl(getConf, jobAttemptId.value)
  }

  override def createTaskAttemptContext(
      jobTrackerId: String,
      jobId: Int,
      splitId: Int,
      taskAttemptId: Int): NewTaskAttemptContext = {
    // Update JobConf.
    HadoopRDD.addLocalConfiguration(jobTrackerId, jobId, splitId, taskAttemptId, conf.value)
    // Create taskContext.
    val attemptId = new TaskAttemptID(jobTrackerId, jobId, TaskType.MAP, splitId, taskAttemptId)
    new TaskAttemptContextImpl(getConf, attemptId)
  }

  // --------------------------------------------------------------------------
  // Create committer
  // --------------------------------------------------------------------------

  override def createCommitter(jobId: Int): HadoopMapReduceCommitProtocol = {
    // Update JobConf.
    HadoopRDD.addLocalConfiguration("", 0, 0, 0, getConf)
    // Create commit protocol.
    FileCommitProtocol.instantiate(
      className = classOf[HadoopMapRedCommitProtocol].getName,
      jobId = jobId.toString,
      outputPath = getConf.get("mapred.output.dir")
    ).asInstanceOf[HadoopMapReduceCommitProtocol]
  }

  // --------------------------------------------------------------------------
  // Create writer
  // --------------------------------------------------------------------------

  override def initWriter(taskContext: NewTaskAttemptContext, splitId: Int): Unit = {
    val numfmt = NumberFormat.getInstance(Locale.US)
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    val outputName = "part-" + numfmt.format(splitId)
    val path = FileOutputFormat.getOutputPath(getConf)
    val fs: FileSystem = {
      if (path != null) {
        path.getFileSystem(getConf)
      } else {
        // scalastyle:off FileSystemGet
        FileSystem.get(getConf)
        // scalastyle:on FileSystemGet
      }
    }

    writer = getConf.getOutputFormat
      .getRecordWriter(fs, getConf, outputName, Reporter.NULL)
      .asInstanceOf[RecordWriter[K, V]]

    require(writer != null, "Unable to obtain RecordWriter")
  }

  override def write(pair: (K, V)): Unit = {
    require(writer != null, "Must call createWriter before write.")
    writer.write(pair._1, pair._2)
  }

  override def closeWriter(taskContext: NewTaskAttemptContext): Unit = {
    if (writer != null) {
      writer.close(Reporter.NULL)
    }
  }

  // --------------------------------------------------------------------------
  // Create OutputFormat
  // --------------------------------------------------------------------------

  override def initOutputFormat(jobContext: NewJobContext): Unit = {
    if (outputFormat == null) {
      outputFormat = getConf.getOutputFormat.getClass
        .asInstanceOf[Class[_ <: OutputFormat[K, V]]]
    }
  }

  private def getOutputFormat(): OutputFormat[K, V] = {
    require(outputFormat != null, "Must call initOutputFormat first.")

    outputFormat.getConstructor().newInstance()
  }

  // --------------------------------------------------------------------------
  // Verify hadoop config
  // --------------------------------------------------------------------------

  override def assertConf(jobContext: NewJobContext, conf: SparkConf): Unit = {
    val outputFormatInstance = getOutputFormat()
    val keyClass = getConf.getOutputKeyClass
    val valueClass = getConf.getOutputValueClass
    if (outputFormatInstance == null) {
      throw new SparkException("Output format class not set")
    }
    if (keyClass == null) {
      throw new SparkException("Output key class not set")
    }
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    SparkHadoopUtil.get.addCredentials(getConf)

    logDebug("Saving as hadoop file of type (" + keyClass.getSimpleName + ", " +
      valueClass.getSimpleName + ")")

    if (SparkHadoopWriterUtils.isOutputSpecValidationEnabled(conf)) {
      // FileOutputFormat ignores the filesystem parameter
      // scalastyle:off FileSystemGet
      val ignoredFs = FileSystem.get(getConf)
      // scalastyle:on FileSystemGet
      getOutputFormat().checkOutputSpecs(ignoredFs, getConf)
    }
  }
}

/**
 * A helper class that reads Configuration from newer mapreduce API, creates output
 * Format/Committer/Writer.
 */
private[spark]
class HadoopMapReduceWriteConfigUtil[K, V: ClassTag](conf: SerializableConfiguration)
  extends HadoopWriteConfigUtil[K, V] with Logging {

  private var outputFormat: Class[_ <: NewOutputFormat[K, V]] = null
  private var writer: NewRecordWriter[K, V] = null

  private def getConf: Configuration = conf.value

  // --------------------------------------------------------------------------
  // Create JobContext/TaskAttemptContext
  // --------------------------------------------------------------------------

  override def createJobContext(jobTrackerId: String, jobId: Int): NewJobContext = {
    val jobAttemptId = new NewTaskAttemptID(jobTrackerId, jobId, TaskType.MAP, 0, 0)
    new NewTaskAttemptContextImpl(getConf, jobAttemptId)
  }

  override def createTaskAttemptContext(
      jobTrackerId: String,
      jobId: Int,
      splitId: Int,
      taskAttemptId: Int): NewTaskAttemptContext = {
    val attemptId = new NewTaskAttemptID(
      jobTrackerId, jobId, TaskType.REDUCE, splitId, taskAttemptId)
    new NewTaskAttemptContextImpl(getConf, attemptId)
  }

  // --------------------------------------------------------------------------
  // Create committer
  // --------------------------------------------------------------------------

  override def createCommitter(jobId: Int): HadoopMapReduceCommitProtocol = {
    FileCommitProtocol.instantiate(
      className = classOf[HadoopMapReduceCommitProtocol].getName,
      jobId = jobId.toString,
      outputPath = getConf.get("mapreduce.output.fileoutputformat.outputdir")
    ).asInstanceOf[HadoopMapReduceCommitProtocol]
  }

  // --------------------------------------------------------------------------
  // Create writer
  // --------------------------------------------------------------------------

  override def initWriter(taskContext: NewTaskAttemptContext, splitId: Int): Unit = {
    val taskFormat = getOutputFormat()
    // If OutputFormat is Configurable, we should set conf to it.
    taskFormat match {
      case c: Configurable => c.setConf(getConf)
      case _ => ()
    }

    writer = taskFormat.getRecordWriter(taskContext)
      .asInstanceOf[NewRecordWriter[K, V]]

    require(writer != null, "Unable to obtain RecordWriter")
  }

  override def write(pair: (K, V)): Unit = {
    require(writer != null, "Must call createWriter before write.")
    writer.write(pair._1, pair._2)
  }

  override def closeWriter(taskContext: NewTaskAttemptContext): Unit = {
    if (writer != null) {
      writer.close(taskContext)
      writer = null
    } else {
      logWarning("Writer has been closed.")
    }
  }

  // --------------------------------------------------------------------------
  // Create OutputFormat
  // --------------------------------------------------------------------------

  override def initOutputFormat(jobContext: NewJobContext): Unit = {
    if (outputFormat == null) {
      outputFormat = jobContext.getOutputFormatClass
        .asInstanceOf[Class[_ <: NewOutputFormat[K, V]]]
    }
  }

  private def getOutputFormat(): NewOutputFormat[K, V] = {
    require(outputFormat != null, "Must call initOutputFormat first.")

    outputFormat.getConstructor().newInstance()
  }

  // --------------------------------------------------------------------------
  // Verify hadoop config
  // --------------------------------------------------------------------------

  override def assertConf(jobContext: NewJobContext, conf: SparkConf): Unit = {
    if (SparkHadoopWriterUtils.isOutputSpecValidationEnabled(conf)) {
      getOutputFormat().checkOutputSpecs(jobContext)
    }
  }
}
