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

import java.util.{Date, UUID}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, _}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.{SortExec, SparkPlan, SQLExecution}
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.{SerializableConfiguration, Utils}


/** A helper object for writing FileFormat data out to a location. */
object FileFormatWriter extends Logging {

  /**
   * Max number of files a single task writes out due to file size. In most cases the number of
   * files written should be very small. This is just a safe guard to protect some really bad
   * settings, e.g. maxRecordsPerFile = 1.
   */
  private val MAX_FILE_COUNTER = 1000 * 1000

  /** Describes how output files should be placed in the filesystem. */
  case class OutputSpec(
    outputPath: String,
    customPartitionLocations: Map[TablePartitionSpec, String],
    outputColumns: Seq[Attribute])

  /** A shared job description for all the write tasks. */
  private class WriteJobDescription(
      val uuid: String,  // prevent collision between different (appending) write jobs
      val serializableHadoopConf: SerializableConfiguration,
      val outputWriterFactory: OutputWriterFactory,
      val allColumns: Seq[Attribute],
      val dataColumns: Seq[Attribute],
      val partitionColumns: Seq[Attribute],
      val bucketIdExpression: Option[Expression],
      val path: String,
      val customPartitionLocations: Map[TablePartitionSpec, String],
      val maxRecordsPerFile: Long,
      val timeZoneId: String,
      val statsTrackers: Seq[WriteJobStatsTracker])
    extends Serializable {

    assert(AttributeSet(allColumns) == AttributeSet(partitionColumns ++ dataColumns),
      s"""
         |All columns: ${allColumns.mkString(", ")}
         |Partition columns: ${partitionColumns.mkString(", ")}
         |Data columns: ${dataColumns.mkString(", ")}
       """.stripMargin)
  }

  /** The result of a successful write task. */
  private case class WriteTaskResult(commitMsg: TaskCommitMessage, summary: ExecutedWriteSummary)

  /**
   * Basic work flow of this command is:
   * 1. Driver side setup, including output committer initialization and data source specific
   *    preparation work for the write job to be issued.
   * 2. Issues a write job consists of one or more executor side tasks, each of which writes all
   *    rows within an RDD partition.
   * 3. If no exception is thrown in a task, commits that task, otherwise aborts that task;  If any
   *    exception is thrown during task commitment, also aborts that task.
   * 4. If all tasks are committed, commit the job, otherwise aborts the job;  If any exception is
   *    thrown during job commitment, also aborts the job.
   * 5. If the job is successfully committed, perform post-commit operations such as
   *    processing statistics.
   * @return The set of all partition paths that were updated during this write job.
   */
  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: FileFormat,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      statsTrackers: Seq[WriteJobStatsTracker],
      options: Map[String, String])
    : Set[String] = {

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, new Path(outputSpec.outputPath))

    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = outputSpec.outputColumns.filterNot(partitionSet.contains)

    val bucketIdExpression = bucketSpec.map { spec =>
      val bucketColumns = spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)
      // Use `HashPartitioning.partitionIdExpression` as our bucket id expression, so that we can
      // guarantee the data distribution is same between shuffle and bucketed data source, which
      // enables us to only shuffle one side when join a bucketed table and a normal one.
      HashPartitioning(bucketColumns, spec.numBuckets).partitionIdExpression
    }
    val sortColumns = bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }

    val caseInsensitiveOptions = CaseInsensitiveMap(options)

    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, caseInsensitiveOptions, dataColumns.toStructType)

    val description = new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = outputSpec.outputColumns,
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      bucketIdExpression = bucketIdExpression,
      path = outputSpec.outputPath,
      customPartitionLocations = outputSpec.customPartitionLocations,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = statsTrackers
    )

    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    val requiredOrdering = partitionColumns ++ bucketIdExpression ++ sortColumns
    // the sort order doesn't matter
    val actualOrdering = plan.outputOrdering.map(_.child)
    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    SQLExecution.checkSQLExecutionId(sparkSession)

    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    committer.setupJob(job)

    try {
      val rdd = if (orderingMatched) {
        plan.execute()
      } else {
        // SPARK-21165: the `requiredOrdering` is based on the attributes from analyzed plan, and
        // the physical plan may have different attribute ids due to optimizer removing some
        // aliases. Here we bind the expression ahead to avoid potential attribute ids mismatch.
        val orderingExpr = requiredOrdering
          .map(SortOrder(_, Ascending))
          .map(BindReferences.bindReference(_, outputSpec.outputColumns))
        SortExec(
          orderingExpr,
          global = false,
          child = plan).execute()
      }
      val ret = new Array[WriteTaskResult](rdd.partitions.length)
      sparkSession.sparkContext.runJob(
        rdd,
        (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
          executeTask(
            description = description,
            sparkStageId = taskContext.stageId(),
            sparkPartitionId = taskContext.partitionId(),
            sparkAttemptNumber = taskContext.attemptNumber(),
            committer,
            iterator = iter)
        },
        0 until rdd.partitions.length,
        (index, res: WriteTaskResult) => {
          committer.onTaskCommit(res.commitMsg)
          ret(index) = res
        })

      val commitMsgs = ret.map(_.commitMsg)

      committer.commitJob(job, commitMsgs)
      logInfo(s"Job ${job.getJobID} committed.")

      processStats(description.statsTrackers, ret.map(_.summary.stats))
      logInfo(s"Finished processing stats for job ${job.getJobID}.")

      // return a set of all the partition paths that were updated during this job
      ret.map(_.summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)
    } catch { case cause: Throwable =>
      logError(s"Aborting job ${job.getJobID}.", cause)
      committer.abortJob(job)
      throw new SparkException("Job aborted.", cause)
    }
  }

  /** Writes data out in a single Spark task. */
  private def executeTask(
      description: WriteJobDescription,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]): WriteTaskResult = {

    val jobId = SparkHadoopWriterUtils.createJobID(new Date, sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the attempt context required to use in the output committer.
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapreduce.job.id", jobId.toString)
      hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapreduce.task.ismap", true)
      hadoopConf.setInt("mapreduce.task.partition", 0)

      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    committer.setupTask(taskAttemptContext)

    val writeTask =
      if (sparkPartitionId != 0 && !iterator.hasNext) {
        // In case of empty job, leave first partition to save meta for file format like parquet.
        new EmptyDirectoryWriteTask(description)
      } else if (description.partitionColumns.isEmpty && description.bucketIdExpression.isEmpty) {
        new SingleDirectoryWriteTask(description, taskAttemptContext, committer)
      } else {
        new DynamicPartitionWriteTask(description, taskAttemptContext, committer)
      }

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Execute the task to write rows out and commit the task.
        val summary = writeTask.execute(iterator)
        writeTask.releaseResources()
        WriteTaskResult(committer.commitTask(taskAttemptContext), summary)
      })(catchBlock = {
        // If there is an error, release resource and then abort the task
        try {
          writeTask.releaseResources()
        } finally {
          committer.abortTask(taskAttemptContext)
          logError(s"Job $jobId aborted.")
        }
      })
    } catch {
      case e: FetchFailedException =>
        throw e
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows.", t)
    }
  }

  /**
   * For every registered [[WriteJobStatsTracker]], call `processStats()` on it, passing it
   * the corresponding [[WriteTaskStats]] from all executors.
   */
  private def processStats(
      statsTrackers: Seq[WriteJobStatsTracker],
      statsPerTask: Seq[Seq[WriteTaskStats]])
    : Unit = {

    val numStatsTrackers = statsTrackers.length
    assert(statsPerTask.forall(_.length == numStatsTrackers),
      s"""Every WriteTask should have produced one `WriteTaskStats` object for every tracker.
         |There are $numStatsTrackers statsTrackers, but some task returned
         |${statsPerTask.find(_.length != numStatsTrackers).get.length} results instead.
       """.stripMargin)

    val statsPerTracker = if (statsPerTask.nonEmpty) {
      statsPerTask.transpose
    } else {
      statsTrackers.map(_ => Seq.empty)
    }

    statsTrackers.zip(statsPerTracker).foreach {
      case (statsTracker, stats) => statsTracker.processStats(stats)
    }
  }

  /**
   * A simple trait for writing out data in a single Spark task, without any concerns about how
   * to commit or abort tasks. Exceptions thrown by the implementation of this trait will
   * automatically trigger task aborts.
   */
  private trait ExecuteWriteTask {

    /**
     * Writes data out to files, and then returns the summary of relative information which
     * includes the list of partition strings written out. The list of partitions is sent back
     * to the driver and used to update the catalog. Other information will be sent back to the
     * driver too and used to e.g. update the metrics in UI.
     */
    def execute(iterator: Iterator[InternalRow]): ExecutedWriteSummary
    def releaseResources(): Unit
  }

  /** ExecuteWriteTask for empty partitions */
  private class EmptyDirectoryWriteTask(description: WriteJobDescription)
    extends ExecuteWriteTask {

    val statsTrackers: Seq[WriteTaskStatsTracker] =
      description.statsTrackers.map(_.newTaskInstance())

    override def execute(iter: Iterator[InternalRow]): ExecutedWriteSummary = {
      ExecutedWriteSummary(
        updatedPartitions = Set.empty,
        stats = statsTrackers.map(_.getFinalStats()))
    }

    override def releaseResources(): Unit = {}
  }

  /** Writes data to a single directory (used for non-dynamic-partition writes). */
  private class SingleDirectoryWriteTask(
      description: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      committer: FileCommitProtocol) extends ExecuteWriteTask {

    private[this] var currentWriter: OutputWriter = _

    val statsTrackers: Seq[WriteTaskStatsTracker] =
      description.statsTrackers.map(_.newTaskInstance())

    private def newOutputWriter(fileCounter: Int): Unit = {
      val ext = description.outputWriterFactory.getFileExtension(taskAttemptContext)
      val currentPath = committer.newTaskTempFile(
        taskAttemptContext,
        None,
        f"-c$fileCounter%03d" + ext)

      currentWriter = description.outputWriterFactory.newInstance(
        path = currentPath,
        dataSchema = description.dataColumns.toStructType,
        context = taskAttemptContext)

      statsTrackers.map(_.newFile(currentPath))
    }

    override def execute(iter: Iterator[InternalRow]): ExecutedWriteSummary = {
      var fileCounter = 0
      var recordsInFile: Long = 0L
      newOutputWriter(fileCounter)

      while (iter.hasNext) {
        if (description.maxRecordsPerFile > 0 && recordsInFile >= description.maxRecordsPerFile) {
          fileCounter += 1
          assert(fileCounter < MAX_FILE_COUNTER,
            s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")

          recordsInFile = 0
          releaseResources()
          newOutputWriter(fileCounter)
        }

        val internalRow = iter.next()
        currentWriter.write(internalRow)
        statsTrackers.foreach(_.newRow(internalRow))
        recordsInFile += 1
      }
      releaseResources()
      ExecutedWriteSummary(
        updatedPartitions = Set.empty,
        stats = statsTrackers.map(_.getFinalStats()))
    }

    override def releaseResources(): Unit = {
      if (currentWriter != null) {
        try {
          currentWriter.close()
        } finally {
          currentWriter = null
        }
      }
    }
  }

  /**
   * Writes data to using dynamic partition writes, meaning this single function can write to
   * multiple directories (partitions) or files (bucketing).
   */
  private class DynamicPartitionWriteTask(
      desc: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      committer: FileCommitProtocol) extends ExecuteWriteTask {

    /** Flag saying whether or not the data to be written out is partitioned. */
    val isPartitioned = desc.partitionColumns.nonEmpty

    /** Flag saying whether or not the data to be written out is bucketed. */
    val isBucketed = desc.bucketIdExpression.isDefined

    assert(isPartitioned || isBucketed,
      s"""DynamicPartitionWriteTask should be used for writing out data that's either
         |partitioned or bucketed. In this case neither is true.
         |WriteJobDescription: ${desc}
       """.stripMargin)

    // currentWriter is initialized whenever we see a new key (partitionValues + BucketId)
    private var currentWriter: OutputWriter = _

    /** Trackers for computing various statistics on the data as it's being written out. */
    private val statsTrackers: Seq[WriteTaskStatsTracker] =
      desc.statsTrackers.map(_.newTaskInstance())

    /** Extracts the partition values out of an input row. */
    private lazy val getPartitionValues: InternalRow => UnsafeRow = {
      val proj = UnsafeProjection.create(desc.partitionColumns, desc.allColumns)
      row => proj(row)
    }

    /** Expression that given partition columns builds a path string like: col1=val/col2=val/... */
    private lazy val partitionPathExpression: Expression = Concat(
      desc.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
        val partitionName = ScalaUDF(
          ExternalCatalogUtils.getPartitionPathString _,
          StringType,
          Seq(Literal(c.name), Cast(c, StringType, Option(desc.timeZoneId))))
        if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
      })

    /** Evaluates the `partitionPathExpression` above on a row of `partitionValues` and returns
     * the partition string. */
    private lazy val getPartitionPath: InternalRow => String = {
      val proj = UnsafeProjection.create(Seq(partitionPathExpression), desc.partitionColumns)
      row => proj(row).getString(0)
    }

    /** Given an input row, returns the corresponding `bucketId` */
    private lazy val getBucketId: InternalRow => Int = {
      val proj = UnsafeProjection.create(desc.bucketIdExpression.toSeq, desc.allColumns)
      row => proj(row).getInt(0)
    }

    /** Returns the data columns to be written given an input row */
    private val getOutputRow = UnsafeProjection.create(desc.dataColumns, desc.allColumns)

    /**
     * Opens a new OutputWriter given a partition key and/or a bucket id.
     * If bucket id is specified, we will append it to the end of the file name, but before the
     * file extension, e.g. part-r-00009-ea518ad4-455a-4431-b471-d24e03814677-00002.gz.parquet
     *
     * @param partitionValues the partition which all tuples being written by this `OutputWriter`
     *                        belong to
     * @param bucketId the bucket which all tuples being written by this `OutputWriter` belong to
     * @param fileCounter the number of files that have been written in the past for this specific
     *                    partition. This is used to limit the max number of records written for a
     *                    single file. The value should start from 0.
     * @param updatedPartitions the set of updated partition paths, we should add the new partition
     *                          path of this writer to it.
     */
    private def newOutputWriter(
        partitionValues: Option[InternalRow],
        bucketId: Option[Int],
        fileCounter: Int,
        updatedPartitions: mutable.Set[String]): Unit = {

      val partDir = partitionValues.map(getPartitionPath(_))
      partDir.foreach(updatedPartitions.add)

      val bucketIdStr = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")

      // This must be in a form that matches our bucketing format. See BucketingUtils.
      val ext = f"$bucketIdStr.c$fileCounter%03d" +
        desc.outputWriterFactory.getFileExtension(taskAttemptContext)

      val customPath = partDir.flatMap { dir =>
          desc.customPartitionLocations.get(PartitioningUtils.parsePathFragment(dir))
      }
      val currentPath = if (customPath.isDefined) {
        committer.newTaskTempFileAbsPath(taskAttemptContext, customPath.get, ext)
      } else {
        committer.newTaskTempFile(taskAttemptContext, partDir, ext)
      }

      currentWriter = desc.outputWriterFactory.newInstance(
        path = currentPath,
        dataSchema = desc.dataColumns.toStructType,
        context = taskAttemptContext)

      statsTrackers.foreach(_.newFile(currentPath))
    }

    override def execute(iter: Iterator[InternalRow]): ExecutedWriteSummary = {
      // If anything below fails, we should abort the task.
      var recordsInFile: Long = 0L
      var fileCounter = 0
      val updatedPartitions = mutable.Set[String]()
      var currentPartionValues: Option[UnsafeRow] = None
      var currentBucketId: Option[Int] = None

      for (row <- iter) {
        val nextPartitionValues = if (isPartitioned) Some(getPartitionValues(row)) else None
        val nextBucketId = if (isBucketed) Some(getBucketId(row)) else None

        if (currentPartionValues != nextPartitionValues || currentBucketId != nextBucketId) {
          // See a new partition or bucket - write to a new partition dir (or a new bucket file).
          if (isPartitioned && currentPartionValues != nextPartitionValues) {
            currentPartionValues = Some(nextPartitionValues.get.copy())
            statsTrackers.foreach(_.newPartition(currentPartionValues.get))
          }
          if (isBucketed) {
            currentBucketId = nextBucketId
            statsTrackers.foreach(_.newBucket(currentBucketId.get))
          }

          recordsInFile = 0
          fileCounter = 0

          releaseResources()
          newOutputWriter(currentPartionValues, currentBucketId, fileCounter, updatedPartitions)
        } else if (desc.maxRecordsPerFile > 0 &&
            recordsInFile >= desc.maxRecordsPerFile) {
          // Exceeded the threshold in terms of the number of records per file.
          // Create a new file by increasing the file counter.
          recordsInFile = 0
          fileCounter += 1
          assert(fileCounter < MAX_FILE_COUNTER,
            s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")

          releaseResources()
          newOutputWriter(currentPartionValues, currentBucketId, fileCounter, updatedPartitions)
        }
        val outputRow = getOutputRow(row)
        currentWriter.write(outputRow)
        statsTrackers.foreach(_.newRow(outputRow))
        recordsInFile += 1
      }
      releaseResources()

      ExecutedWriteSummary(
        updatedPartitions = updatedPartitions.toSet,
        stats = statsTrackers.map(_.getFinalStats()))
    }

    override def releaseResources(): Unit = {
      if (currentWriter != null) {
        try {
          currentWriter.close()
        } finally {
          currentWriter = null
        }
      }
    }
  }
}

/**
 * Wrapper class for the metrics of writing data out.
 *
 * @param updatedPartitions the partitions updated during writing data out. Only valid
 *                          for dynamic partition.
 * @param stats one `WriteTaskStats` object for every `WriteJobStatsTracker` that the job had.
 */
case class ExecutedWriteSummary(
  updatedPartitions: Set[String],
  stats: Seq[WriteTaskStats])
