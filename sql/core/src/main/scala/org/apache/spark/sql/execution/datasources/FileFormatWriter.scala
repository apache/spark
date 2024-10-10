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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark._
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan, SQLExecution, UnsafeExternalRowSorter}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.util.ArrayImplicits._


/** A helper object for writing FileFormat data out to a location. */
object FileFormatWriter extends Logging {
  /** Describes how output files should be placed in the filesystem. */
  case class OutputSpec(
      outputPath: String,
      customPartitionLocations: Map[TablePartitionSpec, String],
      outputColumns: Seq[Attribute])

  /** Describes how concurrent output writers should be executed. */
  case class ConcurrentOutputWriterSpec(
      maxWriters: Int,
      createSorter: () => UnsafeExternalRowSorter)

  /**
   * A variable used in tests to check whether the output ordering of the query matches the
   * required ordering of the write command.
   */
  private[sql] var outputOrderingMatched: Boolean = false

  /**
   * A variable used in tests to check the final executed plan.
   */
  private[sql] var executedPlan: Option[SparkPlan] = None

  // scalastyle:off argcount
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
      options: Map[String, String],
      numStaticPartitionCols: Int = 0)
    : Set[String] = {
    require(partitionColumns.size >= numStaticPartitionCols)

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, new Path(outputSpec.outputPath))

    val partitionSet = AttributeSet(partitionColumns)
    // cleanup the internal metadata information of
    // the file source metadata attribute if any before write out
    val finalOutputSpec = outputSpec.copy(outputColumns = outputSpec.outputColumns
      .map(FileSourceMetadataAttribute.cleanupFileSourceMetadataInformation))
    val dataColumns = finalOutputSpec.outputColumns.filterNot(partitionSet.contains)

    val writerBucketSpec = V1WritesUtils.getWriterBucketSpec(bucketSpec, dataColumns, options)
    val sortColumns = V1WritesUtils.getBucketSortColumns(bucketSpec, dataColumns)

    val caseInsensitiveOptions = CaseInsensitiveMap(options)

    val dataSchema = dataColumns.toStructType
    DataSourceUtils.verifySchema(fileFormat, dataSchema)
    DataSourceUtils.checkFieldNames(fileFormat, dataSchema)
    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, caseInsensitiveOptions, dataSchema)

    val description = new WriteJobDescription(
      uuid = UUID.randomUUID.toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = finalOutputSpec.outputColumns,
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      bucketSpec = writerBucketSpec,
      path = finalOutputSpec.outputPath,
      customPartitionLocations = finalOutputSpec.customPartitionLocations,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = statsTrackers
    )

    // We should first sort by dynamic partition columns, then bucket id, and finally sorting
    // columns.
    val requiredOrdering = partitionColumns.drop(numStaticPartitionCols) ++
        writerBucketSpec.map(_.bucketIdExpression) ++ sortColumns
    val writeFilesOpt = V1WritesUtils.getWriteFilesOpt(plan)

    // SPARK-40588: when planned writing is disabled and AQE is enabled,
    // plan contains an AdaptiveSparkPlanExec, which does not know
    // its final plan's ordering, so we have to materialize that plan first
    // it is fine to use plan further down as the final plan is cached in that plan
    def materializeAdaptiveSparkPlan(plan: SparkPlan): SparkPlan = plan match {
      case a: AdaptiveSparkPlanExec => a.finalPhysicalPlan
      case p: SparkPlan => p.withNewChildren(p.children.map(materializeAdaptiveSparkPlan))
    }

    // the sort order doesn't matter
    val actualOrdering = writeFilesOpt.map(_.child)
      .getOrElse(materializeAdaptiveSparkPlan(plan))
      .outputOrdering
    val orderingMatched = V1WritesUtils.isOrderingMatched(requiredOrdering, actualOrdering)

    SQLExecution.checkSQLExecutionId(sparkSession)

    // propagate the description UUID into the jobs, so that committers
    // get an ID guaranteed to be unique.
    job.getConfiguration.set("spark.sql.sources.writeJobUUID", description.uuid)

    // When `PLANNED_WRITE_ENABLED` is true, the optimizer rule V1Writes will add logical sort
    // operator based on the required ordering of the V1 write command. So the output
    // ordering of the physical plan should always match the required ordering. Here
    // we set the variable to verify this behavior in tests.
    // There are two cases where FileFormatWriter still needs to add physical sort:
    // 1) When the planned write config is disabled.
    // 2) When the concurrent writers are enabled (in this case the required ordering of a
    //    V1 write command will be empty).
    if (Utils.isTesting) outputOrderingMatched = orderingMatched

    if (writeFilesOpt.isDefined) {
      // build `WriteFilesSpec` for `WriteFiles`
      val concurrentOutputWriterSpecFunc = (plan: SparkPlan) => {
        val sortPlan = createSortPlan(plan, requiredOrdering, outputSpec)
        createConcurrentOutputWriterSpec(sparkSession, sortPlan, sortColumns)
      }
      val writeSpec = WriteFilesSpec(
        description = description,
        committer = committer,
        concurrentOutputWriterSpecFunc = concurrentOutputWriterSpecFunc
      )
      executeWrite(sparkSession, plan, writeSpec, job)
    } else {
      executeWrite(sparkSession, plan, job, description, committer, outputSpec,
        requiredOrdering, partitionColumns, sortColumns, orderingMatched)
    }
  }
  // scalastyle:on argcount

  private def executeWrite(
      sparkSession: SparkSession,
      plan: SparkPlan,
      job: Job,
      description: WriteJobDescription,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      requiredOrdering: Seq[Expression],
      partitionColumns: Seq[Attribute],
      sortColumns: Seq[Attribute],
      orderingMatched: Boolean): Set[String] = {
    val projectList = V1WritesUtils.convertEmptyToNull(plan.output, partitionColumns)
    val empty2NullPlan = if (projectList.nonEmpty) ProjectExec(projectList, plan) else plan

    writeAndCommit(job, description, committer) {
      val (planToExecute, concurrentOutputWriterSpec) = if (orderingMatched) {
        (empty2NullPlan, None)
      } else {
        val sortPlan = createSortPlan(empty2NullPlan, requiredOrdering, outputSpec)
        val concurrentOutputWriterSpec = createConcurrentOutputWriterSpec(
          sparkSession, sortPlan, sortColumns)
        if (concurrentOutputWriterSpec.isDefined) {
          (empty2NullPlan, concurrentOutputWriterSpec)
        } else {
          (sortPlan, concurrentOutputWriterSpec)
        }
      }

      // In testing, this is the only way to get hold of the actually executed plan written to file
      if (Utils.isTesting) executedPlan = Some(planToExecute)

      val rdd = planToExecute.execute()

      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
        sparkSession.sparkContext.parallelize(Array.empty[InternalRow].toImmutableArraySeq, 1)
      } else {
        rdd
      }

      val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date())
      val ret = new Array[WriteTaskResult](rddWithNonEmptyPartitions.partitions.length)
      sparkSession.sparkContext.runJob(
        rddWithNonEmptyPartitions,
        (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
          executeTask(
            description = description,
            jobTrackerID = jobTrackerID,
            sparkStageId = taskContext.stageId(),
            sparkPartitionId = taskContext.partitionId(),
            sparkAttemptNumber = taskContext.taskAttemptId().toInt & Integer.MAX_VALUE,
            committer,
            iterator = iter,
            concurrentOutputWriterSpec = concurrentOutputWriterSpec)
        },
        rddWithNonEmptyPartitions.partitions.indices,
        (index, res: WriteTaskResult) => {
          committer.onTaskCommit(res.commitMsg)
          ret(index) = res
        })
      ret
    }
  }

  private def writeAndCommit(
      job: Job,
      description: WriteJobDescription,
      committer: FileCommitProtocol)(f: => Array[WriteTaskResult]): Set[String] = {
    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    committer.setupJob(job)
    try {
      val ret = f
      val commitMsgs = ret.map(_.commitMsg)

      logInfo(log"Start to commit write Job ${MDC(LogKeys.UUID, description.uuid)}.")
      val (_, duration) = Utils
        .timeTakenMs { committer.commitJob(job, commitMsgs.toImmutableArraySeq) }
      logInfo(log"Write Job ${MDC(LogKeys.UUID, description.uuid)} committed. " +
        log"Elapsed time: ${MDC(LogKeys.ELAPSED_TIME, duration)} ms.")

      processStats(
        description.statsTrackers, ret.map(_.summary.stats).toImmutableArraySeq, duration)
      logInfo(log"Finished processing stats for write job ${MDC(LogKeys.UUID, description.uuid)}.")

      // return a set of all the partition paths that were updated during this job
      ret.map(_.summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)
    } catch { case cause: Throwable =>
      logError(log"Aborting job ${MDC(WRITE_JOB_UUID, description.uuid)}.", cause)
      committer.abortJob(job)
      throw cause
    }
  }

  /**
   * Write files using [[SparkPlan.executeWrite]]
   */
  private def executeWrite(
      session: SparkSession,
      planForWrites: SparkPlan,
      writeFilesSpec: WriteFilesSpec,
      job: Job): Set[String] = {
    val committer = writeFilesSpec.committer
    val description = writeFilesSpec.description

    // In testing, this is the only way to get hold of the actually executed plan written to file
    if (Utils.isTesting) executedPlan = Some(planForWrites)

    writeAndCommit(job, description, committer) {
      val rdd = planForWrites.executeWrite(writeFilesSpec)
      val ret = new Array[WriteTaskResult](rdd.partitions.length)
      session.sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[WriterCommitMessage]) => {
          assert(iter.hasNext)
          val commitMessage = iter.next()
          assert(!iter.hasNext)
          commitMessage
        },
        rdd.partitions.indices,
        (index, res: WriterCommitMessage) => {
          assert(res.isInstanceOf[WriteTaskResult])
          val writeTaskResult = res.asInstanceOf[WriteTaskResult]
          committer.onTaskCommit(writeTaskResult.commitMsg)
          ret(index) = writeTaskResult
        })
      ret
    }
  }

  private def createSortPlan(
      plan: SparkPlan,
      requiredOrdering: Seq[Expression],
      outputSpec: OutputSpec): SortExec = {
    // SPARK-21165: the `requiredOrdering` is based on the attributes from analyzed plan, and
    // the physical plan may have different attribute ids due to optimizer removing some
    // aliases. Here we bind the expression ahead to avoid potential attribute ids mismatch.
    val orderingExpr = bindReferences(
      requiredOrdering.map(SortOrder(_, Ascending)), outputSpec.outputColumns)
    SortExec(
      orderingExpr,
      global = false,
      child = plan)
  }

  private def createConcurrentOutputWriterSpec(
      sparkSession: SparkSession,
      sortPlan: SortExec,
      sortColumns: Seq[Attribute]): Option[ConcurrentOutputWriterSpec] = {
    val maxWriters = sparkSession.sessionState.conf.maxConcurrentOutputFileWriters
    val concurrentWritersEnabled = maxWriters > 0 && sortColumns.isEmpty
    if (concurrentWritersEnabled) {
      Some(ConcurrentOutputWriterSpec(maxWriters, () => sortPlan.createSorter()))
    } else {
      None
    }
  }

  /** Writes data out in a single Spark task. */
  private[spark] def executeTask(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow],
      concurrentOutputWriterSpec: Option[ConcurrentOutputWriterSpec]): WriteTaskResult = {

    val jobId = SparkHadoopWriterUtils.createJobID(jobTrackerID, sparkStageId)
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

    var dataWriter: FileFormatDataWriter = null

    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      dataWriter =
        if (sparkPartitionId != 0 && !iterator.hasNext) {
          // In case of empty job, leave first partition to save meta for file format like parquet.
          new EmptyDirectoryDataWriter(description, taskAttemptContext, committer)
        } else if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
          new SingleDirectoryDataWriter(description, taskAttemptContext, committer)
        } else {
          concurrentOutputWriterSpec match {
            case Some(spec) =>
              new DynamicPartitionDataConcurrentWriter(
                description, taskAttemptContext, committer, spec)
            case _ =>
              new DynamicPartitionDataSingleWriter(description, taskAttemptContext, committer)
          }
        }

      // Execute the task to write rows out and commit the task.
      dataWriter.writeWithIterator(iterator)
      val (_, taskWriteDataTime) = Utils.timeTakenMs {
        dataWriter.writeWithIterator(iterator)
      }
      dataWriter.commit(taskWriteDataTime)
    })(catchBlock = {
      // If there is an error, abort the task
      if (dataWriter != null) {
        dataWriter.abort()
      } else {
        committer.abortTask(taskAttemptContext)
      }
      logError(log"Job: ${MDC(JOB_ID, jobId)}, Task: ${MDC(TASK_ID, taskId)}, " +
        log"Task attempt ${MDC(TASK_ATTEMPT_ID, taskAttemptId)} aborted.")
    }, finallyBlock = {
      if (dataWriter != null) {
        dataWriter.close()
      }
    })
  }

  /**
   * For every registered [[WriteJobStatsTracker]], call `processStats()` on it, passing it
   * the corresponding [[WriteTaskStats]] from all executors.
   */
  private[datasources] def processStats(
      statsTrackers: Seq[WriteJobStatsTracker],
      statsPerTask: Seq[Seq[WriteTaskStats]],
      jobCommitDuration: Long)
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
      case (statsTracker, stats) => statsTracker.processStats(stats, jobCommitDuration)
    }
  }
}
