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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.{QueryExecution, SortExec, SQLExecution}
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
    outputPath: String, customPartitionLocations: Map[TablePartitionSpec, String])

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
      val timeZoneId: String)
    extends Serializable {

    assert(AttributeSet(allColumns) == AttributeSet(partitionColumns ++ dataColumns),
      s"""
         |All columns: ${allColumns.mkString(", ")}
         |Partition columns: ${partitionColumns.mkString(", ")}
         |Data columns: ${dataColumns.mkString(", ")}
       """.stripMargin)
  }

  /** The result of a successful write task. */
  private case class WriteTaskResult(commitMsg: TaskCommitMessage, updatedPartitions: Set[String])

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
   */
  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      fileFormat: FileFormat,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      refreshFunction: (Seq[TablePartitionSpec]) => Unit,
      options: Map[String, String]): Unit = {

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, new Path(outputSpec.outputPath))

    // Pick the attributes from analyzed plan, as optimizer may not preserve the output schema
    // names' case.
    val allColumns = queryExecution.analyzed.output
    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = allColumns.filterNot(partitionSet.contains)

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
      allColumns = allColumns,
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      bucketIdExpression = bucketIdExpression,
      path = outputSpec.outputPath,
      customPartitionLocations = outputSpec.customPartitionLocations,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone)
    )

    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    val requiredOrdering = partitionColumns ++ bucketIdExpression ++ sortColumns
    // the sort order doesn't matter
    val actualOrdering = queryExecution.executedPlan.outputOrdering.map(_.child)
    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      // This call shouldn't be put into the `try` block below because it only initializes and
      // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
      committer.setupJob(job)

      try {
        val rdd = if (orderingMatched) {
          queryExecution.toRdd
        } else {
          // SPARK-21165: the `requiredOrdering` is based on the attributes from analyzed plan, and
          // the physical plan may have different attribute ids due to optimizer removing some
          // aliases. Here we bind the expression ahead to avoid potential attribute ids mismatch.
          val orderingExpr = requiredOrdering
            .map(SortOrder(_, Ascending)).map(BindReferences.bindReference(_, allColumns))
          SortExec(
            orderingExpr,
            global = false,
            child = queryExecution.executedPlan).execute()
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
        val updatedPartitions = ret.flatMap(_.updatedPartitions)
          .distinct.map(PartitioningUtils.parsePathFragment)

        committer.commitJob(job, commitMsgs)
        logInfo(s"Job ${job.getJobID} committed.")
        refreshFunction(updatedPartitions)
      } catch { case cause: Throwable =>
        logError(s"Aborting job ${job.getJobID}.", cause)
        committer.abortJob(job)
        throw new SparkException("Job aborted.", cause)
      }
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
      if (description.partitionColumns.isEmpty && description.bucketIdExpression.isEmpty) {
        new SingleDirectoryWriteTask(description, taskAttemptContext, committer)
      } else {
        new DynamicPartitionWriteTask(description, taskAttemptContext, committer)
      }

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Execute the task to write rows out and commit the task.
        val outputPartitions = writeTask.execute(iterator)
        writeTask.releaseResources()
        WriteTaskResult(committer.commitTask(taskAttemptContext), outputPartitions)
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
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }
  }

  /**
   * A simple trait for writing out data in a single Spark task, without any concerns about how
   * to commit or abort tasks. Exceptions thrown by the implementation of this trait will
   * automatically trigger task aborts.
   */
  private trait ExecuteWriteTask {
    /**
     * Writes data out to files, and then returns the list of partition strings written out.
     * The list of partitions is sent back to the driver and used to update the catalog.
     */
    def execute(iterator: Iterator[InternalRow]): Set[String]
    def releaseResources(): Unit
  }

  /** Writes data to a single directory (used for non-dynamic-partition writes). */
  private class SingleDirectoryWriteTask(
      description: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      committer: FileCommitProtocol) extends ExecuteWriteTask {

    private[this] var currentWriter: OutputWriter = _

    private def newOutputWriter(fileCounter: Int): Unit = {
      val ext = description.outputWriterFactory.getFileExtension(taskAttemptContext)
      val tmpFilePath = committer.newTaskTempFile(
        taskAttemptContext,
        None,
        f"-c$fileCounter%03d" + ext)

      currentWriter = description.outputWriterFactory.newInstance(
        path = tmpFilePath,
        dataSchema = description.dataColumns.toStructType,
        context = taskAttemptContext)
    }

    override def execute(iter: Iterator[InternalRow]): Set[String] = {
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
        recordsInFile += 1
      }
      releaseResources()
      Set.empty
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

    // currentWriter is initialized whenever we see a new key
    private var currentWriter: OutputWriter = _

    /** Expressions that given partition columns build a path string like: col1=val/col2=val/... */
    private def partitionPathExpression: Seq[Expression] = {
      desc.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
        val partitionName = ScalaUDF(
          ExternalCatalogUtils.getPartitionPathString _,
          StringType,
          Seq(Literal(c.name), Cast(c, StringType, Option(desc.timeZoneId))))
        if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
      }
    }

    /**
     * Opens a new OutputWriter given a partition key and optional bucket id.
     * If bucket id is specified, we will append it to the end of the file name, but before the
     * file extension, e.g. part-r-00009-ea518ad4-455a-4431-b471-d24e03814677-00002.gz.parquet
     *
     * @param partColsAndBucketId a row consisting of partition columns and a bucket id for the
     *                            current row.
     * @param getPartitionPath a function that projects the partition values into a path string.
     * @param fileCounter the number of files that have been written in the past for this specific
     *                    partition. This is used to limit the max number of records written for a
     *                    single file. The value should start from 0.
     * @param updatedPartitions the set of updated partition paths, we should add the new partition
     *                          path of this writer to it.
     */
    private def newOutputWriter(
        partColsAndBucketId: InternalRow,
        getPartitionPath: UnsafeProjection,
        fileCounter: Int,
        updatedPartitions: mutable.Set[String]): Unit = {
      val partDir = if (desc.partitionColumns.isEmpty) {
        None
      } else {
        Option(getPartitionPath(partColsAndBucketId).getString(0))
      }
      partDir.foreach(updatedPartitions.add)

      // If the bucketId expression is defined, the bucketId column is right after the partition
      // columns.
      val bucketId = if (desc.bucketIdExpression.isDefined) {
        BucketingUtils.bucketIdToString(partColsAndBucketId.getInt(desc.partitionColumns.length))
      } else {
        ""
      }

      // This must be in a form that matches our bucketing format. See BucketingUtils.
      val ext = f"$bucketId.c$fileCounter%03d" +
        desc.outputWriterFactory.getFileExtension(taskAttemptContext)

      val customPath = partDir match {
        case Some(dir) =>
          desc.customPartitionLocations.get(PartitioningUtils.parsePathFragment(dir))
        case _ =>
          None
      }
      val path = if (customPath.isDefined) {
        committer.newTaskTempFileAbsPath(taskAttemptContext, customPath.get, ext)
      } else {
        committer.newTaskTempFile(taskAttemptContext, partDir, ext)
      }

      currentWriter = desc.outputWriterFactory.newInstance(
        path = path,
        dataSchema = desc.dataColumns.toStructType,
        context = taskAttemptContext)
    }

    override def execute(iter: Iterator[InternalRow]): Set[String] = {
      val getPartitionColsAndBucketId = UnsafeProjection.create(
        desc.partitionColumns ++ desc.bucketIdExpression, desc.allColumns)

      // Generates the partition path given the row generated by `getPartitionColsAndBucketId`.
      val getPartPath = UnsafeProjection.create(
        Seq(Concat(partitionPathExpression)), desc.partitionColumns)

      // Returns the data columns to be written given an input row
      val getOutputRow = UnsafeProjection.create(desc.dataColumns, desc.allColumns)

      // If anything below fails, we should abort the task.
      var recordsInFile: Long = 0L
      var fileCounter = 0
      var currentPartColsAndBucketId: UnsafeRow = null
      val updatedPartitions = mutable.Set[String]()
      for (row <- iter) {
        val nextPartColsAndBucketId = getPartitionColsAndBucketId(row)
        if (currentPartColsAndBucketId != nextPartColsAndBucketId) {
          // See a new partition or bucket - write to a new partition dir (or a new bucket file).
          currentPartColsAndBucketId = nextPartColsAndBucketId.copy()
          logDebug(s"Writing partition: $currentPartColsAndBucketId")

          recordsInFile = 0
          fileCounter = 0

          releaseResources()
          newOutputWriter(currentPartColsAndBucketId, getPartPath, fileCounter, updatedPartitions)
        } else if (desc.maxRecordsPerFile > 0 &&
            recordsInFile >= desc.maxRecordsPerFile) {
          // Exceeded the threshold in terms of the number of records per file.
          // Create a new file by increasing the file counter.
          recordsInFile = 0
          fileCounter += 1
          assert(fileCounter < MAX_FILE_COUNTER,
            s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")

          releaseResources()
          newOutputWriter(currentPartColsAndBucketId, getPartPath, fileCounter, updatedPartitions)
        }

        currentWriter.write(getOutputRow(row))
        recordsInFile += 1
      }
      releaseResources()
      updatedPartitions.toSet
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
