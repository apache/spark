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
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution, UnsafeKVExternalSorter}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter


/** A helper object for writing FileFormat data out to a location. */
object FileFormatWriter extends Logging {

  /** Describes how output files should be placed in the filesystem. */
  case class OutputSpec(
    outputPath: String, customPartitionLocations: Map[TablePartitionSpec, String])

  /** A shared job description for all the write tasks. */
  private class WriteJobDescription(
      val uuid: String,  // prevent collision between different (appending) write jobs
      val serializableHadoopConf: SerializableConfiguration,
      val outputWriterFactory: OutputWriterFactory,
      val allColumns: Seq[Attribute],
      val partitionColumns: Seq[Attribute],
      val nonPartitionColumns: Seq[Attribute],
      val bucketSpec: Option[BucketSpec],
      val path: String,
      val customPartitionLocations: Map[TablePartitionSpec, String])
    extends Serializable {

    assert(AttributeSet(allColumns) == AttributeSet(partitionColumns ++ nonPartitionColumns),
      s"""
         |All columns: ${allColumns.mkString(", ")}
         |Partition columns: ${partitionColumns.mkString(", ")}
         |Non-partition columns: ${nonPartitionColumns.mkString(", ")}
       """.stripMargin)
  }

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

    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = queryExecution.logical.output.filterNot(partitionSet.contains)

    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, options, dataColumns.toStructType)

    val description = new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = queryExecution.logical.output,
      partitionColumns = partitionColumns,
      nonPartitionColumns = dataColumns,
      bucketSpec = bucketSpec,
      path = outputSpec.outputPath,
      customPartitionLocations = outputSpec.customPartitionLocations)

    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      // This call shouldn't be put into the `try` block below because it only initializes and
      // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
      committer.setupJob(job)

      try {
        val ret = sparkSession.sparkContext.runJob(queryExecution.toRdd,
          (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
            executeTask(
              description = description,
              sparkStageId = taskContext.stageId(),
              sparkPartitionId = taskContext.partitionId(),
              sparkAttemptNumber = taskContext.attemptNumber(),
              committer,
              iterator = iter)
          })

        val commitMsgs = ret.map(_._1)
        val updatedPartitions = ret.flatMap(_._2).distinct.map(PartitioningUtils.parsePathFragment)

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
      iterator: Iterator[InternalRow]): (TaskCommitMessage, Set[String]) = {

    val jobId = SparkHadoopWriterUtils.createJobID(new Date, sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the attempt context required to use in the output committer.
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapred.job.id", jobId.toString)
      hadoopConf.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapred.task.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapred.task.is.map", true)
      hadoopConf.setInt("mapred.task.partition", 0)

      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    committer.setupTask(taskAttemptContext)

    val writeTask =
      if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
        new SingleDirectoryWriteTask(description, taskAttemptContext, committer)
      } else {
        new DynamicPartitionWriteTask(description, taskAttemptContext, committer)
      }

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Execute the task to write rows out and commit the task.
        val outputPartitions = writeTask.execute(iterator)
        writeTask.releaseResources()
        (committer.commitTask(taskAttemptContext), outputPartitions)
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

    private[this] var outputWriter: OutputWriter = {
      val tmpFilePath = committer.newTaskTempFile(
        taskAttemptContext,
        None,
        description.outputWriterFactory.getFileExtension(taskAttemptContext))

      val outputWriter = description.outputWriterFactory.newInstance(
        path = tmpFilePath,
        dataSchema = description.nonPartitionColumns.toStructType,
        context = taskAttemptContext)
      outputWriter.initConverter(dataSchema = description.nonPartitionColumns.toStructType)
      outputWriter
    }

    override def execute(iter: Iterator[InternalRow]): Set[String] = {
      while (iter.hasNext) {
        val internalRow = iter.next()
        outputWriter.writeInternal(internalRow)
      }
      Set.empty
    }

    override def releaseResources(): Unit = {
      if (outputWriter != null) {
        outputWriter.close()
        outputWriter = null
      }
    }
  }

  /**
   * Writes data to using dynamic partition writes, meaning this single function can write to
   * multiple directories (partitions) or files (bucketing).
   */
  private class DynamicPartitionWriteTask(
      description: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      committer: FileCommitProtocol) extends ExecuteWriteTask {

    // currentWriter is initialized whenever we see a new key
    private var currentWriter: OutputWriter = _

    private val bucketColumns: Seq[Attribute] = description.bucketSpec.toSeq.flatMap {
      spec => spec.bucketColumnNames.map(c => description.allColumns.find(_.name == c).get)
    }

    private val sortColumns: Seq[Attribute] = description.bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => description.allColumns.find(_.name == c).get)
    }

    private def bucketIdExpression: Option[Expression] = description.bucketSpec.map { spec =>
      // Use `HashPartitioning.partitionIdExpression` as our bucket id expression, so that we can
      // guarantee the data distribution is same between shuffle and bucketed data source, which
      // enables us to only shuffle one side when join a bucketed table and a normal one.
      HashPartitioning(bucketColumns, spec.numBuckets).partitionIdExpression
    }

    /** Expressions that given a partition key build a string like: col1=val/col2=val/... */
    private def partitionStringExpression: Seq[Expression] = {
      description.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
        val escaped = ScalaUDF(
          ExternalCatalogUtils.escapePathName _,
          StringType,
          Seq(Cast(c, StringType)),
          Seq(StringType))
        val str = If(IsNull(c), Literal(ExternalCatalogUtils.DEFAULT_PARTITION_NAME), escaped)
        val partitionName = Literal(c.name + "=") :: str :: Nil
        if (i == 0) partitionName else Literal(Path.SEPARATOR) :: partitionName
      }
    }

    /**
     * Open and returns a new OutputWriter given a partition key and optional bucket id.
     * If bucket id is specified, we will append it to the end of the file name, but before the
     * file extension, e.g. part-r-00009-ea518ad4-455a-4431-b471-d24e03814677-00002.gz.parquet
     */
    private def newOutputWriter(key: InternalRow, partString: UnsafeProjection): OutputWriter = {
      val partDir =
        if (description.partitionColumns.isEmpty) None else Option(partString(key).getString(0))

      // If the bucket spec is defined, the bucket column is right after the partition columns
      val bucketId = if (description.bucketSpec.isDefined) {
        BucketingUtils.bucketIdToString(key.getInt(description.partitionColumns.length))
      } else {
        ""
      }
      val ext = bucketId + description.outputWriterFactory.getFileExtension(taskAttemptContext)

      val customPath = partDir match {
        case Some(dir) =>
          description.customPartitionLocations.get(PartitioningUtils.parsePathFragment(dir))
        case _ =>
          None
      }
      val path = if (customPath.isDefined) {
        committer.newTaskTempFileAbsPath(taskAttemptContext, customPath.get, ext)
      } else {
        committer.newTaskTempFile(taskAttemptContext, partDir, ext)
      }
      val newWriter = description.outputWriterFactory.newInstance(
        path = path,
        dataSchema = description.nonPartitionColumns.toStructType,
        context = taskAttemptContext)
      newWriter.initConverter(description.nonPartitionColumns.toStructType)
      newWriter
    }

    override def execute(iter: Iterator[InternalRow]): Set[String] = {
      // We should first sort by partition columns, then bucket id, and finally sorting columns.
      val sortingExpressions: Seq[Expression] =
        description.partitionColumns ++ bucketIdExpression ++ sortColumns
      val getSortingKey = UnsafeProjection.create(sortingExpressions, description.allColumns)

      val sortingKeySchema = StructType(sortingExpressions.map {
        case a: Attribute => StructField(a.name, a.dataType, a.nullable)
        // The sorting expressions are all `Attribute` except bucket id.
        case _ => StructField("bucketId", IntegerType, nullable = false)
      })

      // Returns the data columns to be written given an input row
      val getOutputRow = UnsafeProjection.create(
        description.nonPartitionColumns, description.allColumns)

      // Returns the partition path given a partition key.
      val getPartitionString = UnsafeProjection.create(
        Seq(Concat(partitionStringExpression)), description.partitionColumns)

      // Sorts the data before write, so that we only need one writer at the same time.
      val sorter = new UnsafeKVExternalSorter(
        sortingKeySchema,
        StructType.fromAttributes(description.nonPartitionColumns),
        SparkEnv.get.blockManager,
        SparkEnv.get.serializerManager,
        TaskContext.get().taskMemoryManager().pageSizeBytes,
        SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold",
          UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD))

      while (iter.hasNext) {
        val currentRow = iter.next()
        sorter.insertKV(getSortingKey(currentRow), getOutputRow(currentRow))
      }
      logInfo(s"Sorting complete. Writing out partition files one at a time.")

      val getBucketingKey: InternalRow => InternalRow = if (sortColumns.isEmpty) {
        identity
      } else {
        UnsafeProjection.create(sortingExpressions.dropRight(sortColumns.length).zipWithIndex.map {
          case (expr, ordinal) => BoundReference(ordinal, expr.dataType, expr.nullable)
        })
      }

      val sortedIterator = sorter.sortedIterator()

      // If anything below fails, we should abort the task.
      var currentKey: UnsafeRow = null
      val updatedPartitions = mutable.Set[String]()
      while (sortedIterator.next()) {
        val nextKey = getBucketingKey(sortedIterator.getKey).asInstanceOf[UnsafeRow]
        if (currentKey != nextKey) {
          if (currentWriter != null) {
            currentWriter.close()
            currentWriter = null
          }
          currentKey = nextKey.copy()
          logDebug(s"Writing partition: $currentKey")

          currentWriter = newOutputWriter(currentKey, getPartitionString)
          val partitionPath = getPartitionString(currentKey).getString(0)
          if (partitionPath.nonEmpty) {
            updatedPartitions.add(partitionPath)
          }
        }
        currentWriter.writeInternal(sortedIterator.getValue)
      }
      if (currentWriter != null) {
        currentWriter.close()
        currentWriter = null
      }
      updatedPartitions.toSet
    }

    override def releaseResources(): Unit = {
      if (currentWriter != null) {
        currentWriter.close()
        currentWriter = null
      }
    }
  }
}
