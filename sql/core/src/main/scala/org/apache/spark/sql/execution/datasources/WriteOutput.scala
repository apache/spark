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
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SQLExecution, UnsafeKVExternalSorter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter


/**
 * A helper object for writing data out to an existing partition.
 */
object WriteOutput extends Logging {

  /** A shared job description for all the write tasks. */
  private class WriteJobDescription(
      val serializableHadoopConf: SerializableConfiguration,
      val outputWriterFactory: OutputWriterFactory,
      val allColumns: Seq[Attribute],
      val partitionColumns: Seq[Attribute],
      val nonPartitionColumns: Seq[Attribute],
      val bucketSpec: Option[BucketSpec],
      val isAppend: Boolean,
      val path: String,
      val outputFormatClass: Class[_ <: OutputFormat[_, _]])
    extends Serializable {

    assert(allColumns.toSet == (partitionColumns ++ nonPartitionColumns).toSet)
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
      plan: LogicalPlan,
      fileFormat: FileFormat,
      outputPath: Path,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      refreshFunction: () => Unit,
      options: Map[String, String],
      isAppend: Boolean): Unit = {

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, outputPath)

    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = plan.output.filterNot(partitionSet.contains)
    val queryExecution = Dataset.ofRows(sparkSession, plan).queryExecution

    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, options, dataColumns.toStructType)

    val description = new WriteJobDescription(
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = plan.output,
      partitionColumns = partitionColumns,
      nonPartitionColumns = dataColumns,
      bucketSpec = bucketSpec,
      isAppend = isAppend,
      path = outputPath.toString,
      outputFormatClass = job.getOutputFormatClass)

    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      // This call shouldn't be put into the `try` block below because it only initializes and
      // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
      val committer = setupDriverCommitter(job, outputPath.toString, isAppend)

      try {
        sparkSession.sparkContext.runJob(queryExecution.toRdd,
          (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
            executeTask(
              description = description,
              sparkStageId = taskContext.stageId(),
              sparkPartitionId = taskContext.partitionId(),
              sparkAttemptNumber = taskContext.attemptNumber(),
              iterator = iter)
          })

        committer.commitJob(job)
        logInfo(s"Job ${job.getJobID} committed.")

        refreshFunction()
      } catch { case cause: Throwable =>
        logError(s"Aborting job ${job.getJobID}.", cause)
        committer.abortJob(job, JobStatus.State.FAILED)

        throw new SparkException("Job aborted.", cause)
      }
    }
  }

  /**
   * Writes data out in a single Spark task.
   */
  def executeTask(
      description: WriteJobDescription,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      iterator: Iterator[InternalRow]): Unit = {

    val jobId = SparkHadoopWriter.createJobID(new Date, sparkStageId)
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

    val committer = newOutputCommitter(
      description.outputFormatClass, taskAttemptContext, description.path, description.isAppend)
    committer.setupTask(taskAttemptContext)

    // Figure out where we need to write data to for staging.
    // For FileOutputCommitter it has its own staging path called "work path".
    val stagingPath = committer match {
      case f: FileOutputCommitter => f.getWorkPath.toString
      case _ => description.path
    }

    val writeTask =
      if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
        new SingleDirectoryWriteTask(description, taskAttemptContext, stagingPath)
      } else {
        new DynamicPartitionWriteTask(description, taskAttemptContext, stagingPath)
      }

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Execute the task to write rows out
        writeTask.execute(iterator)
        writeTask.releaseResources()

        // Commit the task
        SparkHadoopMapRedUtil.commitTask(committer, taskAttemptContext, jobId.getId, taskId.getId)
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

  trait ExecuteWriteTask {
    def execute(iterator: Iterator[InternalRow]): Unit

    def releaseResources(): Unit
  }

  /** Writes data to a single directory (used for non-dynamic-partition writes). */
  private class SingleDirectoryWriteTask(
      description: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      stagingPath: String) extends ExecuteWriteTask {

    private[this] var outputWriter: OutputWriter = {
      val outputWriter = description.outputWriterFactory.newInstance(
        path = stagingPath,
        bucketId = None,
        dataSchema = description.nonPartitionColumns.toStructType,
        context = taskAttemptContext)
      outputWriter.initConverter(dataSchema = description.nonPartitionColumns.toStructType)
      outputWriter
    }

    override def execute(iter: Iterator[InternalRow]): Unit = {

      while (iter.hasNext) {
        val internalRow = iter.next()
        outputWriter.writeInternal(internalRow)
      }
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
      stagingPath: String) extends ExecuteWriteTask {

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
        val escaped =
          ScalaUDF(
            PartitioningUtils.escapePathName _,
            StringType,
            Seq(Cast(c, StringType)),
            Seq(StringType))
        val str = If(IsNull(c), Literal(PartitioningUtils.DEFAULT_PARTITION_NAME), escaped)
        val partitionName = Literal(c.name + "=") :: str :: Nil
        if (i == 0) partitionName else Literal(Path.SEPARATOR) :: partitionName
      }
    }

    private def getBucketIdFromKey(key: InternalRow): Option[Int] =
      description.bucketSpec.map { _ => key.getInt(description.partitionColumns.length) }

    /**
     * Open and returns a new OutputWriter given a partition key and optional bucket id.
     * If bucket id is specified, we will append it to the end of the file name, but before the
     * file extension, e.g. part-r-00009-ea518ad4-455a-4431-b471-d24e03814677-00002.gz.parquet
     */
    private def newOutputWriter(
        key: InternalRow,
        getPartitionString: UnsafeProjection): OutputWriter = {
      val path =
        if (description.partitionColumns.nonEmpty) {
          val partitionPath = getPartitionString(key).getString(0)
          new Path(stagingPath, partitionPath).toString
        } else {
          stagingPath
        }
      val bucketId = getBucketIdFromKey(key)

      val newWriter = description.outputWriterFactory.newInstance(
        path = path,
        bucketId = bucketId,
        dataSchema = description.nonPartitionColumns.toStructType,
        context = taskAttemptContext)
      newWriter.initConverter(description.nonPartitionColumns.toStructType)
      newWriter
    }

    override def execute(iter: Iterator[InternalRow]): Unit = {
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
      val getPartitionString =
      UnsafeProjection.create(Seq(Concat(partitionStringExpression)), description.partitionColumns)

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
        }
        currentWriter.writeInternal(sortedIterator.getValue)
      }
      if (currentWriter != null) {
        currentWriter.close()
        currentWriter = null
      }
    }

    override def releaseResources(): Unit = {
      if (currentWriter != null) {
        currentWriter.close()
        currentWriter = null
      }
    }
  }

  def setupDriverCommitter(job: Job, path: String, isAppend: Boolean): OutputCommitter = {
    // Setup IDs
    val jobId = SparkHadoopWriter.createJobID(new Date, 0)
    val taskId = new TaskID(jobId, TaskType.MAP, 0)
    val taskAttemptId = new TaskAttemptID(taskId, 0)

    // Set up the configuration object
    job.getConfiguration.set("mapred.job.id", jobId.toString)
    job.getConfiguration.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
    job.getConfiguration.set("mapred.task.id", taskAttemptId.toString)
    job.getConfiguration.setBoolean("mapred.task.is.map", true)
    job.getConfiguration.setInt("mapred.task.partition", 0)

    // This UUID is sent to executor side together with the serialized `Configuration` object within
    // the `Job` instance.  `OutputWriters` on the executor side should use this UUID to generate
    // unique task output files.
    // This UUID is used to avoid output file name collision between different appending write jobs.
    // These jobs may belong to different SparkContext instances. Concrete data source
    // implementations may use this UUID to generate unique file names (e.g.,
    // `part-r-<task-id>-<job-uuid>.parquet`). The reason why this ID is used to identify a job
    // rather than a single task output file is that, speculative tasks must generate the same
    // output file name as the original task.
    job.getConfiguration.set(WriterContainer.DATASOURCE_WRITEJOBUUID, UUID.randomUUID().toString)

    val taskAttemptContext = new TaskAttemptContextImpl(job.getConfiguration, taskAttemptId)
    val outputCommitter = newOutputCommitter(
      job.getOutputFormatClass, taskAttemptContext, path, isAppend)
    outputCommitter.setupJob(job)
    outputCommitter
  }

  private def newOutputCommitter(
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      context: TaskAttemptContext,
      path: String,
      isAppend: Boolean): OutputCommitter = {
    val defaultOutputCommitter = outputFormatClass.newInstance().getOutputCommitter(context)

    if (isAppend) {
      // If we are appending data to an existing dir, we will only use the output committer
      // associated with the file output format since it is not safe to use a custom
      // committer for appending. For example, in S3, direct parquet output committer may
      // leave partial data in the destination dir when the appending job fails.
      // See SPARK-8578 for more details
      logInfo(
        s"Using default output committer ${defaultOutputCommitter.getClass.getCanonicalName} " +
          "for appending.")
      defaultOutputCommitter
    } else {
      val configuration = context.getConfiguration
      val clazz =
        configuration.getClass(SQLConf.OUTPUT_COMMITTER_CLASS.key, null, classOf[OutputCommitter])

      if (clazz != null) {
        logInfo(s"Using user defined output committer class ${clazz.getCanonicalName}")

        // Every output format based on org.apache.hadoop.mapreduce.lib.output.OutputFormat
        // has an associated output committer. To override this output committer,
        // we will first try to use the output committer set in SQLConf.OUTPUT_COMMITTER_CLASS.
        // If a data source needs to override the output committer, it needs to set the
        // output committer in prepareForWrite method.
        if (classOf[FileOutputCommitter].isAssignableFrom(clazz)) {
          // The specified output committer is a FileOutputCommitter.
          // So, we will use the FileOutputCommitter-specified constructor.
          val ctor = clazz.getDeclaredConstructor(classOf[Path], classOf[TaskAttemptContext])
          ctor.newInstance(new Path(path), context)
        } else {
          // The specified output committer is just an OutputCommitter.
          // So, we will use the no-argument constructor.
          val ctor = clazz.getDeclaredConstructor()
          ctor.newInstance()
        }
      } else {
        // If output committer class is not set, we will use the one associated with the
        // file output format.
        logInfo(
          s"Using output committer class ${defaultOutputCommitter.getClass.getCanonicalName}")
        defaultOutputCommitter
      }
    }
  }
}

object WriterContainer {
  val DATASOURCE_WRITEJOBUUID = "spark.sql.sources.writeJobUUID"
}
