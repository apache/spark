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
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter => MapReduceFileOutputCommitter}
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
 * A helper object for writing data out to an existing partition. This is a work-in-progress
 * refactoring. The goal is to eventually remove the class hierarchy in WriterContainer.
 */
object WriteOutput extends Logging {

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

    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = plan.output.filterNot(partitionSet.contains)
    val queryExecution = Dataset.ofRows(sparkSession, plan).queryExecution

    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, options, dataColumns.toStructType)

    val context = new WriteJobDescription(
      outputWriterFactory = outputWriterFactory,
      allColumns = plan.output,
      partitionColumns = partitionColumns,
      nonPartitionColumns = dataColumns,
      isAppend = isAppend,
      path = outputPath,
      outputFormatClass = job.getOutputFormatClass,
      bucketSpec = bucketSpec)

    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)

    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {

      // This call shouldn't be put into the `try` block below because it only initializes and
      // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
      val committer = setupDriverCommitter(hadoopConf, job, outputPath, isAppend)

      try {
        sparkSession.sparkContext.runJob(queryExecution.toRdd,
          (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
            executeTask(context, serializableHadoopConf.value, taskContext, iter)
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

  class WriteJobDescription(
      val outputWriterFactory: OutputWriterFactory,
      val allColumns: Seq[Attribute],
      val partitionColumns: Seq[Attribute],
      val nonPartitionColumns: Seq[Attribute],
      val isAppend: Boolean,
      val path: Path,
      val outputFormatClass: Class[_ <: OutputFormat[_, _]],
      val bucketSpec: Option[BucketSpec])
    extends Serializable

  def executeTask(
      description: WriteJobDescription,
      hadoopConf: Configuration,
      taskContext: TaskContext,
      iterator: Iterator[InternalRow]): Unit = {
    // Setup IDs
    val jobId = SparkHadoopWriter.createJobID(new Date, taskContext.stageId())
    val taskId = new TaskID(jobId, TaskType.MAP, taskContext.partitionId())
    val taskAttemptId = new TaskAttemptID(taskId, taskContext.attemptNumber())

    // Set up the configuration object
    hadoopConf.set("mapred.job.id", jobId.toString)
    hadoopConf.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
    hadoopConf.set("mapred.task.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapred.task.is.map", true)
    hadoopConf.setInt("mapred.task.partition", 0)

    val taskAttemptContext = new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    val committer = newOutputCommitter(
      description.outputFormatClass, taskAttemptContext, description.path, description.isAppend)
    committer.setupTask(taskAttemptContext)

    if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
      executeSingleDirectoryWriteTask(
        description,
        taskAttemptContext,
        committer,
        iterator)
    } else {
      executeDynamicPartitionWriteTask(
        description,
        taskAttemptContext,
        committer,
        iterator)
    }
  }

  def executeSingleDirectoryWriteTask(
      description: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      committer: OutputCommitter,
      iterator: Iterator[InternalRow]): Unit = {

    val jobId = taskAttemptContext.getJobID
    val taskId = taskAttemptContext.getTaskAttemptID.getTaskID
    val schema = description.nonPartitionColumns.toStructType
    var writer = createOutputWriter(description, taskAttemptContext, committer)
    writer.initConverter(schema)

    // If anything below fails, we should abort the task.
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks {
        while (iterator.hasNext) {
          val internalRow = iterator.next()
          writer.writeInternal(internalRow)
        }

        try {
          if (writer != null) {
            writer.close()
            writer = null
          }
          SparkHadoopMapRedUtil.commitTask(committer, taskAttemptContext, jobId.getId, taskId.getId)
        } catch {
          case cause: Throwable =>
            // This exception will be handled in `InsertIntoHadoopFsRelation.insert$writeRows`, and
            // will cause `abortTask()` to be invoked.
            throw new RuntimeException("Failed to commit task", cause)
        }
      }(catchBlock = {
        try {
          if (writer != null) {
            writer.close()
          }
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

  def executeDynamicPartitionWriteTask(
      description: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      committer: OutputCommitter,
      iterator: Iterator[InternalRow]): Unit = {

    val bucketColumns: Seq[Attribute] = description.bucketSpec.toSeq.flatMap {
      spec => spec.bucketColumnNames.map(c => description.allColumns.find(_.name == c).get)
    }

    val sortColumns: Seq[Attribute] = description.bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => description.allColumns.find(_.name == c).get)
    }

    def bucketIdExpression: Option[Expression] = description.bucketSpec.map { spec =>
      // Use `HashPartitioning.partitionIdExpression` as our bucket id expression, so that we can
      // guarantee the data distribution is same between shuffle and bucketed data source, which
      // enables us to only shuffle one side when join a bucketed table and a normal one.
      HashPartitioning(bucketColumns, spec.numBuckets).partitionIdExpression
    }

    // Expressions that given a partition key build a string like: col1=val/col2=val/...
    def partitionStringExpression: Seq[Expression] = {
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

    def getBucketIdFromKey(key: InternalRow): Option[Int] = description.bucketSpec.map { _ =>
      key.getInt(description.partitionColumns.length)
    }

    /*
     * Open and returns a new OutputWriter given a partition key and optional bucket id.
     * If bucket id is specified, we will append it to the end of the file name, but before the
     * file extension, e.g. part-r-00009-ea518ad4-455a-4431-b471-d24e03814677-00002.gz.parquet
     */
    def newOutputWriter(
      key: InternalRow,
      getPartitionString: UnsafeProjection): OutputWriter = {
      val basePath = getWorkPath(committer, description.path.toString)
      val path =
        if (description.partitionColumns.nonEmpty) {
          val partitionPath = getPartitionString(key).getString(0)
          new Path(basePath, partitionPath).toString
        } else {
          basePath
        }
      val bucketId = getBucketIdFromKey(key)
      val newWriter = createOutputWriter(description, taskAttemptContext, committer, bucketId)
      newWriter.initConverter(description.nonPartitionColumns.toStructType)
      newWriter
    }

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
    val getOutputRow =
      UnsafeProjection.create(description.nonPartitionColumns, description.allColumns)

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

    while (iterator.hasNext) {
      val currentRow = iterator.next()
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
    var currentWriter: OutputWriter = null
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks {
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

        // Commit task
        val jobId = taskAttemptContext.getJobID
        val taskId = taskAttemptContext.getTaskAttemptID.getTaskID
        SparkHadoopMapRedUtil.commitTask(committer, taskAttemptContext, jobId.getId, taskId.getId)
      }(catchBlock = {
        if (currentWriter != null) {
          currentWriter.close()
        }

        committer.abortTask(taskAttemptContext)
      })
    } catch {
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }

  }  // end of executeDynamicPartitionWrite

  def getWorkPath(committer: OutputCommitter, defaultPath: String): String = {
    committer match {
      // FileOutputCommitter writes to a temporary location returned by `getWorkPath`.
      case f: MapReduceFileOutputCommitter => f.getWorkPath.toString
      case _ => defaultPath
    }
  }

  def createOutputWriter(
      description: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      committer: OutputCommitter,
      bucketId: Option[Int] = None): OutputWriter = {
    try {
      description.outputWriterFactory.newInstance(
        description.path.toString,
        bucketId,
        description.nonPartitionColumns.toStructType,
        taskAttemptContext)
    } catch {
      case e: org.apache.hadoop.fs.FileAlreadyExistsException =>
        if (committer.getClass.getName.contains("Direct")) {
          // SPARK-11382: DirectParquetOutputCommitter is not idempotent, meaning on retry
          // attempts, the task will fail because the output file is created from a prior attempt.
          // This often means the most visible error to the user is misleading. Augment the error
          // to tell the user to look for the actual error.
          throw new SparkException("The output file already exists but this could be due to a " +
            "failure from an earlier attempt. Look through the earlier logs or stage page for " +
            "the first error.\n  File exists error: " + e, e)
        } else {
          throw e
        }
    }
  }

  def setupDriverCommitter(hadoopConf: Configuration, job: Job, path: Path, isAppend: Boolean)
    : OutputCommitter = {
    // Setup IDs
    val jobId = SparkHadoopWriter.createJobID(new Date, 0)
    val taskId = new TaskID(jobId, TaskType.MAP, 0)
    val taskAttemptId = new TaskAttemptID(taskId, 0)

    // Set up the configuration object
    hadoopConf.set("mapred.job.id", jobId.toString)
    hadoopConf.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
    hadoopConf.set("mapred.task.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapred.task.is.map", true)
    hadoopConf.setInt("mapred.task.partition", 0)

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

    val taskAttemptContext = new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    val outputCommitter = newOutputCommitter(
      job.getOutputFormatClass, taskAttemptContext, path, isAppend)
    outputCommitter.setupJob(job)
    outputCommitter
  }

  private def newOutputCommitter(
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      context: TaskAttemptContext,
      outputPath: Path,
      isAppend: Boolean): OutputCommitter = {
    val defaultOutputCommitter = outputFormatClass.newInstance().getOutputCommitter(context)

    if (isAppend) {
      // If we are appending data to an existing dir, we will only use the output committer
      // associated with the file output format since it is not safe to use a custom
      // committer for appending. For example, in S3, direct parquet output committer may
      // leave partial data in the destination dir when the appending job fails.
      //
      // See SPARK-8578 for more details
      logInfo(
        s"Using default output committer ${defaultOutputCommitter.getClass.getCanonicalName} " +
          "for appending.")
      defaultOutputCommitter
    } else {
      val configuration = context.getConfiguration
      val committerClass = configuration.getClass(
        SQLConf.OUTPUT_COMMITTER_CLASS.key, null, classOf[OutputCommitter])

      Option(committerClass).map { clazz =>
        logInfo(s"Using user defined output committer class ${clazz.getCanonicalName}")

        // Every output format based on org.apache.hadoop.mapreduce.lib.output.OutputFormat
        // has an associated output committer. To override this output committer,
        // we will first try to use the output committer set in SQLConf.OUTPUT_COMMITTER_CLASS.
        // If a data source needs to override the output committer, it needs to set the
        // output committer in prepareForWrite method.
        if (classOf[MapReduceFileOutputCommitter].isAssignableFrom(clazz)) {
          // The specified output committer is a FileOutputCommitter.
          // So, we will use the FileOutputCommitter-specified constructor.
          val ctor = clazz.getDeclaredConstructor(classOf[Path], classOf[TaskAttemptContext])
          ctor.newInstance(outputPath, context)
        } else {
          // The specified output committer is just an OutputCommitter.
          // So, we will use the no-argument constructor.
          val ctor = clazz.getDeclaredConstructor()
          ctor.newInstance()
        }
      }.getOrElse {
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
