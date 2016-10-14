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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter => MapReduceFileOutputCommitter}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter


/** A container for all the details required when writing to a table. */
private[datasources] case class WriteRelation(
    sparkSession: SparkSession,
    dataSchema: StructType,
    path: String,
    prepareJobForWrite: Job => OutputWriterFactory,
    bucketSpec: Option[BucketSpec])

object WriterContainer {
  val DATASOURCE_WRITEJOBUUID = "spark.sql.sources.writeJobUUID"
}

private[datasources] abstract class BaseWriterContainer(
    @transient val relation: WriteRelation,
    @transient private val job: Job,
    isAppend: Boolean)
  extends Logging with Serializable {

  protected val dataSchema = relation.dataSchema

  protected val serializableConf =
    new SerializableConfiguration(job.getConfiguration)

  // This UUID is used to avoid output file name collision between different appending write jobs.
  // These jobs may belong to different SparkContext instances. Concrete data source implementations
  // may use this UUID to generate unique file names (e.g., `part-r-<task-id>-<job-uuid>.parquet`).
  //  The reason why this ID is used to identify a job rather than a single task output file is
  // that, speculative tasks must generate the same output file name as the original task.
  private val uniqueWriteJobId = UUID.randomUUID()

  // This is only used on driver side.
  @transient private val jobContext: JobContext = job

  // The following fields are initialized and used on both driver and executor side.
  @transient protected var outputCommitter: OutputCommitter = _
  @transient private var jobId: JobID = _
  @transient private var taskId: TaskID = _
  @transient private var taskAttemptId: TaskAttemptID = _
  @transient protected var taskAttemptContext: TaskAttemptContext = _

  protected val outputPath: String = relation.path

  protected var outputWriterFactory: OutputWriterFactory = _

  private var outputFormatClass: Class[_ <: OutputFormat[_, _]] = _

  def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit

  def driverSideSetup(): Unit = {
    setupIDs(0, 0, 0)
    setupConf()

    // This UUID is sent to executor side together with the serialized `Configuration` object within
    // the `Job` instance.  `OutputWriters` on the executor side should use this UUID to generate
    // unique task output files.
    job.getConfiguration.set(WriterContainer.DATASOURCE_WRITEJOBUUID, uniqueWriteJobId.toString)

    // Order of the following two lines is important.  For Hadoop 1, TaskAttemptContext constructor
    // clones the Configuration object passed in.  If we initialize the TaskAttemptContext first,
    // configurations made in prepareJobForWrite(job) are not populated into the TaskAttemptContext.
    //
    // Also, the `prepareJobForWrite` call must happen before initializing output format and output
    // committer, since their initialization involve the job configuration, which can be potentially
    // decorated in `prepareJobForWrite`.
    outputWriterFactory = relation.prepareJobForWrite(job)
    taskAttemptContext = new TaskAttemptContextImpl(serializableConf.value, taskAttemptId)

    outputFormatClass = job.getOutputFormatClass
    outputCommitter = newOutputCommitter(taskAttemptContext)
    outputCommitter.setupJob(jobContext)
  }

  def executorSideSetup(taskContext: TaskContext): Unit = {
    setupIDs(taskContext.stageId(), taskContext.partitionId(), taskContext.attemptNumber())
    setupConf()
    taskAttemptContext = new TaskAttemptContextImpl(serializableConf.value, taskAttemptId)
    outputCommitter = newOutputCommitter(taskAttemptContext)
    outputCommitter.setupTask(taskAttemptContext)
  }

  protected def getWorkPath: String = {
    outputCommitter match {
      // FileOutputCommitter writes to a temporary location returned by `getWorkPath`.
      case f: MapReduceFileOutputCommitter => f.getWorkPath.toString
      case _ => outputPath
    }
  }

  protected def newOutputWriter(path: String, bucketId: Option[Int] = None): OutputWriter = {
    try {
      outputWriterFactory.newInstance(path, bucketId, dataSchema, taskAttemptContext)
    } catch {
      case e: org.apache.hadoop.fs.FileAlreadyExistsException =>
        if (outputCommitter.getClass.getName.contains("Direct")) {
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

  private def newOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
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
          ctor.newInstance(new Path(outputPath), context)
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

  private def setupIDs(jobId: Int, splitId: Int, attemptId: Int): Unit = {
    this.jobId = SparkHadoopWriter.createJobID(new Date, jobId)
    this.taskId = new TaskID(this.jobId, TaskType.MAP, splitId)
    this.taskAttemptId = new TaskAttemptID(taskId, attemptId)
  }

  private def setupConf(): Unit = {
    serializableConf.value.set("mapred.job.id", jobId.toString)
    serializableConf.value.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
    serializableConf.value.set("mapred.task.id", taskAttemptId.toString)
    serializableConf.value.setBoolean("mapred.task.is.map", true)
    serializableConf.value.setInt("mapred.task.partition", 0)
  }

  def commitTask(): Unit = {
    SparkHadoopMapRedUtil.commitTask(outputCommitter, taskAttemptContext, jobId.getId, taskId.getId)
  }

  def abortTask(): Unit = {
    if (outputCommitter != null) {
      outputCommitter.abortTask(taskAttemptContext)
    }
    logError(s"Task attempt $taskAttemptId aborted.")
  }

  def commitJob(): Unit = {
    outputCommitter.commitJob(jobContext)
    logInfo(s"Job $jobId committed.")
  }

  def abortJob(): Unit = {
    if (outputCommitter != null) {
      outputCommitter.abortJob(jobContext, JobStatus.State.FAILED)
    }
    logError(s"Job $jobId aborted.")
  }
}

/**
 * A writer that writes all of the rows in a partition to a single file.
 */
private[datasources] class DefaultWriterContainer(
    relation: WriteRelation,
    job: Job,
    isAppend: Boolean)
  extends BaseWriterContainer(relation, job, isAppend) {

  def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit = {
    executorSideSetup(taskContext)
    var writer = newOutputWriter(getWorkPath)
    writer.initConverter(dataSchema)

    // If anything below fails, we should abort the task.
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks {
        while (iterator.hasNext) {
          val internalRow = iterator.next()
          writer.writeInternal(internalRow)
        }
        commitTask()
      }(catchBlock = abortTask())
    } catch {
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }

    def commitTask(): Unit = {
      try {
        if (writer != null) {
          writer.close()
          writer = null
        }
        super.commitTask()
      } catch {
        case cause: Throwable =>
          // This exception will be handled in `InsertIntoHadoopFsRelation.insert$writeRows`, and
          // will cause `abortTask()` to be invoked.
          throw new RuntimeException("Failed to commit task", cause)
      }
    }

    def abortTask(): Unit = {
      try {
        if (writer != null) {
          writer.close()
        }
      } finally {
        super.abortTask()
      }
    }
  }
}

/**
 * A writer that dynamically opens files based on the given partition columns.  Internally this is
 * done by maintaining a HashMap of open files until `maxFiles` is reached.  If this occurs, the
 * writer externally sorts the remaining rows and then writes out them out one file at a time.
 */
private[datasources] class DynamicPartitionWriterContainer(
    relation: WriteRelation,
    job: Job,
    partitionColumns: Seq[Attribute],
    dataColumns: Seq[Attribute],
    inputSchema: Seq[Attribute],
    defaultPartitionName: String,
    maxOpenFiles: Int,
    isAppend: Boolean)
  extends BaseWriterContainer(relation, job, isAppend) {

  private val bucketSpec = relation.bucketSpec

  private val bucketColumns: Seq[Attribute] = bucketSpec.toSeq.flatMap {
    spec => spec.bucketColumnNames.map(c => inputSchema.find(_.name == c).get)
  }

  private val sortColumns: Seq[Attribute] = bucketSpec.toSeq.flatMap {
    spec => spec.sortColumnNames.map(c => inputSchema.find(_.name == c).get)
  }

  private def bucketIdExpression: Option[Expression] = bucketSpec.map { spec =>
    // Use `HashPartitioning.partitionIdExpression` as our bucket id expression, so that we can
    // guarantee the data distribution is same between shuffle and bucketed data source, which
    // enables us to only shuffle one side when join a bucketed table and a normal one.
    HashPartitioning(bucketColumns, spec.numBuckets).partitionIdExpression
  }

  // Expressions that given a partition key build a string like: col1=val/col2=val/...
  private def partitionStringExpression: Seq[Expression] = {
    partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val escaped =
        ScalaUDF(
          PartitioningUtils.escapePathName _,
          StringType,
          Seq(Cast(c, StringType)),
          Seq(StringType))
      val str = If(IsNull(c), Literal(defaultPartitionName), escaped)
      val partitionName = Literal(c.name + "=") :: str :: Nil
      if (i == 0) partitionName else Literal(Path.SEPARATOR) :: partitionName
    }
  }

  private def getBucketIdFromKey(key: InternalRow): Option[Int] = bucketSpec.map { _ =>
    key.getInt(partitionColumns.length)
  }

  /**
   * Open and returns a new OutputWriter given a partition key and optional bucket id.
   * If bucket id is specified, we will append it to the end of the file name, but before the
   * file extension, e.g. part-r-00009-ea518ad4-455a-4431-b471-d24e03814677-00002.gz.parquet
   */
  private def newOutputWriter(
      key: InternalRow,
      getPartitionString: UnsafeProjection): OutputWriter = {
    val path = if (partitionColumns.nonEmpty) {
      val partitionPath = getPartitionString(key).getString(0)
      new Path(getWorkPath, partitionPath).toString
    } else {
      getWorkPath
    }
    val bucketId = getBucketIdFromKey(key)
    val newWriter = super.newOutputWriter(path, bucketId)
    newWriter.initConverter(dataSchema)
    newWriter
  }

  def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit = {
    executorSideSetup(taskContext)

    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    val sortingExpressions: Seq[Expression] = partitionColumns ++ bucketIdExpression ++ sortColumns
    val getSortingKey = UnsafeProjection.create(sortingExpressions, inputSchema)

    val sortingKeySchema = StructType(sortingExpressions.map {
      case a: Attribute => StructField(a.name, a.dataType, a.nullable)
      // The sorting expressions are all `Attribute` except bucket id.
      case _ => StructField("bucketId", IntegerType, nullable = false)
    })

    // Returns the data columns to be written given an input row
    val getOutputRow = UnsafeProjection.create(dataColumns, inputSchema)

    // Returns the partition path given a partition key.
    val getPartitionString =
      UnsafeProjection.create(Concat(partitionStringExpression) :: Nil, partitionColumns)

    // Sorts the data before write, so that we only need one writer at the same time.
    // TODO: inject a local sort operator in planning.
    val sorter = new UnsafeKVExternalSorter(
      sortingKeySchema,
      StructType.fromAttributes(dataColumns),
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

        commitTask()
      }(catchBlock = {
        if (currentWriter != null) {
          currentWriter.close()
        }
        abortTask()
      })
    } catch {
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }
  }
}
