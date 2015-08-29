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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter => MapReduceFileOutputCommitter}
import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.sources.{HadoopFsRelation, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.{StructType, StringType}
import org.apache.spark.util.SerializableConfiguration


private[sql] abstract class BaseWriterContainer(
    @transient val relation: HadoopFsRelation,
    @transient job: Job,
    isAppend: Boolean)
  extends SparkHadoopMapReduceUtil
  with Logging
  with Serializable {

  protected val dataSchema = relation.dataSchema

  protected val serializableConf = new SerializableConfiguration(job.getConfiguration)

  // This UUID is used to avoid output file name collision between different appending write jobs.
  // These jobs may belong to different SparkContext instances. Concrete data source implementations
  // may use this UUID to generate unique file names (e.g., `part-r-<task-id>-<job-uuid>.parquet`).
  //  The reason why this ID is used to identify a job rather than a single task output file is
  // that, speculative tasks must generate the same output file name as the original task.
  private val uniqueWriteJobId = UUID.randomUUID()

  // This is only used on driver side.
  @transient private val jobContext: JobContext = job

  private val speculationEnabled: Boolean =
    relation.sqlContext.sparkContext.conf.getBoolean("spark.speculation", defaultValue = false)

  // The following fields are initialized and used on both driver and executor side.
  @transient protected var outputCommitter: OutputCommitter = _
  @transient private var jobId: JobID = _
  @transient private var taskId: TaskID = _
  @transient private var taskAttemptId: TaskAttemptID = _
  @transient protected var taskAttemptContext: TaskAttemptContext = _

  protected val outputPath: String = {
    assert(
      relation.paths.length == 1,
      s"Cannot write to multiple destinations: ${relation.paths.mkString(",")}")
    relation.paths.head
  }

  protected var outputWriterFactory: OutputWriterFactory = _

  private var outputFormatClass: Class[_ <: OutputFormat[_, _]] = _

  def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit

  def driverSideSetup(): Unit = {
    setupIDs(0, 0, 0)
    setupConf()

    // This UUID is sent to executor side together with the serialized `Configuration` object within
    // the `Job` instance.  `OutputWriters` on the executor side should use this UUID to generate
    // unique task output files.
    job.getConfiguration.set("spark.sql.sources.writeJobUUID", uniqueWriteJobId.toString)

    // Order of the following two lines is important.  For Hadoop 1, TaskAttemptContext constructor
    // clones the Configuration object passed in.  If we initialize the TaskAttemptContext first,
    // configurations made in prepareJobForWrite(job) are not populated into the TaskAttemptContext.
    //
    // Also, the `prepareJobForWrite` call must happen before initializing output format and output
    // committer, since their initialization involve the job configuration, which can be potentially
    // decorated in `prepareJobForWrite`.
    outputWriterFactory = relation.prepareJobForWrite(job)
    taskAttemptContext = newTaskAttemptContext(serializableConf.value, taskAttemptId)

    outputFormatClass = job.getOutputFormatClass
    outputCommitter = newOutputCommitter(taskAttemptContext)
    outputCommitter.setupJob(jobContext)
  }

  def executorSideSetup(taskContext: TaskContext): Unit = {
    setupIDs(taskContext.stageId(), taskContext.partitionId(), taskContext.attemptNumber())
    setupConf()
    taskAttemptContext = newTaskAttemptContext(serializableConf.value, taskAttemptId)
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

  private def newOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    val defaultOutputCommitter = outputFormatClass.newInstance().getOutputCommitter(context)

    if (isAppend) {
      // If we are appending data to an existing dir, we will only use the output committer
      // associated with the file output format since it is not safe to use a custom
      // committer for appending. For example, in S3, direct parquet output committer may
      // leave partial data in the destination dir when the the appending job fails.
      //
      // See SPARK-8578 for more details
      logInfo(
        s"Using default output committer ${defaultOutputCommitter.getClass.getCanonicalName} " +
          "for appending.")
      defaultOutputCommitter
    } else if (speculationEnabled) {
      // When speculation is enabled, it's not safe to use customized output committer classes,
      // especially direct output committers (e.g. `DirectParquetOutputCommitter`).
      //
      // See SPARK-9899 for more details.
      logInfo(
        s"Using default output committer ${defaultOutputCommitter.getClass.getCanonicalName} " +
          "because spark.speculation is configured to be true.")
      defaultOutputCommitter
    } else {
      val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
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
          // The specified output committer is just a OutputCommitter.
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
    this.taskId = new TaskID(this.jobId, true, splitId)
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
    SparkHadoopMapRedUtil.commitTask(
      outputCommitter, taskAttemptContext, jobId.getId, taskId.getId, taskAttemptId.getId)
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
private[sql] class DefaultWriterContainer(
    @transient relation: HadoopFsRelation,
    @transient job: Job,
    isAppend: Boolean)
  extends BaseWriterContainer(relation, job, isAppend) {

  def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit = {
    executorSideSetup(taskContext)
    val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(taskAttemptContext)
    configuration.set("spark.sql.sources.output.path", outputPath)
    val writer = outputWriterFactory.newInstance(getWorkPath, dataSchema, taskAttemptContext)
    writer.initConverter(dataSchema)

    var writerClosed = false

    // If anything below fails, we should abort the task.
    try {
      while (iterator.hasNext) {
        val internalRow = iterator.next()
        writer.writeInternal(internalRow)
      }

      commitTask()
    } catch {
      case cause: Throwable =>
        logError("Aborting task.", cause)
        abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
    }

    def commitTask(): Unit = {
      try {
        assert(writer != null, "OutputWriter instance should have been initialized")
        if (!writerClosed) {
          writer.close()
          writerClosed = true
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
        if (!writerClosed) {
          writer.close()
          writerClosed = true
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
private[sql] class DynamicPartitionWriterContainer(
    @transient relation: HadoopFsRelation,
    @transient job: Job,
    partitionColumns: Seq[Attribute],
    dataColumns: Seq[Attribute],
    inputSchema: Seq[Attribute],
    defaultPartitionName: String,
    maxOpenFiles: Int,
    isAppend: Boolean)
  extends BaseWriterContainer(relation, job, isAppend) {

  def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit = {
    val outputWriters = new java.util.HashMap[InternalRow, OutputWriter]
    executorSideSetup(taskContext)

    var outputWritersCleared = false

    // Returns the partition key given an input row
    val getPartitionKey = UnsafeProjection.create(partitionColumns, inputSchema)
    // Returns the data columns to be written given an input row
    val getOutputRow = UnsafeProjection.create(dataColumns, inputSchema)

    // Expressions that given a partition key build a string like: col1=val/col2=val/...
    val partitionStringExpression = partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val escaped =
        ScalaUDF(
          PartitioningUtils.escapePathName _, StringType, Seq(Cast(c, StringType)), Seq(StringType))
      val str = If(IsNull(c), Literal(defaultPartitionName), escaped)
      val partitionName = Literal(c.name + "=") :: str :: Nil
      if (i == 0) partitionName else Literal(Path.SEPARATOR_CHAR.toString) :: partitionName
    }

    // Returns the partition path given a partition key.
    val getPartitionString =
      UnsafeProjection.create(Concat(partitionStringExpression) :: Nil, partitionColumns)

    // If anything below fails, we should abort the task.
    try {
      // This will be filled in if we have to fall back on sorting.
      var sorter: UnsafeKVExternalSorter = null
      while (iterator.hasNext && sorter == null) {
        val inputRow = iterator.next()
        val currentKey = getPartitionKey(inputRow)
        var currentWriter = outputWriters.get(currentKey)

        if (currentWriter == null) {
          if (outputWriters.size < maxOpenFiles) {
            currentWriter = newOutputWriter(currentKey)
            outputWriters.put(currentKey.copy(), currentWriter)
            currentWriter.writeInternal(getOutputRow(inputRow))
          } else {
            logInfo(s"Maximum partitions reached, falling back on sorting.")
            sorter = new UnsafeKVExternalSorter(
              StructType.fromAttributes(partitionColumns),
              StructType.fromAttributes(dataColumns),
              SparkEnv.get.blockManager,
              SparkEnv.get.shuffleMemoryManager,
              SparkEnv.get.shuffleMemoryManager.pageSizeBytes)
            sorter.insertKV(currentKey, getOutputRow(inputRow))
          }
        } else {
          currentWriter.writeInternal(getOutputRow(inputRow))
        }
      }

      // If the sorter is not null that means that we reached the maxFiles above and need to finish
      // using external sort.
      if (sorter != null) {
        while (iterator.hasNext) {
          val currentRow = iterator.next()
          sorter.insertKV(getPartitionKey(currentRow), getOutputRow(currentRow))
        }

        logInfo(s"Sorting complete. Writing out partition files one at a time.")

        val sortedIterator = sorter.sortedIterator()
        var currentKey: InternalRow = null
        var currentWriter: OutputWriter = null
        try {
          while (sortedIterator.next()) {
            if (currentKey != sortedIterator.getKey) {
              if (currentWriter != null) {
                currentWriter.close()
              }
              currentKey = sortedIterator.getKey.copy()
              logDebug(s"Writing partition: $currentKey")

              // Either use an existing file from before, or open a new one.
              currentWriter = outputWriters.remove(currentKey)
              if (currentWriter == null) {
                currentWriter = newOutputWriter(currentKey)
              }
            }

            currentWriter.writeInternal(sortedIterator.getValue)
          }
        } finally {
          if (currentWriter != null) { currentWriter.close() }
        }
      }

      commitTask()
    } catch {
      case cause: Throwable =>
        logError("Aborting task.", cause)
        abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
    }

    /** Open and returns a new OutputWriter given a partition key. */
    def newOutputWriter(key: InternalRow): OutputWriter = {
      val partitionPath = getPartitionString(key).getString(0)
      val path = new Path(getWorkPath, partitionPath)
      val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(taskAttemptContext)
      configuration.set(
        "spark.sql.sources.output.path", new Path(outputPath, partitionPath).toString)
      val newWriter = outputWriterFactory.newInstance(path.toString, dataSchema, taskAttemptContext)
      newWriter.initConverter(dataSchema)
      newWriter
    }

    def clearOutputWriters(): Unit = {
      if (!outputWritersCleared) {
        outputWriters.asScala.values.foreach(_.close())
        outputWriters.clear()
        outputWritersCleared = true
      }
    }

    def commitTask(): Unit = {
      try {
        clearOutputWriters()
        super.commitTask()
      } catch {
        case cause: Throwable =>
          throw new RuntimeException("Failed to commit task", cause)
      }
    }

    def abortTask(): Unit = {
      try {
        clearOutputWriters()
      } finally {
        super.abortTask()
      }
    }
  }
}
