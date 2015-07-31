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

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.mutable.HashSet

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, FileOutputCommitter => MapReduceFileOutputCommitter}
import org.apache.spark._
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateProjection
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources._
import org.apache.spark.util.SerializableConfiguration


private[sql] case class InsertIntoDataSource(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    overwrite: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = logicalRelation.relation.asInstanceOf[InsertableRelation]
    val data = DataFrame(sqlContext, query)
    // Apply the schema of the existing table to the new data.
    val df = sqlContext.internalCreateDataFrame(data.queryExecution.toRdd, logicalRelation.schema)
    relation.insert(df, overwrite)

    // Invalidate the cache.
    sqlContext.cacheManager.invalidateCache(logicalRelation)

    Seq.empty[Row]
  }
}

/**
 * A command for writing data to a [[HadoopFsRelation]].  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.  Each [[InsertIntoHadoopFsRelation]] issues a
 * single write job, and owns a UUID that identifies this job.  Each concrete implementation of
 * [[HadoopFsRelation]] should use this UUID together with task id to generate unique file path for
 * each task output file.  This UUID is passed to executor side via a property named
 * `spark.sql.sources.writeJobUUID`.
 *
 * Different writer containers, [[DefaultWriterContainer]] and [[DynamicPartitionWriterContainer]]
 * are used to write to normal tables and tables with dynamic partitions.
 *
 * Basic work flow of this command is:
 *
 *   1. Driver side setup, including output committer initialization and data source specific
 *      preparation work for the write job to be issued.
 *   2. Issues a write job consists of one or more executor side tasks, each of which writes all
 *      rows within an RDD partition.
 *   3. If no exception is thrown in a task, commits that task, otherwise aborts that task;  If any
 *      exception is thrown during task commitment, also aborts that task.
 *   4. If all tasks are committed, commit the job, otherwise aborts the job;  If any exception is
 *      thrown during job commitment, also aborts the job.
 */
private[sql] case class InsertIntoHadoopFsRelation(
    @transient relation: HadoopFsRelation,
    @transient query: LogicalPlan,
    mode: SaveMode)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    require(
      relation.paths.length == 1,
      s"Cannot write to multiple destinations: ${relation.paths.mkString(",")}")

    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val outputPath = new Path(relation.paths.head)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val pathExists = fs.exists(qualifiedOutputPath)
    val doInsertion = (mode, pathExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        throw new AnalysisException(s"path $qualifiedOutputPath already exists.")
      case (SaveMode.Overwrite, true) =>
        fs.delete(qualifiedOutputPath, true)
        true
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        true
      case (SaveMode.Ignore, exists) =>
        !exists
      case (s, exists) =>
        throw new IllegalStateException(s"unsupported save mode $s ($exists)")
    }
    // If we are appending data to an existing dir.
    val isAppend = pathExists && (mode == SaveMode.Append)

    if (doInsertion) {
      val job = new Job(hadoopConf)
      job.setOutputKeyClass(classOf[Void])
      job.setOutputValueClass(classOf[InternalRow])
      FileOutputFormat.setOutputPath(job, qualifiedOutputPath)

      // We create a DataFrame by applying the schema of relation to the data to make sure.
      // We are writing data based on the expected schema,
      val df = {
        // For partitioned relation r, r.schema's column ordering can be different from the column
        // ordering of data.logicalPlan (partition columns are all moved after data column). We
        // need a Project to adjust the ordering, so that inside InsertIntoHadoopFsRelation, we can
        // safely apply the schema of r.schema to the data.
        val project = Project(
          relation.schema.map(field => new UnresolvedAttribute(Seq(field.name))), query)

        sqlContext.internalCreateDataFrame(
          DataFrame(sqlContext, project).queryExecution.toRdd, relation.schema)
      }

      val partitionColumns = relation.partitionColumns.fieldNames
      if (partitionColumns.isEmpty) {
        insert(new DefaultWriterContainer(relation, job, isAppend), df)
      } else {
        val writerContainer = new DynamicPartitionWriterContainer(
          relation, job, partitionColumns, PartitioningUtils.DEFAULT_PARTITION_NAME, isAppend)
        insertWithDynamicPartitions(sqlContext, writerContainer, df, partitionColumns)
      }
    }

    Seq.empty[Row]
  }

  /**
   * Inserts the content of the [[DataFrame]] into a table without any partitioning columns.
   */
  private def insert(writerContainer: BaseWriterContainer, df: DataFrame): Unit = {
    // Uses local vals for serialization
    val needsConversion = relation.needConversion
    val dataSchema = relation.dataSchema

    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    writerContainer.driverSideSetup()

    try {
      df.sqlContext.sparkContext.runJob(df.queryExecution.toRdd, writeRows _)
      writerContainer.commitJob()
      relation.refresh()
    } catch { case cause: Throwable =>
      logError("Aborting job.", cause)
      writerContainer.abortJob()
      throw new SparkException("Job aborted.", cause)
    }

    def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit = {
      // If anything below fails, we should abort the task.
      try {
        writerContainer.executorSideSetup(taskContext)

        if (needsConversion) {
          val converter = CatalystTypeConverters.createToScalaConverter(dataSchema)
            .asInstanceOf[InternalRow => Row]
          while (iterator.hasNext) {
            val internalRow = iterator.next()
            writerContainer.outputWriterForRow(internalRow).write(converter(internalRow))
          }
        } else {
          while (iterator.hasNext) {
            val internalRow = iterator.next()
            writerContainer.outputWriterForRow(internalRow)
              .asInstanceOf[OutputWriterInternal].writeInternal(internalRow)
          }
        }

        writerContainer.commitTask()
      } catch { case cause: Throwable =>
        logError("Aborting task.", cause)
        writerContainer.abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
      }
    }
  }

  /**
   * Inserts the content of the [[DataFrame]] into a table with partitioning columns.
   */
  private def insertWithDynamicPartitions(
      sqlContext: SQLContext,
      writerContainer: DynamicPartitionWriterContainer,
      df: DataFrame,
      partitionColumns: Array[String]): Unit = {
    // Uses a local val for serialization
    val needsConversion = relation.needConversion
    val dataSchema = relation.dataSchema

    require(
      df.schema == relation.schema,
      s"""DataFrame must have the same schema as the relation to which is inserted.
         |DataFrame schema: ${df.schema}
         |Relation schema: ${relation.schema}
       """.stripMargin)

    val partitionColumnsInSpec = relation.partitionColumns.fieldNames
    require(
      partitionColumnsInSpec.sameElements(partitionColumns),
      s"""Partition columns mismatch.
         |Expected: ${partitionColumnsInSpec.mkString(", ")}
         |Actual: ${partitionColumns.mkString(", ")}
       """.stripMargin)

    val output = df.queryExecution.executedPlan.output
    val (partitionOutput, dataOutput) = output.partition(a => partitionColumns.contains(a.name))
    val codegenEnabled = df.sqlContext.conf.codegenEnabled

    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    writerContainer.driverSideSetup()

    try {
      df.sqlContext.sparkContext.runJob(df.queryExecution.toRdd, writeRows _)
      writerContainer.commitJob()
      relation.refresh()
    } catch { case cause: Throwable =>
      logError("Aborting job.", cause)
      writerContainer.abortJob()
      throw new SparkException("Job aborted.", cause)
    }

    def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit = {
      // Track which rows have been output to disk so that if a data sort is necessary mid-write,
      // we don't end up outputting the same data twice
      val writtenRows: HashSet[InternalRow] = new HashSet[InternalRow]

      // Flag to track whether data has been sorted in which case it's safe to close previously
      // used outputWriters
      var sorted: Boolean = false

      // If anything below fails, we should abort the task.
      try {
        writerContainer.executorSideSetup(taskContext)

        // Sort the data by partition so that it's possible to use a single outputWriter at a
        // time to process the incoming data
        def sortRows(iterator: Iterator[InternalRow]): Iterator[InternalRow] = {
          // Sort by the same key used to look up the outputWriter to allow us to recyle the writer
          iterator.toArray.sortBy(writerContainer.computePartitionPath).toIterator
        }

        // When outputting rows, we may need to interrupt the file write to sort the underlying data
        // (SPARK-8890) to avoid running out of memory due to creating too many outputWriters. Thus,
        // we extract this functionality into its own function that can be called with updated
        // underlying data.
        def writeRowsSafe(iterator: Iterator[InternalRow]): Unit = {
          var converter: Option[InternalRow => Row] = None
          if (needsConversion) {
            converter = Some(CatalystTypeConverters.createToScalaConverter(dataSchema)
              .asInstanceOf[InternalRow => Row])
          }

          while (iterator.hasNext) {
            val internalRow = iterator.next()

            // Only output rows that we haven't already output, this code can be called after a sort
            // mid-traversal.
            if (!writtenRows.contains(internalRow) &&
              writerContainer.canGetOutputWriter(internalRow)) {

              converter match {
                case Some(converter) =>
                  writerContainer.outputWriterForRow(internalRow).write(converter(internalRow))
                case None =>
                  writerContainer.outputWriterForRow(internalRow)
                    .asInstanceOf[OutputWriterInternal].writeInternal(internalRow)
              }

              writtenRows += internalRow
            } else if (!writtenRows.contains(internalRow)) {
              // If there are no more available output writers, sort the data, and set the sorted
              // flag to true. This will then cause subsequent output writers to be cleared after
              // use, minimizing the memory footprint.
              val sortedRows: Iterator[InternalRow] = sortRows(iterator)
              sorted = true
              writeRowsSafe(sortedRows)
            }
          }
        }

        writeRowsSafe(iterator)
        writerContainer.commitTask()
      } catch { case cause: Throwable =>
        logError("Aborting task.", cause)
        writerContainer.abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
      }
    }
  }

  // This is copied from SparkPlan, probably should move this to a more general place.
  private def newProjection(
      codegenEnabled: Boolean,
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute]): Projection = {
    log.debug(
      s"Creating Projection: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if (codegenEnabled) {

      try {
        GenerateProjection.generate(expressions, inputSchema)
      } catch {
        case e: Exception =>
          if (sys.props.contains("spark.testing")) {
            throw e
          } else {
            log.error("failed to generate projection, fallback to interpreted", e)
            new InterpretedProjection(expressions, inputSchema)
          }
      }
    } else {
      new InterpretedProjection(expressions, inputSchema)
    }
  }
}

private[sql] abstract class BaseWriterContainer(
    @transient val relation: HadoopFsRelation,
    @transient job: Job,
    isAppend: Boolean)
  extends SparkHadoopMapReduceUtil
  with Logging
  with Serializable {

  protected val serializableConf = new SerializableConfiguration(job.getConfiguration)

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

  protected val outputPath: String = {
    assert(
      relation.paths.length == 1,
      s"Cannot write to multiple destinations: ${relation.paths.mkString(",")}")
    relation.paths.head
  }

  protected val dataSchema = relation.dataSchema

  protected var outputWriterFactory: OutputWriterFactory = _

  private var outputFormatClass: Class[_ <: OutputFormat[_, _]] = _

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
    initWriters()
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
      logInfo(
        s"Using output committer class ${defaultOutputCommitter.getClass.getCanonicalName} " +
        "for appending.")
      defaultOutputCommitter
    } else {
      val committerClass = context.getConfiguration.getClass(
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

  // Called on executor side when writing rows
  def outputWriterForRow(row: InternalRow): OutputWriter

  protected def initWriters(): Unit

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

private[sql] class DefaultWriterContainer(
    @transient relation: HadoopFsRelation,
    @transient job: Job,
    isAppend: Boolean)
  extends BaseWriterContainer(relation, job, isAppend) {

  @transient private var writer: OutputWriter = _

  override protected def initWriters(): Unit = {
    taskAttemptContext.getConfiguration.set("spark.sql.sources.output.path", outputPath)
    writer = outputWriterFactory.newInstance(getWorkPath, dataSchema, taskAttemptContext)
  }

  override def outputWriterForRow(row: InternalRow): OutputWriter = writer

  override def commitTask(): Unit = {
    try {
      assert(writer != null, "OutputWriter instance should have been initialized")
      writer.close()
      super.commitTask()
    } catch { case cause: Throwable =>
      // This exception will be handled in `InsertIntoHadoopFsRelation.insert$writeRows`, and will
      // cause `abortTask()` to be invoked.
      throw new RuntimeException("Failed to commit task", cause)
    }
  }

  override def abortTask(): Unit = {
    try {
      // It's possible that the task fails before `writer` gets initialized
      if (writer != null) {
        writer.close()
      }
    } finally {
      super.abortTask()
    }
  }
}

private[sql] class DynamicPartitionWriterContainer(
    @transient relation: HadoopFsRelation,
    @transient job: Job,
    partitionColumns: Array[String],
    defaultPartitionName: String,
    isAppend: Boolean)
  extends BaseWriterContainer(relation, job, isAppend) {

  // All output writers are created on executor side.
  @transient protected var outputWriters: java.util.HashMap[String, OutputWriter] = _

  protected var maxOutputWriters = 50

  override protected def initWriters(): Unit = {
    outputWriters = new java.util.HashMap[String, OutputWriter]
  }

  /**
   * Extract the functionality to create a partitionPath, a grouping of columns in a row, which
   * serves as a key variable when allocating new outputWriters.
   */
  def computePartitionPath(row: InternalRow): String = {
    val partitionPath = {
      val partitionPathBuilder = new StringBuilder
      var i = 0

      while (i < partitionColumns.length) {
        val col = partitionColumns(i)
        val partitionValueString = {
          val string = row.getUTF8String(i)
          if (string.eq(null)) {
            defaultPartitionName
          } else {
            PartitioningUtils.escapePathName(string.toString)
          }
        }

        if (i > 0) {
          partitionPathBuilder.append(Path.SEPARATOR_CHAR)
        }

        partitionPathBuilder.append(s"$col=$partitionValueString")
        i += 1
      }

      partitionPathBuilder.toString()
    }
    partitionPath
  }

  /**
   * Returns true if it's possible to create a new outputWriter for a given row or to use an
   * existing writer without triggering a sort operation on the incoming data to avoid memory
   * problems.
   *
   * During {{ InsertIntoHadoopFsRelation }} new outputWriters are created for every partition.
   * Creating too many outputWriters can cause us to run out of memory (SPARK-8890). Therefore, only
   * create up to a certain number of outputWriters. If the number of allowed writers is exceeded,
   * the existing outputWriters will be closed and a sort operation will be triggered on the
   * incoming data, ensuring that it's sorted by key such that a single outputWriter may be used
   * at a time. E.g. process all key1, close the writer, process key2, etc.
   */
  def canGetOutputWriter(row: InternalRow): Boolean = {
    (outputWriters.size() < (maxOutputWriters - 1)) || {
      // Only compute this when we're near the max allowed number of outputWriters
      val partitionPath = computePartitionPath(row)
      outputWriters.containsKey(partitionPath)
    }
  }

  /**
   * Create the outputWriter to output a given row to disk.
   *
   * @param row The `row` argument is supposed to only contain partition column values
   *            which have been casted to strings.
   */
  override def outputWriterForRow(row: InternalRow): OutputWriter = {
    val partitionPath: String = computePartitionPath(row)

    val writer = outputWriters.get(partitionPath)
    if (writer.eq(null)) {
      val path = new Path(getWorkPath, partitionPath)
      taskAttemptContext.getConfiguration.set(
        "spark.sql.sources.output.path", new Path(outputPath, partitionPath).toString)
      val newWriter = outputWriterFactory.newInstance(path.toString, dataSchema, taskAttemptContext)
      outputWriters.put(partitionPath, newWriter)
      newWriter
    } else {
      writer
    }
  }

  /**
   * Create the outputWriter to output a given row to disk. If dealing with sorted data, we
   * can close previously used writers since they will no longer be necessary.
   *
   * @param row The `row` argument is supposed to only contain partition column values
   *            which have been casted to strings.
   * @param shouldCloseWriters If true, close all existing writers before creating new writers
   */
  def outputWriterForRow(row: InternalRow, shouldCloseWriters: Boolean): OutputWriter = {
    if (shouldCloseWriters) {
      clearOutputWriters()
    }

    outputWriterForRow(row)
  }

  private def clearOutputWriters(): Unit = {
    if (!outputWriters.isEmpty) {
      asScalaIterator(outputWriters.values().iterator()).foreach(_.close())
      outputWriters.clear()
    }
  }

  override def commitTask(): Unit = {
    try {
      clearOutputWriters()
      super.commitTask()
    } catch { case cause: Throwable =>
      throw new RuntimeException("Failed to commit task", cause)
    }
  }

  override def abortTask(): Unit = {
    try {
      clearOutputWriters()
    } finally {
      super.abortTask()
    }
  }
}
