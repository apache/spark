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

package org.apache.spark.sql.sources

import java.util.Date

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.util.Shell
import parquet.hadoop.util.ContextUtil

import org.apache.spark._
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateProjection
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

private[sql] case class InsertIntoDataSource(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    overwrite: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = logicalRelation.relation.asInstanceOf[InsertableRelation]
    val data = DataFrame(sqlContext, query)
    // Apply the schema of the existing table to the new data.
    val df = sqlContext.createDataFrame(
      data.queryExecution.toRdd, logicalRelation.schema, needsConversion = false)
    relation.insert(df, overwrite)

    // Invalidate the cache.
    sqlContext.cacheManager.invalidateCache(logicalRelation)

    Seq.empty[Row]
  }
}

private[sql] case class InsertIntoHadoopFsRelation(
    @transient relation: HadoopFsRelation,
    @transient query: LogicalPlan,
    partitionColumns: Array[String],
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

    val doInsertion = (mode, fs.exists(qualifiedOutputPath)) match {
      case (SaveMode.ErrorIfExists, true) =>
        sys.error(s"path $qualifiedOutputPath already exists.")
      case (SaveMode.Overwrite, true) =>
        fs.delete(qualifiedOutputPath, true)
        true
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        true
      case (SaveMode.Ignore, exists) =>
        !exists
    }

    if (doInsertion) {
      val job = new Job(hadoopConf)
      job.setOutputKeyClass(classOf[Void])
      job.setOutputValueClass(classOf[Row])
      FileOutputFormat.setOutputPath(job, qualifiedOutputPath)

      val df = sqlContext.createDataFrame(
        DataFrame(sqlContext, query).queryExecution.toRdd,
        relation.schema,
        needsConversion = false)

      if (partitionColumns.isEmpty) {
        insert(new DefaultWriterContainer(relation, job), df)
      } else {
        val writerContainer = new DynamicPartitionWriterContainer(
          relation, job, partitionColumns, PartitioningUtils.DEFAULT_PARTITION_NAME)
        insertWithDynamicPartitions(sqlContext, writerContainer, df, partitionColumns)
      }
    }

    Seq.empty[Row]
  }

  private def insert(writerContainer: BaseWriterContainer, df: DataFrame): Unit = {
    // Uses local vals for serialization
    val needsConversion = relation.needConversion
    val dataSchema = relation.dataSchema

    try {
      writerContainer.driverSideSetup()
      df.sqlContext.sparkContext.runJob(df.queryExecution.executedPlan.execute(), writeRows _)
      writerContainer.commitJob()
      relation.refresh()
    } catch { case cause: Throwable =>
      logError("Aborting job.", cause)
      writerContainer.abortJob()
      throw new SparkException("Job aborted.", cause)
    }

    def writeRows(taskContext: TaskContext, iterator: Iterator[Row]): Unit = {
      writerContainer.executorSideSetup(taskContext)

      try {
        if (needsConversion) {
          val converter = CatalystTypeConverters.createToScalaConverter(dataSchema)
          while (iterator.hasNext) {
            val row = converter(iterator.next()).asInstanceOf[Row]
            writerContainer.outputWriterForRow(row).write(row)
          }
        } else {
          while (iterator.hasNext) {
            val row = iterator.next()
            writerContainer.outputWriterForRow(row).write(row)
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

  private def insertWithDynamicPartitions(
      sqlContext: SQLContext,
      writerContainer: BaseWriterContainer,
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

    try {
      writerContainer.driverSideSetup()
      df.sqlContext.sparkContext.runJob(df.queryExecution.executedPlan.execute(), writeRows _)
      writerContainer.commitJob()
      relation.refresh()
    } catch { case cause: Throwable =>
      logError("Aborting job.", cause)
      writerContainer.abortJob()
      throw new SparkException("Job aborted.", cause)
    }

    def writeRows(taskContext: TaskContext, iterator: Iterator[Row]): Unit = {
      writerContainer.executorSideSetup(taskContext)

      val partitionProj = newProjection(codegenEnabled, partitionOutput, output)
      val dataProj = newProjection(codegenEnabled, dataOutput, output)

      if (needsConversion) {
        val converter = CatalystTypeConverters.createToScalaConverter(dataSchema)
        while (iterator.hasNext) {
          val row = iterator.next()
          val partitionPart = partitionProj(row)
          val dataPart = dataProj(row)
          val convertedDataPart = converter(dataPart).asInstanceOf[Row]
          writerContainer.outputWriterForRow(partitionPart).write(convertedDataPart)
        }
      } else {
        while (iterator.hasNext) {
          val row = iterator.next()
          val partitionPart = partitionProj(row)
          val dataPart = dataProj(row)
          writerContainer.outputWriterForRow(partitionPart).write(dataPart)
        }
      }

      writerContainer.commitTask()
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
      GenerateProjection.generate(expressions, inputSchema)
    } else {
      new InterpretedProjection(expressions, inputSchema)
    }
  }
}

private[sql] abstract class BaseWriterContainer(
    @transient val relation: HadoopFsRelation,
    @transient job: Job)
  extends SparkHadoopMapReduceUtil
  with Logging
  with Serializable {

  protected val serializableConf = new SerializableWritable(ContextUtil.getConfiguration(job))

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
    taskAttemptContext = newTaskAttemptContext(serializableConf.value, taskAttemptId)
    outputWriterFactory = relation.prepareJobForWrite(job)
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
      case f: FileOutputCommitter => f.getWorkPath.toString
      case _ => outputPath
    }
  }

  private def newOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    val committerClass = context.getConfiguration.getClass(
      "mapred.output.committer.class", null, classOf[OutputCommitter])

    Option(committerClass).map { clazz =>
      val ctor = clazz.getDeclaredConstructor(classOf[Path], classOf[TaskAttemptContext])
      ctor.newInstance(new Path(outputPath), context)
    }.getOrElse {
      outputFormatClass.newInstance().getOutputCommitter(context)
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
  def outputWriterForRow(row: Row): OutputWriter

  protected def initWriters(): Unit

  def commitTask(): Unit = {
    SparkHadoopMapRedUtil.commitTask(
      outputCommitter, taskAttemptContext, jobId.getId, taskId.getId, taskAttemptId.getId)
  }

  def abortTask(): Unit = {
    outputCommitter.abortTask(taskAttemptContext)
    logError(s"Task attempt $taskAttemptId aborted.")
  }

  def commitJob(): Unit = {
    outputCommitter.commitJob(jobContext)
    logInfo(s"Job $jobId committed.")
  }

  def abortJob(): Unit = {
    outputCommitter.abortJob(jobContext, JobStatus.State.FAILED)
    logError(s"Job $jobId aborted.")
  }
}

private[sql] class DefaultWriterContainer(
    @transient relation: HadoopFsRelation,
    @transient job: Job)
  extends BaseWriterContainer(relation, job) {

  @transient private var writer: OutputWriter = _

  override protected def initWriters(): Unit = {
    taskAttemptContext.getConfiguration.set("spark.sql.sources.output.path", outputPath)
    writer = outputWriterFactory.newInstance(getWorkPath, dataSchema, taskAttemptContext)
  }

  override def outputWriterForRow(row: Row): OutputWriter = writer

  override def commitTask(): Unit = {
    writer.close()
    super.commitTask()
  }

  override def abortTask(): Unit = {
    writer.close()
    super.abortTask()
  }
}

private[sql] class DynamicPartitionWriterContainer(
    @transient relation: HadoopFsRelation,
    @transient job: Job,
    partitionColumns: Array[String],
    defaultPartitionName: String)
  extends BaseWriterContainer(relation, job) {

  // All output writers are created on executor side.
  @transient protected var outputWriters: mutable.Map[String, OutputWriter] = _

  override protected def initWriters(): Unit = {
    outputWriters = mutable.Map.empty[String, OutputWriter]
  }

  override def outputWriterForRow(row: Row): OutputWriter = {
    val partitionPath = partitionColumns.zip(row.toSeq).map { case (col, rawValue) =>
      val string = if (rawValue == null) null else String.valueOf(rawValue)
      val valueString = if (string == null || string.isEmpty) {
        defaultPartitionName
      } else {
        DynamicPartitionWriterContainer.escapePathName(string)
      }
      s"/$col=$valueString"
    }.mkString.stripPrefix(Path.SEPARATOR)

    outputWriters.getOrElseUpdate(partitionPath, {
      val path = new Path(getWorkPath, partitionPath)
      taskAttemptContext.getConfiguration.set(
        "spark.sql.sources.output.path",
        new Path(outputPath, partitionPath).toString)
      outputWriterFactory.newInstance(path.toString, dataSchema, taskAttemptContext)
    })
  }

  override def commitTask(): Unit = {
    outputWriters.values.foreach(_.close())
    super.commitTask()
  }

  override def abortTask(): Unit = {
    outputWriters.values.foreach(_.close())
    super.abortTask()
  }
}

private[sql] object DynamicPartitionWriterContainer {
  //////////////////////////////////////////////////////////////////////////////////////////////////
  // The following string escaping code is mainly copied from Hive (o.a.h.h.common.FileUtils).
  //////////////////////////////////////////////////////////////////////////////////////////////////

  val charToEscape = {
    val bitSet = new java.util.BitSet(128)

    /**
     * ASCII 01-1F are HTTP control characters that need to be escaped.
     * \u000A and \u000D are \n and \r, respectively.
     */
    val clist = Array(
      '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\u0008', '\u0009',
      '\n', '\u000B', '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', '\u0013',
      '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', '\u001B', '\u001C',
      '\u001D', '\u001E', '\u001F', '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F',
      '{', '[', ']', '^')

    clist.foreach(bitSet.set(_))

    if (Shell.WINDOWS) {
      Array(' ', '<', '>', '|').foreach(bitSet.set(_))
    }

    bitSet
  }

  def needsEscaping(c: Char): Boolean = {
    c >= 0 && c < charToEscape.size() && charToEscape.get(c)
  }

  def escapePathName(path: String): String = {
    val builder = new StringBuilder()
    path.foreach { c =>
      if (DynamicPartitionWriterContainer.needsEscaping(c)) {
        builder.append('%')
        builder.append(f"${c.asInstanceOf[Int]}%02x")
      } else {
        builder.append(c)
      }
    }

    builder.toString()
  }
}
