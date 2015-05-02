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

import java.util
import java.util.Date

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.Shell

import org.apache.spark._
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}

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

private[sql] case class InsertIntoFSBasedRelation(
    @transient relation: FSBasedRelation,
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
    val doInsertion = (mode, fs.exists(outputPath)) match {
      case (SaveMode.ErrorIfExists, true) =>
        sys.error(s"path $outputPath already exists.")
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        true
      case (SaveMode.Ignore, exists) =>
        !exists
    }

    if (doInsertion) {
      val jobConf = new JobConf(hadoopConf)
      jobConf.setOutputKeyClass(classOf[Void])
      jobConf.setOutputValueClass(classOf[Row])
      FileOutputFormat.setOutputPath(jobConf, outputPath)

      val df = sqlContext.createDataFrame(
        DataFrame(sqlContext, query).queryExecution.toRdd,
        relation.schema,
        needsConversion = false)

      if (partitionColumns.isEmpty) {
        insert(new DefaultWriterContainer(relation, jobConf), df)
      } else {
        val writerContainer = new DynamicPartitionWriterContainer(
          relation, jobConf, partitionColumns, "__HIVE_DEFAULT_PARTITION__")
        insertWithDynamicPartitions(writerContainer, df, partitionColumns)
      }
    }

    Seq.empty[Row]
  }

  private def insert(writerContainer: BaseWriterContainer, df: DataFrame): Unit = {
    try {
      writerContainer.driverSideSetup()
      df.sqlContext.sparkContext.runJob(df.rdd, writeRows _)
      writerContainer.commitJob()
    } catch { case cause: Throwable =>
      writerContainer.abortJob()
      throw new SparkException("Job aborted.", cause)
    }

    def writeRows(taskContext: TaskContext, iterator: Iterator[Row]): Unit = {
      writerContainer.executorSideSetup(taskContext)

      try {
        while (iterator.hasNext) {
          val row = iterator.next()
          writerContainer.outputWriterForRow(row).write(row)
        }
        writerContainer.commitTask()
      } catch { case cause: Throwable =>
        writerContainer.abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
      }
    }
  }

  private def insertWithDynamicPartitions(
      writerContainer: BaseWriterContainer,
      df: DataFrame,
      partitionColumns: Array[String]): Unit = {

    require(
      df.schema == relation.schema,
      s"""DataFrame must have the same schema as the relation to which is inserted.
         |DataFrame schema: ${df.schema}
         |Relation schema: ${relation.schema}
       """.stripMargin)

    val sqlContext = df.sqlContext

    val (partitionRDD, dataRDD) = {
      val fieldNames = relation.schema.fieldNames
      val dataCols = fieldNames.filterNot(partitionColumns.contains)
      val df = sqlContext.createDataFrame(
        DataFrame(sqlContext, query).queryExecution.toRdd,
        relation.schema,
        needsConversion = false)

      val partitionColumnsInSpec = relation.partitionSpec.partitionColumns.map(_.name)
      require(
        partitionColumnsInSpec.sameElements(partitionColumns),
        s"""Partition columns mismatch.
           |Expected: ${partitionColumnsInSpec.mkString(", ")}
           |Actual: ${partitionColumns.mkString(", ")}
         """.stripMargin)

      val partitionDF = df.select(partitionColumns.head, partitionColumns.tail: _*)
      val dataDF = df.select(dataCols.head, dataCols.tail: _*)

      (partitionDF.rdd, dataDF.rdd)
    }

    try {
      writerContainer.driverSideSetup()
      sqlContext.sparkContext.runJob(partitionRDD.zip(dataRDD), writeRows _)
      writerContainer.commitJob()
      relation.refreshPartitions()
    } catch { case cause: Throwable =>
      writerContainer.abortJob()
      throw new SparkException("Job aborted.", cause)
    }

    def writeRows(taskContext: TaskContext, iterator: Iterator[(Row, Row)]): Unit = {
      writerContainer.executorSideSetup(taskContext)

      try {
        while (iterator.hasNext) {
          val (partitionValues, data) = iterator.next()
          writerContainer.outputWriterForRow(partitionValues).write(data)
        }

        writerContainer.commitTask()
      } catch { case cause: Throwable =>
        writerContainer.abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
      }
    }
  }
}

private[sql] abstract class BaseWriterContainer(
    @transient val relation: FSBasedRelation,
    @transient jobConf: JobConf)
  extends SparkHadoopMapRedUtil
  with Logging
  with Serializable {

  protected val serializableJobConf = new SerializableWritable(jobConf)

  // This is only used on driver side.
  @transient private var jobContext: JobContext = _

  // This is only used on executor side.
  @transient private var taskAttemptContext: TaskAttemptContext = _

  // The following fields are initialized and used on both driver and executor side.
  @transient private var outputCommitter: OutputCommitter = _
  @transient private var jobId: JobID = _
  @transient private var taskId: TaskID = _
  @transient private var taskAttemptId: TaskAttemptID = _

  protected val outputPath = {
    assert(
      relation.paths.length == 1,
      s"Cannot write to multiple destinations: ${relation.paths.mkString(",")}")
    relation.paths.head
  }

  protected val dataSchema = relation.dataSchema

  protected val outputWriterClass: Class[_ <: OutputWriter] = relation.outputWriterClass

  def driverSideSetup(): Unit = {
    setupIDs(0, 0, 0)
    relation.prepareForWrite(serializableJobConf.value)
    setupJobConf()
    jobContext = newJobContext(jobConf, jobId)
    outputCommitter = jobConf.getOutputCommitter
    outputCommitter.setupJob(jobContext)
  }

  def executorSideSetup(taskContext: TaskContext): Unit = {
    setupIDs(taskContext.stageId(), taskContext.partitionId(), taskContext.attemptNumber())
    setupJobConf()
    taskAttemptContext = newTaskAttemptContext(serializableJobConf.value, taskAttemptId)
    outputCommitter = serializableJobConf.value.getOutputCommitter
    outputCommitter.setupTask(taskAttemptContext)
    initWriters()
  }

  private def setupIDs(jobId: Int, splitId: Int, attemptId: Int): Unit = {
    this.jobId = SparkHadoopWriter.createJobID(new Date, jobId)
    this.taskId = new TaskID(this.jobId, true, splitId)
    this.taskAttemptId = new TaskAttemptID(taskId, attemptId)
  }

  private def setupJobConf(): Unit = {
    serializableJobConf.value.set("mapred.job.id", jobId.toString)
    serializableJobConf.value.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
    serializableJobConf.value.set("mapred.task.id", taskAttemptId.toString)
    serializableJobConf.value.setBoolean("mapred.task.is.map", true)
    serializableJobConf.value.setInt("mapred.task.partition", 0)
  }

  // Called on executor side when writing rows
  def outputWriterForRow(row: Row): OutputWriter

  protected def initWriters(): Unit = {
    val writer = outputWriterClass.newInstance()
    writer.init(outputPath, dataSchema, serializableJobConf.value)
    mutable.Map(outputPath -> writer)
  }

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
    outputCommitter.abortJob(jobContext, JobStatus.FAILED)
    logError(s"Job $jobId aborted.")
  }
}

private[sql] class DefaultWriterContainer(
    @transient relation: FSBasedRelation,
    @transient conf: JobConf)
  extends BaseWriterContainer(relation, conf) {

  @transient private var writer: OutputWriter = _

  override protected def initWriters(): Unit = {
    writer = relation.outputWriterClass.newInstance()
    writer.init(outputPath, dataSchema, serializableJobConf.value)
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
    @transient relation: FSBasedRelation,
    @transient conf: JobConf,
    partitionColumns: Array[String],
    defaultPartitionName: String)
  extends BaseWriterContainer(relation, conf) {

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
    }.mkString

    outputWriters.getOrElseUpdate(partitionPath, {
      val path = new Path(outputPath, partitionPath.stripPrefix(Path.SEPARATOR))
      val writer = outputWriterClass.newInstance()
      writer.init(path.toString, dataSchema, serializableJobConf.value)
      writer
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
  val charToEscape = {
    val bitSet = new util.BitSet(128)

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
    c >= 0 && c < charToEscape.size() && charToEscape.get(c);
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
