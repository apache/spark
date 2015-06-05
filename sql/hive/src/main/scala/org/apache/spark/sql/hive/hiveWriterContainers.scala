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

package org.apache.spark.sql.hive

import java.text.NumberFormat
import java.util.Date

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator, Utilities}
import org.apache.hadoop.hive.ql.io.{HiveFileFormatUtils, HiveOutputFormat}
import org.apache.hadoop.hive.ql.plan.{PlanUtils, TableDesc}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred._
import org.apache.hadoop.hive.common.FileUtils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.sql.Row
import org.apache.spark.{SerializableWritable, Logging, SparkHadoopWriter}
import org.apache.spark.sql.catalyst.util.DateUtils
import org.apache.spark.sql.hive.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.sql.types._

/**
 * Internal helper class that saves an RDD using a Hive OutputFormat.
 * It is based on [[SparkHadoopWriter]].
 */
private[hive] class SparkHiveWriterContainer(
    jobConf: Broadcast[SerializableWritable[JobConf]],
    fileSinkConf: FileSinkDesc)
  extends Logging
  with SparkHadoopMapRedUtil
  with Serializable {

  private val now = new Date()
  private val tableDesc: TableDesc = fileSinkConf.getTableInfo
  // Add table properties from storage handler to jobConf, so any custom storage
  // handler settings can be set to jobConf
  if (tableDesc != null) {
    PlanUtils.configureOutputJobPropertiesForStorageHandler(tableDesc)
    Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf.value.value)
  }
  @transient var conf: JobConf = jobConf.value.value

  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0

  @transient private var jID: JobID = null
  @transient private var taID: TaskAttemptID = null
  private var jIDString: String = null
  private var taskIDString: String = null
  private var taskAttemptIDString: String = null

  @transient private var writer: FileSinkOperator.RecordWriter = null
  @transient protected lazy val committer = conf.getOutputCommitter
  /** Only used on driver side **/
  @transient protected lazy val jobContext = newJobContext(conf, jID)
  /** Only used on executor side */
  @transient private lazy val taskContext = newTaskAttemptContext(conf, taID)
  @transient private lazy val outputFormat =
    conf.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef, Writable]]

  def driverSideSetup() {
    setIDs(0, 0, 0)
    setConfParams()
    committer.setupJob(jobContext)
  }

  def executorSideSetup(jobId: Int, splitId: Int, attemptId: Int) {
    conf = new JobConf(jobConf.value.value)
    setIDs(jobId, splitId, attemptId)
    setConfParams()
    committer.setupTask(taskContext)
    initWriters()
  }

  protected def getOutputName: String = {
    val numberFormat = NumberFormat.getInstance()
    numberFormat.setMinimumIntegerDigits(5)
    numberFormat.setGroupingUsed(false)
    val extension = Utilities.getFileExtension(conf, fileSinkConf.getCompressed, outputFormat)
    "part-" + numberFormat.format(splitID) + extension
  }

  def getLocalFileWriter(row: Row, schema: StructType): FileSinkOperator.RecordWriter = writer

  def close() {
    // Seems the boolean value passed into close does not matter.
    writer.close(false)
    commit()
  }

  def commitJob() {
    committer.commitJob(jobContext)
  }

  protected def initWriters() {
    // NOTE this method is executed at the executor side.
    // For Hive tables without partitions or with only static partitions, only 1 writer is needed.
    writer = HiveFileFormatUtils.getHiveRecordWriter(
      conf,
      fileSinkConf.getTableInfo,
      conf.getOutputValueClass.asInstanceOf[Class[Writable]],
      fileSinkConf,
      FileOutputFormat.getTaskOutputPath(conf, getOutputName),
      Reporter.NULL)
  }

  protected def commit() {
    SparkHadoopMapRedUtil.commitTask(committer, taskContext, jobID, splitID, attemptID)
  }

  private def setIDs(jobId: Int, splitId: Int, attemptId: Int) {
    jobID = jobId
    splitID = splitId
    attemptID = attemptId

    // note: sparkHadoopwriter.createjobid may be locale-dependent because it doesn't pass a locale
    // to date format; we should fix this so that its results is location-independent in case
    // different cluster nodes have different locales (e.g. driver and executor may be different
    // types of machines with different configurations).
    jID = SparkHadoopWriter.createJobID(now, jobId)
    taID  = new TaskAttemptID(new TaskID(jID, true, splitID), attemptID)
    jIDString = jID.toString
    taskAttemptIDString = taID.toString
    taskIDString = taID.getTaskID.toString
  }

  private def setConfParams() {
    conf.set("mapred.job.id", jIDString)
    conf.set("mapred.tip.id", taskIDString)
    conf.set("mapred.task.id", taskAttemptIDString)
    conf.setBoolean("mapred.task.is.map", true)
    conf.setInt("mapred.task.partition", splitID)
  }
}

private[hive] object SparkHiveWriterContainer {
  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (outputPath == null || fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }
}

private[spark] object SparkHiveDynamicPartitionWriterContainer {
  val SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = "mapreduce.fileoutputcommitter.marksuccessfuljobs"
}

private[spark] class SparkHiveDynamicPartitionWriterContainer(
  jobConf: Broadcast[SerializableWritable[JobConf]],
    fileSinkConf: FileSinkDesc,
    dynamicPartColNames: Array[String])
  extends SparkHiveWriterContainer(jobConf, fileSinkConf) {

  import SparkHiveDynamicPartitionWriterContainer._

  private val defaultPartName = jobConf.value.value.get(
    ConfVars.DEFAULTPARTITIONNAME.varname, ConfVars.DEFAULTPARTITIONNAME.defaultVal)

  @transient private var writers: mutable.HashMap[String, FileSinkOperator.RecordWriter] = _

  override protected def initWriters(): Unit = {
    // NOTE: This method is executed at the executor side.
    // Actual writers are created for each dynamic partition on the fly.
    writers = mutable.HashMap.empty[String, FileSinkOperator.RecordWriter]
  }

  override def close(): Unit = {
    writers.values.foreach(_.close(false))
    commit()
  }

  override def commitJob(): Unit = {
    // This is a hack to avoid writing _SUCCESS mark file. In lower versions of Hadoop (e.g. 1.0.4),
    // semantics of FileSystem.globStatus() is different from higher versions (e.g. 2.4.1) and will
    // include _SUCCESS file when glob'ing for dynamic partition data files.
    //
    // Better solution is to add a step similar to what Hive FileSinkOperator.jobCloseOp does:
    // calling something like Utilities.mvFileToFinalPath to cleanup the output directory and then
    // load it with loadDynamicPartitions/loadPartition/loadTable.
    val oldMarker = jobConf.value.value.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)
    jobConf.value.value.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false)
    super.commitJob()
    jobConf.value.value.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, oldMarker)
  }

  override def getLocalFileWriter(row: Row, schema: StructType): FileSinkOperator.RecordWriter = {
    def convertToHiveRawString(col: String, value: Any): String = {
      val raw = String.valueOf(value)
      schema(col).dataType match {
        case DateType => DateUtils.toString(raw.toInt)
        case _: DecimalType => BigDecimal(raw).toString()
        case _ => raw
      }
    }

    val dynamicPartPath = dynamicPartColNames
      .zip(row.toSeq.takeRight(dynamicPartColNames.length))
      .map { case (col, rawVal) =>
        val string = if (rawVal == null) null else convertToHiveRawString(col, rawVal)
        val colString =
          if (string == null || string.isEmpty) {
            defaultPartName
          } else {
            FileUtils.escapePathName(string, defaultPartName)
          }
        s"/$col=$colString"
      }.mkString

    def newWriter(): FileSinkOperator.RecordWriter = {
      val newFileSinkDesc = new FileSinkDesc(
        fileSinkConf.getDirName + dynamicPartPath,
        fileSinkConf.getTableInfo,
        fileSinkConf.getCompressed)
      newFileSinkDesc.setCompressCodec(fileSinkConf.getCompressCodec)
      newFileSinkDesc.setCompressType(fileSinkConf.getCompressType)

      val path = {
        val outputPath = FileOutputFormat.getOutputPath(conf)
        assert(outputPath != null, "Undefined job output-path")
        val workPath = new Path(outputPath, dynamicPartPath.stripPrefix("/"))
        new Path(workPath, getOutputName)
      }

      HiveFileFormatUtils.getHiveRecordWriter(
        conf,
        fileSinkConf.getTableInfo,
        conf.getOutputValueClass.asInstanceOf[Class[Writable]],
        newFileSinkDesc,
        path,
        Reporter.NULL)
    }

    writers.getOrElseUpdate(dynamicPartPath, newWriter())
  }
}
