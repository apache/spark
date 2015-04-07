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

import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.sql.Row
import org.apache.spark.{Logging, SerializableWritable, SparkHadoopWriter}
import org.apache.spark.sql.hive.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.hive.HiveShim._

/**
 * Internal helper class that saves an RDD using a Hive OutputFormat.
 * It is based on [[SparkHadoopWriter]].
 */
private[hive] class SparkHiveWriterContainer(
    @transient jobConf: JobConf,
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
    Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
  }
  protected val conf = new SerializableWritable(jobConf)

  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null

  @transient private var writer: FileSinkOperator.RecordWriter = null
  @transient protected lazy val committer = conf.value.getOutputCommitter
  @transient protected lazy val jobContext = newJobContext(conf.value, jID.value)
  @transient private lazy val taskContext = newTaskAttemptContext(conf.value, taID.value)
  @transient private lazy val outputFormat =
    conf.value.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef,Writable]]

  def driverSideSetup() {
    setIDs(0, 0, 0)
    setConfParams()
    committer.setupJob(jobContext)
  }

  def executorSideSetup(jobId: Int, splitId: Int, attemptId: Int) {
    setIDs(jobId, splitId, attemptId)
    setConfParams()
    committer.setupTask(taskContext)
    initWriters()
  }

  protected def getOutputName: String = {
    val numberFormat = NumberFormat.getInstance()
    numberFormat.setMinimumIntegerDigits(5)
    numberFormat.setGroupingUsed(false)
    val extension = Utilities.getFileExtension(conf.value, fileSinkConf.getCompressed, outputFormat)
    "part-" + numberFormat.format(splitID) + extension
  }

  def getLocalFileWriter(row: Row): FileSinkOperator.RecordWriter = writer

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
      conf.value,
      fileSinkConf.getTableInfo,
      conf.value.getOutputValueClass.asInstanceOf[Class[Writable]],
      fileSinkConf,
      FileOutputFormat.getTaskOutputPath(conf.value, getOutputName),
      Reporter.NULL)
  }

  protected def commit() {
    SparkHadoopMapRedUtil.commitTask(committer, taskContext, jobID, splitID, attemptID)
  }

  private def setIDs(jobId: Int, splitId: Int, attemptId: Int) {
    jobID = jobId
    splitID = splitId
    attemptID = attemptId

    jID = new SerializableWritable[JobID](SparkHadoopWriter.createJobID(now, jobId))
    taID = new SerializableWritable[TaskAttemptID](
      new TaskAttemptID(new TaskID(jID.value, true, splitID), attemptID))
  }

  private def setConfParams() {
    conf.value.set("mapred.job.id", jID.value.toString)
    conf.value.set("mapred.tip.id", taID.value.getTaskID.toString)
    conf.value.set("mapred.task.id", taID.value.toString)
    conf.value.setBoolean("mapred.task.is.map", true)
    conf.value.setInt("mapred.task.partition", splitID)
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
    @transient jobConf: JobConf,
    fileSinkConf: FileSinkDesc,
    dynamicPartColNames: Array[String])
  extends SparkHiveWriterContainer(jobConf, fileSinkConf) {

  import SparkHiveDynamicPartitionWriterContainer._

  private val defaultPartName = jobConf.get(
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
    val oldMarker = jobConf.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)
    jobConf.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false)
    super.commitJob()
    jobConf.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, oldMarker)
  }

  override def getLocalFileWriter(row: Row): FileSinkOperator.RecordWriter = {
    val dynamicPartPath = dynamicPartColNames
      .zip(row.toSeq.takeRight(dynamicPartColNames.length))
      .map { case (col, rawVal) =>
        val string = if (rawVal == null) null else String.valueOf(rawVal)
        val colString =
          if (string == null || string.isEmpty) {
            defaultPartName
          } else {
            FileUtils.escapePathName(string)
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
        val outputPath = FileOutputFormat.getOutputPath(conf.value)
        assert(outputPath != null, "Undefined job output-path")
        val workPath = new Path(outputPath, dynamicPartPath.stripPrefix("/"))
        new Path(workPath, getOutputName)
      }

      HiveFileFormatUtils.getHiveRecordWriter(
        conf.value,
        fileSinkConf.getTableInfo,
        conf.value.getOutputValueClass.asInstanceOf[Class[Writable]],
        newFileSinkDesc,
        path,
        Reporter.NULL)
    }

    writers.getOrElseUpdate(dynamicPartPath, newWriter())
  }
}
