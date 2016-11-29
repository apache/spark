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
import java.util.{Date, Locale}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator, Utilities}
import org.apache.hadoop.hive.ql.io.{HiveFileFormatUtils, HiveOutputFormat}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.TaskType

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableJobConf
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter

/**
 * Internal helper class that saves an RDD using a Hive OutputFormat.
 * It is based on `SparkHadoopWriter`.
 */
private[hive] class SparkHiveWriterContainer(
    @transient private val jobConf: JobConf,
    fileSinkConf: FileSinkDesc,
    inputSchema: Seq[Attribute])
  extends Logging
  with HiveInspectors
  with Serializable {

  private val now = new Date()
  private val tableDesc: TableDesc = fileSinkConf.getTableInfo
  // Add table properties from storage handler to jobConf, so any custom storage
  // handler settings can be set to jobConf
  if (tableDesc != null) {
    HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, jobConf, false)
    Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
  }
  protected val conf = new SerializableJobConf(jobConf)

  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null

  @transient private var writer: FileSinkOperator.RecordWriter = null
  @transient protected lazy val committer = conf.value.getOutputCommitter
  @transient protected lazy val jobContext = new JobContextImpl(conf.value, jID.value)
  @transient private lazy val taskContext = new TaskAttemptContextImpl(conf.value, taID.value)
  @transient private lazy val outputFormat =
    conf.value.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef, Writable]]

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
    val numberFormat = NumberFormat.getInstance(Locale.US)
    numberFormat.setMinimumIntegerDigits(5)
    numberFormat.setGroupingUsed(false)
    val extension = Utilities.getFileExtension(conf.value, fileSinkConf.getCompressed, outputFormat)
    "part-" + numberFormat.format(splitID) + extension
  }

  def close() {
    // Seems the boolean value passed into close does not matter.
    if (writer != null) {
      writer.close(false)
      commit()
    }
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
    SparkHadoopMapRedUtil.commitTask(committer, taskContext, jobID, splitID)
  }

  def abortTask(): Unit = {
    if (committer != null) {
      committer.abortTask(taskContext)
    }
    logError(s"Task attempt $taskContext aborted.")
  }

  private def setIDs(jobId: Int, splitId: Int, attemptId: Int) {
    jobID = jobId
    splitID = splitId
    attemptID = attemptId

    jID = new SerializableWritable[JobID](SparkHadoopWriter.createJobID(now, jobId))
    taID = new SerializableWritable[TaskAttemptID](
      new TaskAttemptID(new TaskID(jID.value, TaskType.MAP, splitID), attemptID))
  }

  private def setConfParams() {
    conf.value.set("mapred.job.id", jID.value.toString)
    conf.value.set("mapred.tip.id", taID.value.getTaskID.toString)
    conf.value.set("mapred.task.id", taID.value.toString)
    conf.value.setBoolean("mapred.task.is.map", true)
    conf.value.setInt("mapred.task.partition", splitID)
  }

  def newSerializer(tableDesc: TableDesc): Serializer = {
    val serializer = tableDesc.getDeserializerClass.newInstance().asInstanceOf[Serializer]
    serializer.initialize(null, tableDesc.getProperties)
    serializer
  }

  protected def prepareForWrite() = {
    val serializer = newSerializer(fileSinkConf.getTableInfo)
    val standardOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        fileSinkConf.getTableInfo.getDeserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val fieldOIs = standardOI.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
    val dataTypes = inputSchema.map(_.dataType)
    val wrappers = fieldOIs.zip(dataTypes).map { case (f, dt) => wrapperFor(f, dt) }
    val outputData = new Array[Any](fieldOIs.length)
    (serializer, standardOI, fieldOIs, dataTypes, wrappers, outputData)
  }

  // this function is executed on executor side
  def writeToFile(context: TaskContext, iterator: Iterator[InternalRow]): Unit = {
    val (serializer, standardOI, fieldOIs, dataTypes, wrappers, outputData) = prepareForWrite()
    executorSideSetup(context.stageId, context.partitionId, context.attemptNumber)

    iterator.foreach { row =>
      var i = 0
      while (i < fieldOIs.length) {
        outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(row.get(i, dataTypes(i)))
        i += 1
      }
      writer.write(serializer.serialize(outputData, standardOI))
    }

    close()
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
    jobConf: JobConf,
    fileSinkConf: FileSinkDesc,
    dynamicPartColNames: Array[String],
    inputSchema: Seq[Attribute])
  extends SparkHiveWriterContainer(jobConf, fileSinkConf, inputSchema) {

  import SparkHiveDynamicPartitionWriterContainer._

  private val defaultPartName = jobConf.get(
    ConfVars.DEFAULTPARTITIONNAME.varname, ConfVars.DEFAULTPARTITIONNAME.defaultStrVal)

  override protected def initWriters(): Unit = {
    // do nothing
  }

  override def close(): Unit = {
    // do nothing
  }

  override def commitJob(): Unit = {
    // This is a hack to avoid writing _SUCCESS mark file. In lower versions of Hadoop (e.g. 1.0.4),
    // semantics of FileSystem.globStatus() is different from higher versions (e.g. 2.4.1) and will
    // include _SUCCESS file when glob'ing for dynamic partition data files.
    //
    // Better solution is to add a step similar to what Hive FileSinkOperator.jobCloseOp does:
    // calling something like Utilities.mvFileToFinalPath to cleanup the output directory and then
    // load it with loadDynamicPartitions/loadPartition/loadTable.
    val oldMarker = conf.value.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)
    conf.value.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false)
    super.commitJob()
    conf.value.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, oldMarker)
  }

  // this function is executed on executor side
  override def writeToFile(context: TaskContext, iterator: Iterator[InternalRow]): Unit = {
    val (serializer, standardOI, fieldOIs, dataTypes, wrappers, outputData) = prepareForWrite()
    executorSideSetup(context.stageId, context.partitionId, context.attemptNumber)

    val partitionOutput = inputSchema.takeRight(dynamicPartColNames.length)
    val dataOutput = inputSchema.take(fieldOIs.length)
    // Returns the partition key given an input row
    val getPartitionKey = UnsafeProjection.create(partitionOutput, inputSchema)
    // Returns the data columns to be written given an input row
    val getOutputRow = UnsafeProjection.create(dataOutput, inputSchema)

    val fun: AnyRef = (pathString: String) => FileUtils.escapePathName(pathString, defaultPartName)
    // Expressions that given a partition key build a string like: col1=val/col2=val/...
    val partitionStringExpression = partitionOutput.zipWithIndex.flatMap { case (c, i) =>
      val escaped =
        ScalaUDF(fun, StringType, Seq(Cast(c, StringType)), Seq(StringType))
      val str = If(IsNull(c), Literal(defaultPartName), escaped)
      val partitionName = Literal(dynamicPartColNames(i) + "=") :: str :: Nil
      if (i == 0) partitionName else Literal(Path.SEPARATOR_CHAR.toString) :: partitionName
    }

    // Returns the partition path given a partition key.
    val getPartitionString =
      UnsafeProjection.create(Concat(partitionStringExpression) :: Nil, partitionOutput)

    // If anything below fails, we should abort the task.
    try {
      val sorter: UnsafeKVExternalSorter = new UnsafeKVExternalSorter(
        StructType.fromAttributes(partitionOutput),
        StructType.fromAttributes(dataOutput),
        SparkEnv.get.blockManager,
        SparkEnv.get.serializerManager,
        TaskContext.get().taskMemoryManager().pageSizeBytes,
        SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold",
          UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD))

      while (iterator.hasNext) {
        val inputRow = iterator.next()
        val currentKey = getPartitionKey(inputRow)
        sorter.insertKV(currentKey, getOutputRow(inputRow))
      }

      logInfo(s"Sorting complete. Writing out partition files one at a time.")
      val sortedIterator = sorter.sortedIterator()
      var currentKey: InternalRow = null
      var currentWriter: FileSinkOperator.RecordWriter = null
      try {
        while (sortedIterator.next()) {
          if (currentKey != sortedIterator.getKey) {
            if (currentWriter != null) {
              currentWriter.close(false)
            }
            currentKey = sortedIterator.getKey.copy()
            logDebug(s"Writing partition: $currentKey")
            currentWriter = newOutputWriter(currentKey)
          }

          var i = 0
          while (i < fieldOIs.length) {
            outputData(i) = if (sortedIterator.getValue.isNullAt(i)) {
              null
            } else {
              wrappers(i)(sortedIterator.getValue.get(i, dataTypes(i)))
            }
            i += 1
          }
          currentWriter.write(serializer.serialize(outputData, standardOI))
        }
      } finally {
        if (currentWriter != null) {
          currentWriter.close(false)
        }
      }
      commit()
    } catch {
      case cause: Throwable =>
        logError("Aborting task.", cause)
        abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
    }
    /** Open and returns a new OutputWriter given a partition key. */
    def newOutputWriter(key: InternalRow): FileSinkOperator.RecordWriter = {
      val partitionPath = getPartitionString(key).getString(0)
      val newFileSinkDesc = new FileSinkDesc(
        fileSinkConf.getDirName + partitionPath,
        fileSinkConf.getTableInfo,
        fileSinkConf.getCompressed)
      newFileSinkDesc.setCompressCodec(fileSinkConf.getCompressCodec)
      newFileSinkDesc.setCompressType(fileSinkConf.getCompressType)

      // use the path like ${hive_tmp}/_temporary/${attemptId}/
      // to avoid write to the same file when `spark.speculation=true`
      val path = FileOutputFormat.getTaskOutputPath(
        conf.value,
        partitionPath.stripPrefix("/") + "/" + getOutputName)

      HiveFileFormatUtils.getHiveRecordWriter(
        conf.value,
        fileSinkConf.getTableInfo,
        conf.value.getOutputValueClass.asInstanceOf[Class[Writable]],
        newFileSinkDesc,
        path,
        Reporter.NULL)
    }
  }
}
